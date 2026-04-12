from __future__ import annotations

import json
import shutil
import sys
import re
import shlex
from difflib import get_close_matches
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from . import artifact_resolution as ar
from . import branch_lifecycle as bl
from . import branch_mechanics as bmech
from . import manager_actions as mgr_actions
from . import manager_hooks as mgr_hooks
from . import manager_packet as mgr_packet
from . import profile_identity as pi
from . import typed_tools as tt
from . import validation_outcome as vo
from .config import AppConfig
from .controller_protocol import (
    LOCAL_OPENING_STEP_PROTOCOL,
    SFT_SYSTEM_PROTOCOL,
    SYSTEM_PROTOCOL,
)
from .fuzzfolio import CliError, CommandResult, FuzzfolioCli
from .manager_models import ManagerHookEvent
from .manager_state import ManagerRuntimeState
from .ledger import (
    append_attempt,
    attempt_exists,
    attempts_path_for_run_dir,
    load_run_metadata,
    load_attempts,
    load_run_attempts,
    make_attempt_record,
    write_run_metadata,
)
from .plotting import compute_frontier, render_progress_artifacts
from .provider import ChatMessage, ProviderError, create_provider, provider_trace_scope
from .scoring import AttemptScore, build_attempt_score, load_sensitivity_snapshot
from trainingdatapipeline.normalize_state import build_prompt_variants

_RUNTIME_TRACE_STDERR_MODE = "verbose"


def set_runtime_trace_stderr_mode(mode: str) -> None:
    global _RUNTIME_TRACE_STDERR_MODE
    normalized = str(mode or "").strip().lower()
    if normalized not in {"verbose", "warnings_only", "off"}:
        normalized = "verbose"
    _RUNTIME_TRACE_STDERR_MODE = normalized


def _should_emit_runtime_trace_line(*, status: str, level: str | None) -> bool:
    mode = _RUNTIME_TRACE_STDERR_MODE
    if mode == "verbose":
        return True
    if mode == "off":
        return False
    normalized_level = str(level or "").strip().lower()
    normalized_status = str(status or "").strip().lower()
    if normalized_level in {"warning", "error"}:
        return True
    warning_statuses = {"blocked", "denied", "action_failed", "error", "failed"}
    return normalized_status in warning_statuses


SUPERVISED_EXTRA_RULES = """
- You are running in supervised mode. The controller/session policy, not you, decides when the session stops.
- Do not use `finish` in supervised mode. Keep working until the controller stops prompting you.
- When you have a good candidate, keep exploring nearby and contrasting branches instead of trying to end the run.
"""

COMPACTION_PROMPT = """You are writing a handoff summary for the same research controller.

Include:
- Current progress
- Important decisions
- Constraints and user preferences
- Concrete next steps
- Critical paths or artifact locations

Return JSON with this shape only:
{
  "checkpoint_summary": "concise multi-line summary"
}
"""

SUMMARY_PREFIX = """Another language model started to solve this problem and produced a summary of its thinking process.
Use the summary below to continue the same autonomous Fuzzfolio research run without repeating old work.
"""

RESPONSE_REPAIR_PROMPT = """Your previous JSON response was structurally invalid for the controller.

Return a corrected full replacement response in the exact required top-level shape:
{
  "reasoning": "one short paragraph",
  "actions": [{ ... }]
}

Hard requirements:
- Preserve the same intent. If you were using typed tools (prepare_profile, mutate_profile, validate_profile, register_profile, evaluate_candidate, run_parameter_sweep, inspect_artifact, compare_artifacts), keep them—do not rewrite into run_cli unless the original plan was already run_cli or recovery truly requires it.
- Every write_file action must include a full non-empty string `content` field.
- Use candidate_name for local drafts and exact profile_ref for registered profiles. Do not emit profile_path, destination_path, or source_profile_path.
- evaluate_candidate requires instruments[].
- If you cannot fit all planned work, reduce the number of actions.
- Do not omit required fields.
- Return exactly one raw JSON object only.
"""

LOCAL_OPENING_ALLOWED_FIELDS = (
    "tool",
    "mode",
    "indicator_ids",
    "instruments",
    "candidate_name",
)
LOCAL_OPENING_ALLOWED_FIELD_SET = frozenset(LOCAL_OPENING_ALLOWED_FIELDS)
LOCAL_OPENING_PRIORITY_INSTRUMENTS = (
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "NZDUSD",
    "USDCAD",
    "USDCHF",
    "EURJPY",
    "GBPJPY",
)
LOCAL_OPENING_BROAD_GOAL_KEYWORDS = (
    "broad",
    "breadth",
    "coverage",
    "basket",
    "divers",
    "clustered positivity",
)
LOCAL_OPENING_NARROW_GOAL_KEYWORDS = (
    "narrow",
    "selective",
    "focus",
    "sharper",
    "sharp",
)


def _normalize_instrument_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        normalized.append(text)
    return normalized


def _uses_forbidden_opening_instrument(value: Any) -> bool:
    instruments = [item.upper() for item in _normalize_instrument_list(value)]
    return any(item in {"ALL", "__BASKET__"} for item in instruments)


def _normalize_windows_path_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text.replace("/", "\\").rstrip("\\")


def _sanitize_opening_candidate_name(value: Any) -> str | None:
    text = re.sub(r"[^\w\-]+", "_", str(value or "").strip()) or ""
    return text[:72] if text else None


def _candidate_name_from_profile_path_text(value: Any) -> str | None:
    path_text = _normalize_windows_path_text(value)
    if not path_text:
        return None
    try:
        return _sanitize_opening_candidate_name(Path(path_text).stem)
    except Exception:
        return None


def canonicalize_local_opening_step_response(
    payload: dict[str, Any],
    *,
    starter_instruments: list[str] | None = None,
    candidate_name_hint: str | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    actions = payload.get("actions")
    if not isinstance(actions, list):
        return payload
    first_action = next(
        (item for item in actions if isinstance(item, dict)),
        None,
    )
    if not isinstance(first_action, dict):
        return payload
    if str(first_action.get("tool") or "").strip() != "prepare_profile":
        return payload
    salvage_hints = (
        "mode",
        "indicator_ids",
        "seed_indicators",
        "candidate_name",
        "profile_name",
        "destination_path",
        "instruments",
    )
    if not any(first_action.get(key) for key in salvage_hints):
        return payload
    normalized_action: dict[str, Any] = {"tool": "prepare_profile"}
    mode = first_action.get("mode")
    if isinstance(mode, str) and mode.strip():
        normalized_action["mode"] = mode.strip()
    indicator_ids = first_action.get("indicator_ids")
    if not isinstance(indicator_ids, list) or not indicator_ids:
        seed_indicators = first_action.get("seed_indicators")
        if isinstance(seed_indicators, list) and seed_indicators:
            indicator_ids = seed_indicators
    if isinstance(indicator_ids, list) and indicator_ids:
        normalized_action["indicator_ids"] = indicator_ids
    if "mode" not in normalized_action and normalized_action.get("indicator_ids"):
        normalized_action["mode"] = "scaffold_from_seed"
    candidate_name = first_action.get("candidate_name")
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        profile_name = first_action.get("profile_name")
        if isinstance(profile_name, str) and profile_name.strip():
            candidate_name = profile_name
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        candidate_name = _candidate_name_from_profile_path_text(
            first_action.get("destination_path")
        )
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        candidate_name = candidate_name_hint
    candidate_name_text = _sanitize_opening_candidate_name(candidate_name)
    if candidate_name_text:
        normalized_action["candidate_name"] = candidate_name_text
    resolved_starter_instruments = _normalize_instrument_list(starter_instruments)
    instruments = _normalize_instrument_list(first_action.get("instruments"))
    if _uses_forbidden_opening_instrument(instruments):
        instruments = []
    if not instruments and resolved_starter_instruments:
        instruments = resolved_starter_instruments
    if instruments:
        normalized_action["instruments"] = instruments
    cleaned_action = {
        key: value
        for key, value in normalized_action.items()
        if key in LOCAL_OPENING_ALLOWED_FIELD_SET
    }
    return {
        "reasoning": str(payload.get("reasoning", "")).strip(),
        "actions": [cleaned_action],
    }


LEGACY_MODEL_PATH_FIELDS = frozenset(
    {
        "profile_path",
        "destination_path",
        "source_profile_path",
        "metadata_out_path",
    }
)


def _pathless_action_from_legacy_fields(action: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(action)
    tool = str(normalized.get("tool") or "").strip()
    candidate_name = normalized.get("candidate_name")
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        candidate_name = _candidate_name_from_profile_path_text(
            normalized.get("profile_path")
        ) or _candidate_name_from_profile_path_text(normalized.get("destination_path"))
    if isinstance(candidate_name, str) and candidate_name.strip():
        normalized["candidate_name"] = _sanitize_opening_candidate_name(candidate_name)

    source_candidate_name = normalized.get("source_candidate_name")
    if not isinstance(source_candidate_name, str) or not source_candidate_name.strip():
        source_candidate_name = _candidate_name_from_profile_path_text(
            normalized.get("source_profile_path")
        )
    if isinstance(source_candidate_name, str) and source_candidate_name.strip():
        normalized["source_candidate_name"] = _sanitize_opening_candidate_name(
            source_candidate_name
        )

    destination_candidate_name = normalized.get("destination_candidate_name")
    if (
        not isinstance(destination_candidate_name, str)
        or not destination_candidate_name.strip()
    ):
        destination_candidate_name = _candidate_name_from_profile_path_text(
            normalized.get("destination_path")
        )
    if (
        isinstance(destination_candidate_name, str)
        and destination_candidate_name.strip()
    ):
        normalized["destination_candidate_name"] = _sanitize_opening_candidate_name(
            destination_candidate_name
        )

    if tool == "prepare_profile" and str(normalized.get("mode") or "").strip() == "clone_local":
        normalized.pop("candidate_name", None)
        if "destination_candidate_name" not in normalized:
            derived_name = _candidate_name_from_profile_path_text(
                normalized.get("destination_path")
            )
            if isinstance(derived_name, str) and derived_name.strip():
                normalized["destination_candidate_name"] = _sanitize_opening_candidate_name(
                    derived_name
                )

    for field in LEGACY_MODEL_PATH_FIELDS:
        normalized.pop(field, None)
    return normalized


def canonicalize_followup_step_response(
    payload: dict[str, Any],
    *,
    next_action_template: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(next_action_template, dict):
        return payload
    actions = payload.get("actions")
    if not isinstance(actions, list):
        return payload
    first_action = next((item for item in actions if isinstance(item, dict)), None)
    if not isinstance(first_action, dict):
        return payload
    template = _pathless_action_from_legacy_fields(dict(next_action_template))
    template_tool = str(template.get("tool") or "").strip()
    if template_tool not in {
        "validate_profile",
        "register_profile",
        "mutate_profile",
        "evaluate_candidate",
    }:
        return payload
    normalized_first = _pathless_action_from_legacy_fields(dict(first_action))
    if str(normalized_first.get("tool") or "").strip() != template_tool:
        return payload
    changed = normalized_first != first_action or len(actions) > 1
    if template_tool == "evaluate_candidate":
        if (
            not isinstance(normalized_first.get("instruments"), list)
            or not normalized_first.get("instruments")
        ) and isinstance(template.get("instruments"), list) and template.get("instruments"):
            normalized_first["instruments"] = list(template.get("instruments") or [])
            changed = True
    for field in (
        "candidate_name",
        "profile_ref",
        "attempt_id",
        "artifact_dir",
        "evaluation_mode",
        "timeframe_policy",
        "requested_horizon_months",
        "view",
        "operation",
    ):
        if normalized_first.get(field) not in (None, "", [], {}):
            continue
        if template.get(field) in (None, "", [], {}):
            continue
        normalized_first[field] = template.get(field)
        changed = True
    if not changed:
        return payload
    return {
        "reasoning": str(payload.get("reasoning", "")).strip(),
        "actions": [normalized_first],
    }


def _read_json_if_exists(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json_payload(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _extract_profile_instruments_from_payload(
    payload: dict[str, Any] | None,
) -> list[str]:
    profile = _profile_root(payload)
    if not isinstance(profile, dict):
        return []
    instruments = profile.get("instruments")
    if not isinstance(instruments, list):
        return []
    return [str(item).strip() for item in instruments if str(item).strip()]


def _profile_root(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    nested = payload.get("profile")
    if isinstance(nested, dict):
        return nested
    indicators = payload.get("indicators")
    if isinstance(indicators, list):
        return payload
    return None


def _ensure_profile_name_matches_candidate(
    path: Path | None,
    candidate_name: str | None,
) -> dict[str, Any] | None:
    desired = str(candidate_name or "").strip()
    payload = _read_json_if_exists(path)
    root = _profile_root(payload)
    if not desired or path is None or not isinstance(root, dict):
        return payload
    current = str(root.get("name") or "").strip()
    if current == desired:
        return payload
    root["name"] = desired
    if isinstance(payload, dict):
        try:
            _write_json_payload(path, payload)
        except OSError:
            return payload
    return payload


def _summary_join(
    values: list[str],
    *,
    separator: str,
    max_visible: int,
) -> str:
    clean = [str(value).strip() for value in values if str(value).strip()]
    if not clean:
        return ""
    if len(clean) <= max_visible:
        return separator.join(clean)
    visible = separator.join(clean[:max_visible])
    return f"{visible}{separator}{len(clean) - max_visible} more"


def _normalize_timeframe_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip().strip("\"'").strip()
    if not text:
        return None
    return text.upper()


def _timeframe_mismatch_message(requested_timeframe: str, effective_timeframe: str) -> str:
    return (
        f"Timeframe auto-adjustment detected: requested {requested_timeframe} "
        f"but CLI ran {effective_timeframe}. "
        f"This does NOT count as a valid higher-timeframe experiment. "
        f"Next action must resolve the mismatch: patch active indicator timeframe(s) "
        f"to the intended timeframe, reformulate as {effective_timeframe} test, "
        f"or abandon that timeframe hypothesis. "
        f"Do NOT request the same higher-timeframe eval with the same unchanged profile."
    )


def _build_timeframe_mismatch_entry(
    requested_timeframe: Any,
    effective_timeframe: Any,
    *,
    source: str,
) -> dict[str, Any] | None:
    requested = _normalize_timeframe_value(requested_timeframe)
    effective = _normalize_timeframe_value(effective_timeframe)
    if not requested or not effective or requested == effective:
        return None
    return {
        "requested": requested,
        "effective": effective,
        "mismatch": True,
        "source": source,
        "message": _timeframe_mismatch_message(requested, effective),
    }


def _extract_timeframe_mismatch_from_output(output: str) -> dict[str, Any] | None:
    if "Auto-adjusted timeframe from" not in output:
        return None
    match = re.search(
        r"Auto-adjusted timeframe from\s+'?([^'\s]+)'?\s+to\s+'?([^'\s]+)'?",
        output,
    )
    if not match:
        return None
    return _build_timeframe_mismatch_entry(
        match.group(1),
        match.group(2),
        source="cli_output",
    )


def _candidate_summary_from_profile_payload(
    payload: dict[str, Any] | None,
    *,
    profile_path: Path | None = None,
    profile_ref: str | None = None,
    draft_name: str | None = None,
) -> dict[str, Any] | None:
    profile = _profile_root(payload)
    if not isinstance(profile, dict):
        return None
    indicators = profile.get("indicators")
    if not isinstance(indicators, list) or not indicators:
        return None
    indicator_ids: list[str] = []
    instance_ids: list[str] = []
    timeframes: list[str] = []
    for indicator in indicators:
        if not isinstance(indicator, dict):
            continue
        meta = indicator.get("meta") if isinstance(indicator.get("meta"), dict) else {}
        config = (
            indicator.get("config") if isinstance(indicator.get("config"), dict) else {}
        )
        indicator_id = str(meta.get("id") or "").strip()
        if indicator_id:
            indicator_ids.append(indicator_id)
        instance_id = str(meta.get("instanceId") or "").strip()
        if instance_id:
            instance_ids.append(instance_id)
        timeframe = str(config.get("timeframe") or "").strip()
        if timeframe:
            timeframes.append(timeframe)
    instruments_raw = profile.get("instruments")
    instruments = (
        [str(item).strip() for item in instruments_raw if str(item).strip()]
        if isinstance(instruments_raw, list)
        else []
    )
    family_id = "|".join(sorted(instance_ids)) if instance_ids else None
    fingerprint: str | None = None
    if profile_path is not None:
        fp, _err = pi.compute_profile_fingerprint(profile_path)
        fingerprint = fp
    if fingerprint is None:
        source_payload = payload if isinstance(payload, dict) else {"profile": profile}
        fingerprint = pi.fingerprint_for_json_object(source_payload)
    summary: dict[str, Any] = {
        "candidate_name": str(draft_name or profile.get("name") or "").strip() or None,
        "draft_name": str(draft_name or profile.get("name") or "").strip() or None,
        "profile_name": str(profile.get("name") or "").strip() or None,
        "candidate_fingerprint": fingerprint,
        "family_id": family_id,
        "indicator_ids": indicator_ids,
        "indicator_instance_ids": instance_ids,
        "instruments": instruments,
        "timeframe_summary": _summary_join(
            timeframes, separator=" + ", max_visible=4
        ),
        "instrument_summary": _summary_join(
            instruments, separator=", ", max_visible=4
        ),
        "instrument_count": len(instruments),
        "indicator_count": len(indicator_ids),
        "profile_ref": str(profile_ref).strip() if profile_ref else None,
    }
    return {
        key: value
        for key, value in summary.items()
        if value is not None and value != ""
    }


def _prompt_visible_candidate_fields(
    summary: dict[str, Any] | None,
    *,
    include_strategy: bool,
    include_profile_ref: bool = True,
) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return {}
    compact: dict[str, Any] = {}
    candidate_name = str(
        summary.get("candidate_name")
        or summary.get("draft_name")
        or summary.get("profile_name")
        or ""
    ).strip()
    if candidate_name:
        compact["candidate_name"] = candidate_name
    if include_profile_ref:
        profile_ref = str(summary.get("profile_ref") or "").strip()
        if profile_ref:
            compact["profile_ref"] = profile_ref
    if include_strategy:
        indicator_ids = summary.get("indicator_ids")
        if isinstance(indicator_ids, list) and indicator_ids:
            compact["indicator_ids"] = [
                str(item).strip()
                for item in indicator_ids
                if str(item).strip()
            ]
        instruments = summary.get("instruments")
        if isinstance(instruments, list) and instruments:
            compact["instruments"] = [
                str(item).strip() for item in instruments if str(item).strip()
            ]
        timeframe_summary = str(summary.get("timeframe_summary") or "").strip()
        if timeframe_summary:
            compact["timeframe_summary"] = timeframe_summary
    return compact


def _compact_compare_summary_for_prompt(
    compare: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(compare, dict):
        return None
    best = compare.get("best") if isinstance(compare.get("best"), dict) else {}
    compact: dict[str, Any] = {}
    for key in ("quality_score", "signal_count", "timeframe", "dsr"):
        if best.get(key) is not None:
            compact[key] = best.get(key)
    best_cell = best.get("best_cell")
    if isinstance(best_cell, dict):
        for key in ("resolved_trades", "avg_net_r_per_closed_trade"):
            if best_cell.get(key) is not None:
                compact[key] = best_cell.get(key)
    best_path = best.get("best_cell_path_metrics")
    if isinstance(best_path, dict):
        for key in ("psr", "k_ratio", "sharpe_r", "max_drawdown_r"):
            if best_path.get(key) is not None:
                compact[key] = best_path.get(key)
    market_window = best.get("market_data_window")
    if isinstance(market_window, dict):
        for key in ("effective_window_months", "window_truncated"):
            if market_window.get(key) is not None:
                compact[key] = market_window.get(key)
    matrix_summary = best.get("matrix_summary")
    if isinstance(matrix_summary, dict) and matrix_summary.get("positive_cell_ratio") is not None:
        compact["positive_cell_ratio"] = matrix_summary.get("positive_cell_ratio")
    return compact or None


def _compact_sweep_summary_for_prompt(
    summary: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(summary, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        "fitness_metric",
        "top_score",
        "top_effective_window_months",
        "parameter_importance_flat",
        "recommended_interpretation",
    ):
        if summary.get(key) is not None:
            compact[key] = summary.get(key)
    top_parameters = summary.get("top_parameters")
    if isinstance(top_parameters, dict) and top_parameters:
        compact["top_parameters"] = top_parameters
    return compact or None


def _compact_ranked_comparison_for_prompt(
    ranked: Any,
) -> list[dict[str, Any]] | None:
    if not isinstance(ranked, list) or not ranked:
        return None
    compact_rows: list[dict[str, Any]] = []
    for row in ranked[:2]:
        if not isinstance(row, dict):
            continue
        compact: dict[str, Any] = {}
        for key in ("label", "artifact_dir", "quality_score"):
            if row.get(key) is not None:
                compact[key] = row.get(key)
        best = row.get("best")
        if isinstance(best, dict):
            for key in ("quality_score", "signal_count", "timeframe"):
                if best.get(key) is not None:
                    compact[key] = best.get(key)
            best_cell = best.get("best_cell")
            if isinstance(best_cell, dict) and best_cell.get("resolved_trades") is not None:
                compact["resolved_trades"] = best_cell.get("resolved_trades")
            market_window = best.get("market_data_window")
            if isinstance(market_window, dict) and market_window.get("effective_window_months") is not None:
                compact["effective_window_months"] = market_window.get("effective_window_months")
        if compact:
            compact_rows.append(compact)
    return compact_rows or None


def _normalized_profile_material_changes(
    source_payload: dict[str, Any] | None,
    normalized_profile: dict[str, Any] | None,
) -> bool | None:
    source_root = _profile_root(source_payload)
    normalized_root = (
        normalized_profile if isinstance(normalized_profile, dict) else None
    )
    if not isinstance(source_root, dict) or not isinstance(normalized_root, dict):
        return None
    return (
        pi.fingerprint_for_json_object(source_root)
        != pi.fingerprint_for_json_object(normalized_root)
    )


def _extract_effective_window_from_sweep_fitness(
    fitness: dict[str, Any],
) -> float | None:
    candidates: list[Any] = [
        fitness.get("effective_window_months"),
        (
            fitness.get("quality_score_payload") or {}
        ).get("inputs", {}).get("effective_window_months")
        if isinstance(fitness.get("quality_score_payload"), dict)
        else None,
    ]
    for value in candidates:
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _summarize_sweep_results_payload(
    payload: dict[str, Any] | None,
    *,
    artifact_dir: Path,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return None
    ranked = data.get("ranked_permutations")
    if not isinstance(ranked, list):
        ranked = data.get("ranked") if isinstance(data.get("ranked"), list) else []
    top = ranked[0] if ranked and isinstance(ranked[0], dict) else None
    fitness = (
        top.get("fitness")
        if isinstance(top, dict) and isinstance(top.get("fitness"), dict)
        else {}
    )
    parameter_importance = (
        data.get("parameter_importance")
        if isinstance(data.get("parameter_importance"), list)
        else []
    )
    flat_importance = bool(parameter_importance) and all(
        (
            abs(float(item.get("importance_pct") or 0.0)) < 1e-9
            and abs(float(item.get("raw_spread") or 0.0)) < 1e-9
        )
        for item in parameter_importance
        if isinstance(item, dict)
    )
    top_parameters = (
        top.get("parameters")
        if isinstance(top, dict) and isinstance(top.get("parameters"), dict)
        else {}
    )
    recommended = (
        "Sweep found no meaningful parameter gradient; treat this as a plateau and move on."
        if flat_importance and parameter_importance
        else "Sweep isolated a leading permutation worth comparing against the current branch leader."
    )
    summary: dict[str, Any] = {
        "artifact_dir": str(artifact_dir),
        "fitness_metric": data.get("fitness_metric"),
        "mode": data.get("mode"),
        "elapsed_seconds": data.get("elapsed_seconds"),
        "ranked_permutation_count": len(ranked),
        "top_rank": top.get("rank") if isinstance(top, dict) else None,
        "top_score": (
            top.get("fitness_value")
            if isinstance(top, dict) and top.get("fitness_value") is not None
            else fitness.get("quality_score")
        ),
        "top_parameters": top_parameters,
        "top_effective_window_months": _extract_effective_window_from_sweep_fitness(
            fitness
        ),
        "top_resolved_trades": fitness.get("resolved_trade_count_max")
        or fitness.get("best_cell_resolved_trades"),
        "parameter_importance_flat": flat_importance,
        "parameter_importance": parameter_importance[:8],
        "recommended_interpretation": recommended,
    }
    return {
        key: value
        for key, value in summary.items()
        if value is not None and value != ""
    }


@dataclass
class ToolContext:
    run_id: str
    run_dir: Path
    attempts_path: Path
    run_metadata_path: Path
    profiles_dir: Path
    evals_dir: Path
    notes_dir: Path
    progress_plot_path: Path
    cli_help_catalog_path: Path
    seed_prompt_path: Path | None
    profile_template_path: Path
    indicator_catalog_summary: str | None
    seed_indicator_parameter_hints: str | None
    instrument_catalog_summary: str | None


@dataclass
class RunPolicy:
    allow_finish: bool = True
    window_start: str | None = None
    window_end: str | None = None
    timezone_name: str = "America/Chicago"
    stop_mode: str = "after_step"
    mode_name: str = "run"
    soft_wrap_minutes: int = 0


class ResearchController:
    def __init__(
        self, app_config: AppConfig, *, llm_request_snapshots: bool = False
    ):
        self.config = app_config
        self._llm_request_snapshots_enabled = bool(llm_request_snapshots)
        self.provider = create_provider(app_config.provider)
        self.manager_providers = [
            (
                f"manager{index}",
                profile_name,
                create_provider(app_config.providers[profile_name]),
            )
            for index, profile_name in enumerate(app_config.manager.profiles, start=1)
            if profile_name in app_config.providers
        ]
        self._manager_runtime = ManagerRuntimeState()
        self._frontier_prior_best: float | None = None
        self.cli = FuzzfolioCli(app_config.fuzzfolio)
        self.profile_sources: dict[str, Path] = {}
        self.last_created_profile_ref: str | None = None
        self.finish_denials = 0
        self.profile_template_path = (
            self.config.repo_root / "portable_profile_template.json"
        )
        self._cli_help_catalog_cache: dict[str, Any] | None = None
        self._family_mutation_counts: dict[str, int] = {}
        self._profile_path_validate_cache: dict[str, tuple[str, float]] = {}
        self._profile_fingerprint_validate_ok: dict[str, bool] = {}
        self._profile_fingerprint_to_ref: dict[str, str] = {}
        self._consecutive_same_family_exploit: int = 0
        self._last_family_id: str | None = None
        self._timeframe_mismatches: list[dict[str, Any]] = []
        self._same_family_exploit_history: list[str] = []
        self._family_branches: dict[str, bl.FamilyBranchState] = {}
        self._branch_overlay = bl.BranchRunOverlay()
        self._current_controller_step: int = 0
        self._current_step_limit: int = 0
        self._current_run_policy: RunPolicy | None = None
        self._tool_usage_counts: dict[str, int] = {}
        self._validation_stale_without_validated: int = 0
        self._pending_manager_events: list[dict[str, Any]] = []

    def _reset_run_state(self) -> None:
        self._family_mutation_counts = {}
        self._profile_path_validate_cache = {}
        self._profile_fingerprint_validate_ok = {}
        self._profile_fingerprint_to_ref = {}
        self._consecutive_same_family_exploit = 0
        self._last_family_id = None
        self._timeframe_mismatches = []
        self._same_family_exploit_history = []
        self._family_branches = {}
        self._branch_overlay = bl.BranchRunOverlay()
        self._current_controller_step = 0
        self._current_step_limit = 0
        self._current_run_policy = None
        self._tool_usage_counts = {}
        self._validation_stale_without_validated = 0
        self._pending_manager_events = []
        self._manager_runtime = ManagerRuntimeState()
        self._frontier_prior_best = None

    def _bump_tool_usage(self, tool: str) -> None:
        key = str(tool).strip() or "unknown"
        self._tool_usage_counts[key] = int(self._tool_usage_counts.get(key, 0)) + 1

    def _record_pending_manager_event(
        self,
        *,
        hook: ManagerHookEvent,
        status: str,
        action_count: int = 0,
        rationale: str | None = None,
        error: str | None = None,
    ) -> None:
        event: dict[str, Any] = {
            "hook": hook.value,
            "status": str(status or "").strip() or "unknown",
            "action_count": max(0, int(action_count)),
        }
        rationale_text = str(rationale or "").strip()
        if rationale_text:
            event["rationale"] = rationale_text[:500]
        error_text = str(error or "").strip()
        if error_text:
            event["error"] = error_text[:300]
        self._pending_manager_events.append(event)

    def _flush_pending_manager_events(self) -> list[dict[str, Any]]:
        if not self._pending_manager_events:
            return []
        events = list(self._pending_manager_events)
        self._pending_manager_events = []
        return events

    def _parse_lookback_months_from_cli_args(self, args: list[str]) -> int | None:
        if "--lookback-months" not in args:
            return None
        idx = args.index("--lookback-months") + 1
        if idx >= len(args):
            return None
        try:
            return int(str(args[idx]).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _positive_int_or_none(val: Any) -> int | None:
        if isinstance(val, bool):
            return None
        if isinstance(val, int) and val > 0:
            return val
        if isinstance(val, float) and val > 0:
            return int(val)
        if isinstance(val, str) and val.strip():
            try:
                n = int(float(val.strip()))
                return n if n > 0 else None
            except (TypeError, ValueError):
                return None
        return None

    def _requested_horizon_from_source_action(
        self, source_action: dict[str, Any]
    ) -> int | None:
        if str(source_action.get("tool") or "").strip() != "evaluate_candidate":
            return None
        return self._positive_int_or_none(source_action.get("requested_horizon_months"))

    def _requested_horizon_from_artifact_dir(self, artifact_dir: Path) -> int | None:
        """Resolve requested lookback months from CLI echoes in sensitivity-response.json."""
        snap = load_sensitivity_snapshot(artifact_dir.resolve())
        if not isinstance(snap, dict):
            return None
        candidates: list[Any] = []
        for path in (
            ("data", "request", "lookback_months"),
            ("data", "request", "lookbackMonths"),
            ("data", "request", "lookback"),
            ("request", "lookback_months"),
            ("meta", "lookback_months"),
            ("meta", "lookbackMonths"),
            ("lookback_months"),
            ("lookbackMonths",),
        ):
            cur: Any = snap
            for key in path[:-1]:
                if not isinstance(cur, dict):
                    cur = None
                    break
                cur = cur.get(key)
            if isinstance(cur, dict):
                v = cur.get(path[-1])
                candidates.append(v)
        cur = snap.get("data") if isinstance(snap.get("data"), dict) else None
        if isinstance(cur, dict):
            candidates.append(cur.get("lookback_months"))
        for raw in candidates:
            if isinstance(raw, int) and raw > 0:
                return raw
            if isinstance(raw, float) and raw > 0:
                return int(raw)
            if isinstance(raw, str) and raw.strip():
                try:
                    n = int(float(raw.strip()))
                    if n > 0:
                        return n
                except (TypeError, ValueError):
                    continue
        return None

    def _timeframes_from_sensitivity_snapshot(
        self, snap: dict[str, Any] | None
    ) -> tuple[str | None, str | None]:
        """Extract requested and effective timeframe strings without dropping either."""
        if not isinstance(snap, dict):
            return None, None
        req_tf: str | None = None
        eff_tf: str | None = None
        data = snap.get("data") if isinstance(snap.get("data"), dict) else None
        if isinstance(data, dict):
            req = data.get("request")
            if isinstance(req, dict):
                for key in ("timeframe", "time_frame", "primary_timeframe"):
                    v = req.get(key)
                    if isinstance(v, str) and v.strip():
                        req_tf = v.strip()
                        break
            for key in ("effective_timeframe", "effectiveTimeframe"):
                v = data.get(key)
                if isinstance(v, str) and v.strip():
                    eff_tf = v.strip()
                    break
            mw = data.get("market_data_window")
            if isinstance(mw, dict):
                if req_tf is None:
                    mreq = mw.get("requested_timeframe")
                    if isinstance(mreq, str) and mreq.strip():
                        req_tf = mreq.strip()
                if eff_tf is None:
                    meff = mw.get("effective_timeframe")
                    if isinstance(meff, str) and meff.strip():
                        eff_tf = meff.strip()
        if req_tf is None:
            for key in ("requested_timeframe", "requestedTimeframe"):
                v = snap.get(key)
                if isinstance(v, str) and v.strip():
                    req_tf = v.strip()
                    break
        if eff_tf is None:
            for key in ("effective_timeframe", "effectiveTimeframe"):
                v = snap.get(key)
                if isinstance(v, str) and v.strip():
                    eff_tf = v.strip()
                    break
        return req_tf, eff_tf

    @staticmethod
    def _nested_dict_get(obj: Any, path: list[str]) -> Any:
        cur: Any = obj
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    @staticmethod
    def _coerce_float_months(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if value is not None and str(value).strip() not in {"", "none"}:
            try:
                return float(str(value).strip())
            except (TypeError, ValueError):
                return None
        return None

    def _extract_effective_window_months(
        self,
        best_summary: dict[str, Any],
        sensitivity_snapshot: dict[str, Any] | None,
        compare_payload: dict[str, Any] | None,
    ) -> tuple[float | None, str | None]:
        """effective_window often lives only under sensitivity aggregate, not compare `best`."""
        mw = best_summary.get("market_data_window")
        if isinstance(mw, dict):
            w = self._coerce_float_months(mw.get("effective_window_months"))
            if w is not None:
                return w, "best_summary.market_data_window"
        if isinstance(sensitivity_snapshot, dict):
            snap_paths = (
                ["data", "aggregate", "market_data_window", "effective_window_months"],
                ["data", "market_data_window", "effective_window_months"],
                ["data", "aggregate", "effective_window_months"],
            )
            for path in snap_paths:
                w = self._coerce_float_months(
                    self._nested_dict_get(sensitivity_snapshot, path)
                )
                if w is not None:
                    return w, ".".join(path)
        if isinstance(compare_payload, dict):
            best = compare_payload.get("best")
            if isinstance(best, dict):
                cmw = best.get("market_data_window")
                if isinstance(cmw, dict):
                    w = self._coerce_float_months(cmw.get("effective_window_months"))
                    if w is not None:
                        return w, "compare.best.market_data_window"
            ranked = compare_payload.get("ranked")
            if isinstance(ranked, list) and ranked:
                first = ranked[0]
                if isinstance(first, dict):
                    rmw = first.get("market_data_window")
                    if isinstance(rmw, dict):
                        w = self._coerce_float_months(rmw.get("effective_window_months"))
                        if w is not None:
                            return w, "compare.ranked[0].market_data_window"
            data = compare_payload.get("data")
            if isinstance(data, dict):
                agg = data.get("aggregate")
                if isinstance(agg, dict):
                    amw = agg.get("market_data_window")
                    if isinstance(amw, dict):
                        w = self._coerce_float_months(amw.get("effective_window_months"))
                        if w is not None:
                            return w, "compare.data.aggregate.market_data_window"
        return None, None

    @staticmethod
    def _timeframes_compatible_for_provisional(
        requested_timeframe: str | None, effective_timeframe: str | None
    ) -> bool:
        if not requested_timeframe or not effective_timeframe:
            return True
        return (
            requested_timeframe.strip().upper()
            == effective_timeframe.strip().upper()
        )

    def _normalized_attempt_record_evidence(
        self,
        artifact_dir: Path,
        sensitivity_snapshot: dict[str, Any] | None,
        score: AttemptScore,
        compare_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        req_h = self._requested_horizon_from_artifact_dir(artifact_dir)
        req_tf, snap_eff_tf = self._timeframes_from_sensitivity_snapshot(
            sensitivity_snapshot
        )
        resolved_trades: int | None = None
        trades_per_month: float | None = None
        positive_cell_ratio: float | None = None
        eff_tf: str | None = snap_eff_tf
        best = score.best_summary if isinstance(score.best_summary, dict) else {}
        eff_w, eff_src = self._extract_effective_window_months(
            best, sensitivity_snapshot, compare_payload
        )
        bc = best.get("best_cell") if isinstance(best.get("best_cell"), dict) else {}
        if bc:
            rt = bc.get("resolved_trades")
            if isinstance(rt, int):
                resolved_trades = rt
            elif isinstance(rt, float):
                resolved_trades = int(rt)
        mw = best.get("market_data_window")
        if isinstance(mw, dict):
            mreq = mw.get("requested_timeframe")
            meff = mw.get("effective_timeframe")
            if isinstance(mreq, str) and mreq.strip():
                req_tf = mreq.strip()
            if isinstance(meff, str) and meff.strip():
                eff_tf = meff.strip()
        if isinstance(score.metrics, dict):
            tpm = score.metrics.get("trades_per_month")
            if isinstance(tpm, (int, float)):
                trades_per_month = float(tpm)
            pcr = score.metrics.get("positive_cell_ratio")
            if isinstance(pcr, (int, float)):
                positive_cell_ratio = float(pcr)
        support_attempt = {
            "best_summary": best if isinstance(best, dict) else {},
            "effective_window_months": eff_w,
        }
        (
            resolved_trades,
            trades_per_month,
            positive_cell_ratio,
        ) = self._resolve_support_metrics(
            support_attempt,
            resolved_trades=resolved_trades,
            trades_per_month=trades_per_month,
            positive_ratio=positive_cell_ratio,
        )
        cov_status, _ = vo.classify_coverage(
            requested_horizon_months=req_h,
            effective_window_months=eff_w,
            effective_coverage_min_ratio=self.config.research.effective_coverage_min_ratio,
        )
        val_out: str | None = None
        if cov_status == vo.COVERAGE_UNRESOLVED:
            val_out = vo.VALIDATION_UNRESOLVED
        elif cov_status == vo.COVERAGE_INADEQUATE:
            val_out = vo.VALIDATION_FAILED
        job_status: str | None = None
        if (artifact_dir / "deep-replay-job.json").exists():
            job_status = "deep_replay_job_present"
        return {
            "requested_horizon_months": req_h,
            "effective_window_months": eff_w,
            "effective_window_source": eff_src,
            "requested_timeframe": req_tf,
            "effective_timeframe": eff_tf,
            "validation_outcome": val_out,
            "coverage_status": cov_status,
            "job_status": job_status,
            "resolved_trades": resolved_trades,
            "trades_per_month": trades_per_month,
            "positive_cell_ratio": positive_cell_ratio,
        }

    def _scored_attempt_count(self, tool_context: ToolContext) -> int:
        return sum(
            1
            for row in self._run_attempts(tool_context.run_id)
            if row.get("composite_score") is not None
        )

    def _finalize_attempt_branch_state(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        *,
        auto_log: dict[str, Any],
        cli_args: list[str] | None,
        source_action: dict[str, Any],
        timeframe_mismatch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """After a new scored attempt: exploit counters, retention gating (requested horizon), branch lifecycle."""
        artifact_dir_str = str(auto_log.get("artifact_dir") or "").strip()
        if not artifact_dir_str:
            return {}
        score = auto_log.get("composite_score")
        if score is None:
            return {}
        profile_ref_for_family = auto_log.get("profile_ref")
        profile_path_for_family = (
            self.profile_sources.get(profile_ref_for_family)
            if profile_ref_for_family
            else None
        )
        family_id = self._derive_family_id_from_profile(profile_path_for_family)
        if not family_id:
            return {}

        is_exploit = self._is_same_family_exploit_action(source_action)
        artifact_path = Path(artifact_dir_str).resolve()
        attempt_id_log = (
            str(auto_log.get("attempt_id")).strip()
            if auto_log.get("attempt_id")
            else None
        )
        ledger_row = (
            self._attempt_row_for_id(tool_context, attempt_id_log)
            if attempt_id_log
            else None
        )
        attempt_dict = ledger_row
        if attempt_dict is None:
            attempts_list = self._run_attempts(tool_context.run_id)
            attempt_dict = attempts_list[-1] if attempts_list else None

        resolved_trades, trades_per_month, positive_ratio = self._resolve_support_metrics(
            attempt_dict,
            resolved_trades=auto_log.get("resolved_trades"),
            trades_per_month=auto_log.get("trades_per_month"),
            positive_ratio=auto_log.get("positive_cell_ratio"),
        )

        requested_horizon: int | None = self._requested_horizon_from_source_action(
            source_action
        )
        if requested_horizon is None and cli_args is not None:
            requested_horizon = self._parse_lookback_months_from_cli_args(cli_args)
        if requested_horizon is None and isinstance(ledger_row, dict):
            requested_horizon = self._positive_int_or_none(
                ledger_row.get("requested_horizon_months")
            )
        if requested_horizon is None:
            requested_horizon = self._requested_horizon_from_artifact_dir(artifact_path)

        eff_raw = auto_log.get("effective_window_months")
        eff_float: float | None = None
        if isinstance(eff_raw, (int, float)):
            eff_float = float(eff_raw)
        elif eff_raw is not None and str(eff_raw).strip() not in {"", "none"}:
            try:
                eff_float = float(str(eff_raw).strip())
            except (TypeError, ValueError):
                eff_float = None
        if eff_float is None and isinstance(ledger_row, dict):
            lew = ledger_row.get("effective_window_months")
            if isinstance(lew, (int, float)):
                eff_float = float(lew)
            elif lew is not None and str(lew).strip() not in {"", "none"}:
                try:
                    eff_float = float(str(lew).strip())
                except (TypeError, ValueError):
                    eff_float = None

        support_quality = self._cadence_support_quality(
            trades_per_month=trades_per_month,
            positive_ratio=positive_ratio,
            effective_window_months=eff_float,
        )
        self._update_family_exploit_state(family_id, is_exploit, support_quality)

        retention_result = self._check_retention_gating(
            tool_context,
            family_id,
            float(score),
            requested_horizon,
        )
        digest = (
            self._generate_behavior_digest(attempt_dict)
            if isinstance(attempt_dict, dict)
            else {}
        )
        req_tf_a = None
        eff_tf_a = None
        if isinstance(ledger_row, dict):
            rt = ledger_row.get("requested_timeframe")
            et = ledger_row.get("effective_timeframe")
            if isinstance(rt, str) and rt.strip():
                req_tf_a = rt.strip()
            if isinstance(et, str) and et.strip():
                eff_tf_a = et.strip()
        if req_tf_a is None or eff_tf_a is None:
            snap_tf = load_sensitivity_snapshot(artifact_path)
            s_req, s_eff = self._timeframes_from_sensitivity_snapshot(
                snap_tf if isinstance(snap_tf, dict) else None
            )
            if req_tf_a is None:
                req_tf_a = s_req
            if eff_tf_a is None:
                eff_tf_a = s_eff
        if req_tf_a is None and isinstance(attempt_dict, dict):
            rt = attempt_dict.get("requested_timeframe")
            if isinstance(rt, str) and rt.strip():
                req_tf_a = rt.strip()
        if eff_tf_a is None and isinstance(attempt_dict, dict):
            et = attempt_dict.get("effective_timeframe")
            if isinstance(et, str) and et.strip():
                eff_tf_a = et.strip()
        had_mismatch = timeframe_mismatch is not None
        if not had_mismatch:
            timeframe_mismatch = self._record_structured_timeframe_mismatch(
                req_tf_a,
                eff_tf_a,
                source="attempt_record",
            )
            had_mismatch = timeframe_mismatch is not None

        ew_src = auto_log.get("effective_window_source")
        effective_window_source = (
            str(ew_src).strip()
            if isinstance(ew_src, str) and ew_src.strip()
            else None
        )
        self._refresh_branch_lifecycle_after_eval(
            tool_context,
            step,
            step_limit,
            policy,
            family_id=family_id,
            profile_ref=str(profile_ref_for_family).strip()
            if profile_ref_for_family
            else None,
            attempt_id=(
                str(auto_log.get("attempt_id"))
                if auto_log.get("attempt_id")
                else None
            ),
            score=float(score),
            requested_horizon_months=requested_horizon,
            effective_window_months=eff_float,
            retention_result=retention_result,
            behavior_digest=digest,
            had_timeframe_mismatch=had_mismatch,
            requested_timeframe=req_tf_a,
            effective_timeframe=eff_tf_a,
            effective_window_source=effective_window_source,
        )
        return retention_result if isinstance(retention_result, dict) else {}

    def _family_id_for_profile_ref(self, profile_ref: str | None) -> str | None:
        if not profile_ref:
            return None
        path = self.profile_sources.get(str(profile_ref).strip())
        return self._derive_family_id_from_profile(path)

    def _family_id_from_cli_args(self, args: list[str]) -> str | None:
        if "--profile-ref" not in args:
            return None
        idx = args.index("--profile-ref") + 1
        if idx >= len(args):
            return None
        return self._family_id_for_profile_ref(str(args[idx]).strip())

    def _resolve_cli_family_id(self, args: list[str]) -> str | None:
        fid = self._family_id_from_cli_args(args)
        if fid:
            return fid
        if (
            len(args) >= 2
            and str(args[0]).lower() == "profiles"
            and str(args[1]).lower() == "patch"
            and "--file" in args
        ):
            fi = args.index("--file") + 1
            if fi < len(args):
                return self._derive_family_id_from_profile(
                    Path(str(args[fi])).resolve()
                )
        return None

    def _profile_ref_for_local_file(self, path: Path) -> str | None:
        try:
            resolved = path.resolve()
        except OSError:
            return None
        for ref, src in self.profile_sources.items():
            try:
                if src.resolve() == resolved:
                    return str(ref).strip()
            except OSError:
                continue
        return None

    def _synthetic_cli_args_for_branch_validation(
        self, action: dict[str, Any]
    ) -> list[str] | None:
        tool = str(action.get("tool", "")).strip()
        if tool == "run_cli":
            try:
                return [str(item) for item in self._normalize_cli_args(action)]
            except Exception:
                return None
        if tool == "evaluate_candidate":
            ref = action.get("profile_ref")
            candidate_name = action.get("candidate_name")
            args = []
            if isinstance(ref, str) and ref.strip():
                args = ["sensitivity-basket", "--profile-ref", ref.strip()]
            elif isinstance(candidate_name, str) and candidate_name.strip():
                args = ["sensitivity-basket", "--profile-ref", candidate_name.strip()]
            else:
                return None
            rh = self._positive_int_or_none(action.get("requested_horizon_months"))
            if rh is not None:
                args.extend(["--lookback-months", str(rh)])
            return args
        if tool == "run_parameter_sweep":
            ref = action.get("profile_ref")
            if isinstance(ref, str) and ref.strip():
                return ["sweep", "run", "--profile-ref", ref.strip()]
            return None
        if tool == "mutate_profile":
            candidate_name = action.get("candidate_name")
            if isinstance(candidate_name, str) and candidate_name.strip():
                return ["profiles", "patch", "--file", candidate_name.strip()]
            return None
        return None

    def _cli_action_hits_family_exploit_surface(
        self, args: list[str] | None, family_id: str | None
    ) -> bool:
        if not family_id:
            return False
        if not isinstance(args, list):
            return False
        head = [str(a).lower() for a in args[:3]]
        if not head:
            return False
        if head[0] in {"sensitivity", "sensitivity-basket"}:
            return True
        if head[0] == "sweep":
            return True
        if len(head) >= 2 and head[:2] == ["profiles", "patch"]:
            return True
        return False

    def _branch_step_maintenance(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> None:
        for branch in self._family_branches.values():
            if branch.bankrupt and branch.cooldown_until_step <= step:
                branch.bankrupt = False
                if branch.hard_dead:
                    if branch.exploit_dead:
                        branch.lifecycle_state = bl.LIFECYCLE_RESEED_ELIGIBLE
                else:
                    branch.exploit_dead = False
                    if branch.lifecycle_state in {
                        bl.LIFECYCLE_COLLAPSED,
                        bl.LIFECYCLE_RESEED_ELIGIBLE,
                    }:
                        branch.lifecycle_state = bl.LIFECYCLE_SCOUT
        if (
            self._branch_overlay.reseed_active
            and self._branch_overlay.collapse_recovery_remaining > 0
        ):
            self._branch_overlay.collapse_recovery_remaining -= 1
            if self._branch_overlay.collapse_recovery_remaining <= 0:
                self._branch_overlay.reseed_active = False
        self._sync_branch_budget_mode(step, step_limit, policy)
        if (
            not self._branch_overlay.validated_leader_family_id
            and self._branch_overlay.provisional_leader_family_id
            and self._branch_overlay.budget_mode == bl.BUDGET_VALIDATION
            and not self._branch_overlay.reseed_active
        ):
            self._validation_stale_without_validated += 1
        else:
            self._validation_stale_without_validated = 0
        thr = self.config.research.reseed_after_stale_validation_steps
        if (
            self.config.manager.enabled
            and self.manager_providers
            and thr > 0
            and self._validation_stale_without_validated == thr
        ):
            self._manager_invoke_for_hook(
                ManagerHookEvent.on_stale_validation_without_validated,
                tool_context,
                step,
                step_limit,
                policy,
                extra_issues=["stale_validation_threshold_reached"],
            )
            self._sync_branch_budget_mode(step, step_limit, policy)
            self._persist_branch_runtime_state(tool_context, step)

    def _sync_branch_budget_mode(
        self,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> None:
        bmech.sync_branch_budget_mode(self, step, step_limit, policy)

    def _manager_invoke_for_hook(
        self,
        hook: ManagerHookEvent,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        *,
        extra_issues: list[str] | None = None,
    ) -> bool:
        """Return True when manager returned actions and all applied successfully."""
        if not self.config.manager.enabled or not self.manager_providers:
            return False
        packet = mgr_packet.build_manager_packet(
            self,
            tool_context,
            hook,
            step,
            step_limit,
            policy,
            extra_issues=extra_issues,
        )
        user_msg = mgr_packet.manager_user_message(packet)
        last_err: str | None = None
        for mgr_label, _profile_name, provider in self.manager_providers:
            try:
                with self._provider_scope(
                    tool_context=tool_context,
                    step=step,
                    label=f"manager:{mgr_label}",
                    phase="manager",
                    provider=provider,
                ):
                    last_raw = provider.complete_json(
                        [
                            ChatMessage(
                                role="system",
                                content=mgr_packet.MANAGER_SYSTEM_PROMPT,
                            ),
                            ChatMessage(role="user", content=user_msg),
                        ]
                    )
            except ProviderError as exc:
                last_err = str(exc)
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="manager",
                    status="failed",
                    message="Manager request failed.",
                    error=exc,
                    level="warning",
                )
                continue
            if not isinstance(last_raw, dict):
                last_err = "non_object_response"
                continue
            decision = mgr_actions.parse_manager_decision(last_raw)
            if decision is None:
                last_err = "parse_failed"
                continue
            if not decision.actions:
                self._manager_runtime.record_invocation(
                    hook=hook,
                    step=step,
                    rationale=decision.rationale,
                    actions_applied=[],
                    raw_ok=True,
                    error=None,
                    invocation_incomplete=False,
                )
                self._record_pending_manager_event(
                    hook=hook,
                    status="no_change",
                    rationale=decision.rationale,
                )
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="manager",
                    status="no_change",
                    message=f"Manager hook {hook.value} returned no state change.",
                )
                return True
            applied = mgr_actions.apply_manager_decision(
                self, tool_context, decision, step, step_limit, policy
            )
            ok = all(x.get("ok", True) for x in applied)
            self._manager_runtime.record_invocation(
                hook=hook,
                step=step,
                rationale=decision.rationale,
                actions_applied=applied,
                raw_ok=ok,
                error=None if ok else "action_failed",
                invocation_incomplete=not ok,
            )
            self._record_pending_manager_event(
                hook=hook,
                status="ok" if ok else "partial",
                action_count=len(applied),
                rationale=decision.rationale,
                error=None if ok else "action_failed",
            )
            self._trace_runtime(
                tool_context,
                step=step,
                phase="manager",
                status="ok" if ok else "partial",
                message=f"Manager hook {hook.value} — {len(applied)} action(s).",
            )
            return bool(ok)
        self._manager_runtime.record_invocation(
            hook=hook,
            step=step,
            rationale=None,
            actions_applied=[],
            raw_ok=False,
            error=last_err or "no_provider",
            invocation_incomplete=True,
        )
        self._record_pending_manager_event(
            hook=hook,
            status="failed",
            error=last_err or "no_provider",
        )
        return False

    def _refresh_branch_lifecycle_after_eval(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        *,
        family_id: str | None,
        profile_ref: str | None,
        attempt_id: str | None,
        score: float,
        requested_horizon_months: int | None,
        effective_window_months: float | None,
        retention_result: dict[str, Any] | None,
        behavior_digest: dict[str, Any] | None,
        had_timeframe_mismatch: bool,
        requested_timeframe: str | None = None,
        effective_timeframe: str | None = None,
        effective_window_source: str | None = None,
    ) -> None:
        if not family_id:
            return
        try:
            sc = float(score)
            frontier_improved = self._frontier_prior_best is None or self._score_better(
                sc, float(self._frontier_prior_best)
            )
        except (TypeError, ValueError):
            frontier_improved = False
        mgr_on = bool(self.config.manager.enabled and self.manager_providers)
        bmech.refresh_family_after_scored_eval(
            self,
            tool_context,
            step,
            step_limit,
            family_id=family_id,
            profile_ref=profile_ref,
            attempt_id=attempt_id,
            score=score,
            requested_horizon_months=requested_horizon_months,
            effective_window_months=effective_window_months,
            retention_result=retention_result,
            behavior_digest=behavior_digest,
            had_timeframe_mismatch=had_timeframe_mismatch,
            requested_timeframe=requested_timeframe,
            effective_timeframe=effective_timeframe,
            effective_window_source=effective_window_source,
        )
        if mgr_on:
            extra: list[str] = []
            if had_timeframe_mismatch:
                extra.append("timeframe_mismatch")
            rr = retention_result or {}
            if rr.get("retention_failed"):
                extra.append("explicit_retention_fail")
            digest_snap = self._branch_overlay.last_scored_validation_digest
            unresolved_validation = False
            if isinstance(digest_snap, dict):
                ve = digest_snap.get("validation_evidence")
                if isinstance(ve, dict) and ve.get("outcome") == vo.VALIDATION_UNRESOLVED:
                    unresolved_validation = True
                    extra.append("unresolved_validation")
            if frontier_improved:
                extra.append("candidate_frontier_change")
            hook = mgr_hooks.select_post_eval_hook(
                had_timeframe_mismatch=had_timeframe_mismatch,
                explicit_retention_fail=bool(rr.get("retention_failed")),
                unresolved_validation=unresolved_validation,
                frontier_improved=frontier_improved,
            )
            self._manager_invoke_for_hook(
                hook,
                tool_context,
                step,
                step_limit,
                policy,
                extra_issues=extra,
            )
        self._sync_branch_budget_mode(step, step_limit, policy)
        self._persist_branch_runtime_state(tool_context, step)
        self._maybe_update_frontier_prior_best(tool_context)

    def _maybe_update_frontier_prior_best(self, tool_context: ToolContext) -> None:
        br = self._best_attempt(self._run_attempts(tool_context.run_id))
        if not br or br.get("composite_score") is None:
            return
        try:
            gb = float(br["composite_score"])
        except (TypeError, ValueError):
            return
        if self._frontier_prior_best is None or self._score_better(
            gb, float(self._frontier_prior_best)
        ):
            self._frontier_prior_best = gb

    def _persist_branch_runtime_state(self, tool_context: ToolContext, step: int) -> None:
        snapshot = self._build_branch_runtime_snapshot(tool_context, step)
        path = self._runtime_state_path(tool_context)
        prior: dict[str, Any] = {}
        if path.exists():
            try:
                prior = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                prior = {}
        if not isinstance(prior, dict):
            prior = {}
        prior["controller"] = snapshot
        prior["controller_updated_at"] = datetime.now(timezone.utc).isoformat()
        prior["manager"] = self._manager_runtime.to_snapshot_dict()
        path.write_text(json.dumps(prior, ensure_ascii=True, indent=2), encoding="utf-8")

    def _build_branch_runtime_snapshot(
        self, tool_context: ToolContext, step: int
    ) -> dict[str, Any]:
        overlay = self._branch_overlay
        attempts = self._run_attempts(tool_context.run_id)
        wrap_up_focus = self._current_wrap_up_focus_state(attempts)
        run_outcome = self._run_outcome_snapshot(attempts)
        lifecycle_collapsed_ids = [
            fid
            for fid, st in self._family_branches.items()
            if st.lifecycle_state == bl.LIFECYCLE_COLLAPSED
        ]
        exploit_dead_ids = [
            fid for fid, st in self._family_branches.items() if st.exploit_dead
        ]
        suppressed_ids = sorted(
            set(lifecycle_collapsed_ids) | set(exploit_dead_ids),
            key=lambda x: x,
        )
        cooldown = [
            {
                "family_id": fid[:24] + ("..." if len(fid) > 24 else ""),
                "until_step": st.cooldown_until_step,
            }
            for fid, st in self._family_branches.items()
            if st.bankrupt and st.cooldown_until_step > step
        ]
        return {
            "step": step,
            "run_id": tool_context.run_id,
            "provisional_leader_family_prefix": (
                (overlay.provisional_leader_family_id or "")[:20] + "..."
                if overlay.provisional_leader_family_id
                else None
            ),
            "validated_leader_family_prefix": (
                (overlay.validated_leader_family_id or "")[:20] + "..."
                if overlay.validated_leader_family_id
                else None
            ),
            "shadow_leader_family_prefix": (
                (overlay.shadow_leader_family_id or "")[:20] + "..."
                if overlay.shadow_leader_family_id
                else None
            ),
            "shadow_leader_reason": overlay.shadow_leader_reason,
            "provisional_leader_promotability": overlay.provisional_leader_promotability,
            "budget_mode": overlay.budget_mode,
            "reseed_active": overlay.reseed_active,
            "reseed_started_step": overlay.reseed_started_step,
            "collapse_recovery_remaining": overlay.collapse_recovery_remaining,
            "explored_family_count": overlay.explored_family_count,
            "collapsed_families_count": len(lifecycle_collapsed_ids),
            "collapsed_family_prefixes": [
                c[:16] + "..." for c in lifecycle_collapsed_ids[:12]
            ],
            "exploit_dead_families_count": len(exploit_dead_ids),
            "exploit_dead_family_prefixes": [
                c[:16] + "..." for c in exploit_dead_ids[:12]
            ],
            "policy_suppressed_families_count": len(suppressed_ids),
            "policy_suppressed_family_prefixes": [
                c[:16] + "..." for c in suppressed_ids[:12]
            ],
            "last_scored_validation_digest": overlay.last_scored_validation_digest,
            "wrap_up_focus": wrap_up_focus,
            "run_outcome": run_outcome,
            "families_on_cooldown": cooldown,
            "families": {
                k[:24] + ("..." if len(k) > 24 else ""): v.to_dict()
                for k, v in list(self._family_branches.items())[:40]
            },
            "tool_usage_counts": dict(sorted(self._tool_usage_counts.items())),
            "validation_stale_without_validated": self._validation_stale_without_validated,
            "reseed_stale_validation_threshold": self.config.research.reseed_after_stale_validation_steps,
            "frontier_prior_best": self._frontier_prior_best,
        }

    def _branch_lifecycle_run_packet_text(
        self, tool_context: ToolContext, step: int, step_limit: int
    ) -> str:
        ov = self._branch_overlay
        lines = [
            "Branch lifecycle (authoritative controller/manager state; this outranks raw frontier score when they conflict):",
            f"- budget_mode: {ov.budget_mode}",
            f"- reseed_active: {ov.reseed_active} (collapse_recovery_steps_left={ov.collapse_recovery_remaining})",
            f"- provisional_leader_family: {self._short_family_id(ov.provisional_leader_family_id)}",
            f"- validated_leader_family: {self._short_family_id(ov.validated_leader_family_id)}",
            f"- shadow_leader_family: {self._short_family_id(ov.shadow_leader_family_id)}"
            + (f" ({ov.shadow_leader_reason})" if ov.shadow_leader_reason else ""),
            f"- provisional_leader_promotability: {ov.provisional_leader_promotability or 'n/a'}",
            f"- explored_distinct_families: {ov.explored_family_count}",
        ]
        thr = self.config.research.reseed_after_stale_validation_steps
        if thr > 0 and self._validation_stale_without_validated > 0:
            lines.append(
                f"- validation_stale_without_validated: "
                f"{self._validation_stale_without_validated}/{thr} controller steps "
                f"(reopens exploration / collapse_recovery when threshold hit without a validated leader)"
            )
        dead = [fid for fid, st in self._family_branches.items() if st.exploit_dead]
        if dead:
            lines.append(
                "- exploit_dead_families (same profile_ref sensitivity/sweeps blocked): "
                + ", ".join(d[:12] + "..." for d in dead[:6])
            )
        contrast = any(
            st.structural_contrast_required for st in self._family_branches.values()
        )
        if contrast or ov.budget_mode == bl.BUDGET_COLLAPSE_RECOVERY:
            lines.append(
                "- STRUCTURAL CONTRAST PRIORITY: pivot indicator family, instrument cluster, "
                "timeframe architecture, or directional logic before more same-family tuning."
            )
        if ov.budget_mode == bl.BUDGET_WRAP_UP:
            lines.append(
                "- Wrap-up budget: favor validating or pressure-testing validated survivors; "
                "avoid broad new search unless config allows and no validated leader exists."
            )
        digest = ov.last_scored_validation_digest
        if isinstance(digest, dict) and digest.get("validation_evidence"):
            ev = digest.get("validation_evidence")
            tier = ev.get("evidence_tier") if isinstance(ev, dict) else None
            lines.append(
                "- last_scored_validation: "
                f"family={str(digest.get('family_id') or '')[:32]}"
                f" attempt={digest.get('attempt_id')}"
                f" lifecycle={digest.get('lifecycle_state')}"
                f" retention={digest.get('retention_status')}"
                f" outcome={ev.get('outcome') if isinstance(ev, dict) else '?'}"
                + (f" tier={tier}" if tier else "")
            )
        return "\n".join(lines)

    def _validate_branch_lifecycle_actions(
        self,
        tool_context: ToolContext,
        actions: Any,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> list[str]:
        if not isinstance(actions, list):
            return []
        errors: list[str] = []
        overlay = self._branch_overlay
        for index, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                continue
            args = self._synthetic_cli_args_for_branch_validation(action)
            action_families = self._action_family_ids_for_branch_validation(
                tool_context, action, synthetic_args=args
            )
            if not args:
                family_id = next(iter(action_families), None) if action_families else None
            else:
                family_id = self._resolve_cli_family_id(args)
                if family_id:
                    action_families.add(family_id)
            if not family_id and action_families:
                family_id = next(iter(action_families))
            branch = self._family_branches.get(family_id)
            is_exploit = self._is_same_family_exploit_action(action)
            if branch and branch.exploit_dead and self._cli_action_hits_family_exploit_surface(
                args, family_id
            ):
                errors.append(
                    f"Action {index}: branch lifecycle BLOCK — family {family_id[:16]}... is exploit_dead "
                    "(retention collapse). Do not run sensitivity, sweep, or profiles patch on this profile; "
                    "use a structural contrast (new scaffold/clone path) or different instruments."
                )
                continue
            if overlay.budget_mode == bl.BUDGET_COLLAPSE_RECOVERY and is_exploit:
                errors.append(
                    f"Action {index}: collapse_recovery budget — same-family exploit blocked; "
                    f"prefer structural contrast or validation on a different family."
                )
            if branch and branch.structural_contrast_required and is_exploit:
                errors.append(
                    f"Action {index}: structural contrast required for family {family_id[:16]}... "
                    "— blocked same-family exploit until contrast pivot progresses."
                )
            if overlay.budget_mode == bl.BUDGET_WRAP_UP and not overlay.validated_leader_family_id:
                wrap_up_focus = self._current_wrap_up_focus_state(
                    self._run_attempts(tool_context.run_id)
                )
                focus_family = (
                    str(wrap_up_focus.get("family_id") or "").strip()
                    if isinstance(wrap_up_focus, dict)
                    else ""
                )
                if focus_family:
                    if action_families and any(
                        fam and fam != focus_family for fam in action_families
                    ):
                        errors.append(
                            f"Action {index}: wrap_up focus is {focus_family[:16]}... "
                            "— block unrelated family work until the focus path is resolved or invalidated."
                        )
                        continue
                    if not self._is_decisive_wrap_up_focus_action(
                        tool_context,
                        action,
                        focus_family=focus_family,
                        step=step,
                        step_limit=step_limit,
                        action_families=action_families,
                    ):
                        errors.append(
                            f"Action {index}: wrap_up focus is {focus_family[:16]}... "
                            "— only decisive focus-path actions are allowed here. "
                            "Use evaluate_candidate on the focus family, or inspect/compare that focus path only when a follow-up step remains."
                        )
        return errors

    def _attempt_row_for_artifact_dir(
        self, tool_context: ToolContext, artifact_dir: str | Path
    ) -> dict[str, Any] | None:
        needle = str(artifact_dir).strip()
        if not needle:
            return None
        try:
            needle_path = str(Path(needle).resolve())
        except (OSError, RuntimeError, TypeError, ValueError):
            needle_path = needle
        for att in load_run_attempts(tool_context.run_dir):
            if not isinstance(att, dict):
                continue
            artifact = str(att.get("artifact_dir") or "").strip()
            if not artifact:
                continue
            try:
                artifact_path = str(Path(artifact).resolve())
            except (OSError, RuntimeError, TypeError, ValueError):
                artifact_path = artifact
            if artifact_path == needle_path:
                return att
        return None

    def _action_family_ids_for_branch_validation(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        synthetic_args: list[str] | None = None,
    ) -> set[str]:
        families: set[str] = set()
        args = synthetic_args
        if args:
            family_id = self._resolve_cli_family_id(args)
            if family_id:
                families.add(family_id)
        tool = str(action.get("tool", "")).strip()
        if tool == "inspect_artifact":
            attempt_id = action.get("attempt_id")
            if isinstance(attempt_id, str) and attempt_id.strip():
                attempt = self._attempt_row_for_id(tool_context, attempt_id.strip())
                if isinstance(attempt, dict):
                    family_id = self._family_id_for_profile_ref(
                        str(attempt.get("profile_ref") or "").strip()
                    )
                    if family_id:
                        families.add(family_id)
            artifact_dir = action.get("artifact_dir")
            if isinstance(artifact_dir, str) and artifact_dir.strip():
                attempt = self._attempt_row_for_artifact_dir(tool_context, artifact_dir)
                if isinstance(attempt, dict):
                    family_id = self._family_id_for_profile_ref(
                        str(attempt.get("profile_ref") or "").strip()
                    )
                    if family_id:
                        families.add(family_id)
        if tool == "compare_artifacts":
            entries = action.get("attempt_ids") or action.get("artifact_dirs")
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, str) or not entry.strip():
                        continue
                    token = entry.strip()
                    attempt = self._attempt_row_for_id(tool_context, token)
                    if not isinstance(attempt, dict):
                        attempt = self._attempt_row_for_artifact_dir(tool_context, token)
                    if isinstance(attempt, dict):
                        family_id = self._family_id_for_profile_ref(
                            str(attempt.get("profile_ref") or "").strip()
                        )
                        if family_id:
                            families.add(family_id)
        return families

    def _is_decisive_wrap_up_focus_action(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        focus_family: str,
        step: int,
        step_limit: int,
        action_families: set[str] | None = None,
    ) -> bool:
        tool = str(action.get("tool", "")).strip()
        families = {fam for fam in (action_families or set()) if fam}
        steps_remaining_after = max(step_limit - step, 0)
        if tool == "evaluate_candidate":
            return families == {focus_family}
        if tool == "inspect_artifact":
            return families == {focus_family} and steps_remaining_after >= 1
        if tool == "compare_artifacts":
            return families == {focus_family} and steps_remaining_after >= 1
        return False

    def _derive_family_id_from_profile(self, profile_path: Path | None) -> str | None:
        if profile_path is None or not profile_path.exists():
            return None
        try:
            payload = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        profile = (
            payload.get("profile")
            if isinstance(payload.get("profile"), dict)
            else payload
        )
        indicators = profile.get("indicators") if isinstance(profile, dict) else None
        if not isinstance(indicators, list) or not indicators:
            return None
        instance_ids = []
        for ind in indicators:
            if not isinstance(ind, dict):
                continue
            meta = ind.get("meta") if isinstance(ind.get("meta"), dict) else {}
            inst_id = str(meta.get("instanceId") or "").strip()
            if inst_id:
                instance_ids.append(inst_id)
        if not instance_ids:
            return None
        return "|".join(sorted(instance_ids))

    def _derive_support_quality(self, attempt: dict[str, Any]) -> str:
        trades_per_month = self._attempt_trades_per_month(attempt)
        positive_ratio = self._attempt_positive_cell_ratio(attempt)
        effective_window_months = self._attempt_effective_window_months(attempt)
        return self._cadence_support_quality(
            trades_per_month=trades_per_month,
            positive_ratio=positive_ratio,
            effective_window_months=effective_window_months,
        )

    def _resolve_support_metrics(
        self,
        attempt: dict[str, Any] | None,
        *,
        resolved_trades: Any = None,
        trades_per_month: Any = None,
        positive_ratio: Any = None,
    ) -> tuple[Any, Any, Any]:
        if not isinstance(attempt, dict):
            return resolved_trades, trades_per_month, positive_ratio
        if resolved_trades is None:
            resolved_trades = self._attempt_trade_count(attempt)
        if trades_per_month is None:
            trades_per_month = self._attempt_trades_per_month(attempt)
        if positive_ratio is None:
            positive_ratio = self._attempt_positive_cell_ratio(attempt)
        return resolved_trades, trades_per_month, positive_ratio

    def _cadence_support_quality(
        self,
        *,
        trades_per_month: Any,
        positive_ratio: Any,
        effective_window_months: Any,
    ) -> str:
        tpm = (
            float(trades_per_month)
            if isinstance(trades_per_month, (int, float))
            else None
        )
        pos = (
            float(positive_ratio) if isinstance(positive_ratio, (int, float)) else None
        )
        eff = self._coerce_float_months(effective_window_months)
        if tpm is None:
            if eff is not None and eff < 2.0:
                return "selective"
            if pos is not None and pos < 0.3:
                return "selective"
            return "sparse"
        if tpm < 1.5:
            return "sparse"
        if tpm < 3.0:
            return "selective"
        if eff is not None and eff < 3.0 and tpm < 5.0:
            return "selective"
        if pos is not None and pos < 0.3:
            return "selective"
        return "broad"

    def _evaluate_candidate_is_retention_style(self, action: dict[str, Any]) -> bool:
        mode = str(action.get("evaluation_mode") or "screen").strip().lower()
        if mode in {
            "validate",
            "validation",
            "portability",
            "portability_check",
            "pressure_test",
            "pressure",
        }:
            return True
        rh = action.get("requested_horizon_months")
        if rh is None:
            return False
        try:
            return int(rh) >= int(self.config.research.validated_leader_min_horizon_months)
        except (TypeError, ValueError):
            return False

    def _is_same_family_exploit_action(self, action: dict[str, Any]) -> bool:
        tool = str(action.get("tool", "")).strip()
        if tool == "evaluate_candidate":
            return not self._evaluate_candidate_is_retention_style(action)
        if tool == "run_parameter_sweep":
            return True
        if tool == "mutate_profile":
            return True
        if tool != "run_cli":
            return False
        args = action.get("args")
        if not isinstance(args, list):
            command = str(action.get("command", "")).lower()
            args = command.split()
        args_lower = [str(a).lower() for a in args]
        if (
            args_lower
            and args_lower[0] in {"sensitivity", "sensitivity-basket"}
            and "--lookback-months" in args_lower
        ):
            idx = args_lower.index("--lookback-months") + 1
            if idx < len(args_lower):
                try:
                    if int(args_lower[idx]) >= int(
                        self.config.research.validated_leader_min_horizon_months
                    ):
                        return False
                except (TypeError, ValueError):
                    pass
        args_str = " ".join(args_lower)
        exploit_patterns = [
            "notificationthreshold",
            "lookbackbars",
            ".weight",
            "patch",
        ]
        sweep_subcommands = {"scaffold", "patch", "submit"}
        if args_lower and args_lower[0] == "sweep" and len(args_lower) >= 2:
            subcommand = args_lower[1]
            if subcommand in sweep_subcommands:
                return True
        for pattern in exploit_patterns:
            if pattern in args_str:
                return True
        return False

    def _check_retention_gating(
        self,
        tool_context: ToolContext,
        candidate_family_id: str,
        current_score: float,
        horizon_months: int | None = None,
    ) -> dict[str, Any]:
        cfg = self.config.research
        branch = bl.ensure_family_branch(self._family_branches, candidate_family_id)
        threshold = cfg.retention_strong_candidate_threshold
        max_mutations = cfg.retention_max_same_family_mutations_before_check
        current_mutations = self._family_mutation_counts.get(candidate_family_id, 0)
        last_score = branch.retention_last_eval_score
        baseline_score = branch.retention_baseline_score
        last_horizon = branch.retention_last_horizon
        is_strong = current_score >= threshold
        support_quality = branch.retention_support_quality or "normal"
        needs_retention_check = False
        gated_message = None
        min_h = int(cfg.validated_leader_min_horizon_months)
        horizon_increased = False
        if horizon_months is not None:
            try:
                hm = int(horizon_months)
            except (TypeError, ValueError):
                hm = 0
            if last_horizon is None:
                horizon_increased = bool(
                    baseline_score is not None and hm >= min_h
                )
            else:
                horizon_increased = hm > int(last_horizon)
        if is_strong and current_mutations >= max_mutations:
            if branch.retention_check_passed is not True:
                if not branch.retention_check_done:
                    needs_retention_check = True
                    if support_quality == "sparse":
                        suggested_months = cfg.retention_check_months_sparse
                    else:
                        suggested_months = cfg.retention_check_months_normal
                    gated_message = (
                        f"Family {candidate_family_id[:16]}... is a strong candidate (score={current_score:.1f}) "
                        f"but requires a retention check at {suggested_months}m before further same-family exploit. "
                        f"Current same-family mutations={current_mutations} (max allowed={max_mutations}). "
                        f"Suggested next move: run a longer-horizon validation or pivot to a structural contrast branch."
                    )
        if (
            horizon_increased
            and baseline_score is not None
            and current_score < baseline_score
        ):
            delta = current_score - baseline_score
            ratio = current_score / baseline_score if baseline_score != 0 else 0
            if delta <= cfg.retention_fail_delta or ratio < cfg.retention_fail_ratio:
                branch.retention_check_done = True
                branch.retention_check_passed = False
                branch.retention_last_delta = delta
                branch.retention_last_ratio = ratio
                branch.retention_last_horizon = horizon_months
                return {
                    "family_id": candidate_family_id,
                    "retention_failed": True,
                    "delta": delta,
                    "ratio": ratio,
                    "baseline_score": baseline_score,
                    "current_horizon": horizon_months,
                    "message": (
                        f"Retention check FAILED for family {candidate_family_id[:16]}... "
                        f"(delta={delta:.1f}, ratio={ratio:.2f}) at {horizon_months}m horizon vs {baseline_score:.1f} baseline. "
                        f"Next step must be a structural contrast, not another same-family tweak. "
                        f"Disallowed: notificationThreshold tweak, lookbackBars tweak, range-width tweak, same-family sweep. "
                        f"Allowed: different indicator family, different instrument cluster, different timeframe architecture, different directional logic."
                    ),
                }
        if last_score is not None:
            delta = current_score - last_score
            if (
                delta >= cfg.retention_pass_delta
                and current_score >= threshold
            ):
                branch.retention_check_done = True
                branch.retention_check_passed = True
                branch.retention_last_delta = delta
        branch.retention_last_eval_score = current_score
        if horizon_months is not None:
            branch.retention_last_horizon = horizon_months
        if branch.retention_observational_baseline_score is None:
            branch.retention_observational_baseline_score = current_score
        establish = cfg.retention_baseline_establish_min_score
        if establish is None:
            establish = float(cfg.retention_strong_candidate_threshold)
        else:
            establish = float(establish)
        if branch.retention_baseline_score is None and current_score >= establish:
            branch.retention_baseline_score = current_score
        return {
            "family_id": candidate_family_id,
            "retention_failed": False,
            "needs_retention_check": needs_retention_check,
            "gated_message": gated_message,
        }

    def _update_family_exploit_state(
        self,
        family_id: str | None,
        is_exploit: bool,
        support_quality: str | None = None,
    ) -> None:
        if family_id is None:
            return
        if is_exploit:
            if self._last_family_id == family_id:
                self._consecutive_same_family_exploit += 1
            else:
                self._consecutive_same_family_exploit = 1
            self._same_family_exploit_history.append(family_id)
        else:
            self._consecutive_same_family_exploit = 0
        self._last_family_id = family_id
        if family_id not in self._family_mutation_counts:
            self._family_mutation_counts[family_id] = 0
            fb = bl.ensure_family_branch(self._family_branches, family_id)
            sq = support_quality or fb.retention_support_quality or "normal"
            fb.retention_support_quality = sq
        elif support_quality:
            bl.ensure_family_branch(
                self._family_branches, family_id
            ).retention_support_quality = support_quality
        if is_exploit:
            self._family_mutation_counts[family_id] = (
                self._family_mutation_counts.get(family_id, 0) + 1
            )

    def _get_same_family_exploit_status(self) -> dict[str, Any]:
        cap = self.config.research.same_family_exploit_cap
        return {
            "consecutive_exploit_steps": self._consecutive_same_family_exploit,
            "exploit_cap": cap,
            "at_cap": self._consecutive_same_family_exploit >= cap,
            "message": (
                f"Consecutive same-family exploit steps: {self._consecutive_same_family_exploit}/{cap}. "
                f"After {cap} consecutive same-family exploit steps, the next step must be a structural contrast "
                f"(different indicator family, instrument cluster, timeframe architecture, or directional logic) "
                f"unless retention has recently passed."
            )
            if self._consecutive_same_family_exploit >= cap
            else None,
        }

    def _detect_timeframe_mismatch(
        self, cli_result: dict[str, Any]
    ) -> dict[str, Any] | None:
        result_payload = cli_result.get("result")
        if not isinstance(result_payload, dict):
            return None
        stdout = result_payload.get("stdout", "")
        stderr = result_payload.get("stderr", "")
        combined_output = stdout + "\n" + stderr
        entry = _extract_timeframe_mismatch_from_output(combined_output)
        if entry is None:
            return None
        self._timeframe_mismatches.append(entry)
        return entry

    def _record_structured_timeframe_mismatch(
        self,
        requested_timeframe: Any,
        effective_timeframe: Any,
        *,
        source: str,
    ) -> dict[str, Any] | None:
        entry = _build_timeframe_mismatch_entry(
            requested_timeframe,
            effective_timeframe,
            source=source,
        )
        if entry is None:
            return None
        self._timeframe_mismatches.append(entry)
        return entry

    def _resolve_timeframe_mismatch(
        self,
        cli_result: dict[str, Any],
        *,
        auto_log: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        result_payload = cli_result.get("result")
        if isinstance(result_payload, dict):
            parsed_json = result_payload.get("parsed_json")
            if isinstance(parsed_json, dict):
                req_tf, eff_tf = self._timeframes_from_sensitivity_snapshot(parsed_json)
                entry = self._record_structured_timeframe_mismatch(
                    req_tf,
                    eff_tf,
                    source="parsed_json",
                )
                if entry is not None:
                    return entry
        if isinstance(auto_log, dict):
            entry = self._record_structured_timeframe_mismatch(
                auto_log.get("requested_timeframe"),
                auto_log.get("effective_timeframe"),
                source="auto_log",
            )
            if entry is not None:
                return entry
        return self._detect_timeframe_mismatch(cli_result)

    def _get_timeframe_mismatch_status(self) -> dict[str, Any]:
        if not self._timeframe_mismatches:
            return {"has_mismatch": False}
        latest = self._timeframe_mismatches[-1]
        repeat_block = self.config.research.timeframe_adjustment_repeat_block
        recent_count = sum(
            1
            for m in self._timeframe_mismatches[-5:]
            if m.get("requested") == latest.get("requested")
        )
        return {
            "has_mismatch": True,
            "latest": latest,
            "total_mismatches": len(self._timeframe_mismatches),
            "recent_same_count": recent_count,
            "repeat_blocked": repeat_block and recent_count >= 2,
            "message": str(latest.get("message") or ""),
        }

    def _generate_behavior_digest(self, attempt: dict[str, Any]) -> dict[str, Any]:
        score = attempt.get("composite_score")
        trades_per_month = self._attempt_trades_per_month(attempt)
        effective_window_months = self._attempt_effective_window_months(attempt)
        positive_ratio = self._attempt_positive_cell_ratio(attempt)
        max_drawdown = self._attempt_max_drawdown_r(attempt)
        best_summary = attempt.get("best_summary")
        edge_shape = "flat_weak"
        support_shape = "well_supported"
        drawdown_shape = "smooth"
        retention_risk = "low"
        failure_mode_hint = "none"
        next_move_hint = "local_tune_allowed"
        if score is None:
            edge_shape = "flat_weak"
            support_shape = "too_sparse"
            retention_risk = "high"
            failure_mode_hint = "weak_support"
            next_move_hint = "prune_family"
        elif trades_per_month is not None and trades_per_month < 1.5:
            support_shape = "too_sparse"
            retention_risk = "high"
            failure_mode_hint = "weak_support"
            next_move_hint = "contrast_family"
        elif trades_per_month is not None and trades_per_month < 3:
            support_shape = "sparse_risky"
            retention_risk = "moderate"
        if positive_ratio is not None:
            if positive_ratio > 0.7:
                edge_shape = "persistent"
                support_shape = "well_supported"
            elif positive_ratio > 0.4:
                edge_shape = "episodic"
                support_shape = "selective_but_credible"
            else:
                edge_shape = "one_burst"
                support_shape = "sparse_risky"
                retention_risk = "high"
                failure_mode_hint = "recent_only"
        if max_drawdown is not None:
            if max_drawdown > 10:
                drawdown_shape = "late_blowup"
                retention_risk = "high"
                failure_mode_hint = "trend_regime_dependent"
            elif max_drawdown > 5:
                drawdown_shape = "clustered"
        strong_floor = max(
            float(self.config.research.retention_strong_candidate_threshold),
            float(self.config.research.validated_leader_min_score),
        )
        if (
            score is not None
            and effective_window_months is not None
            and effective_window_months
            < float(self.config.research.validated_leader_min_horizon_months)
            and (
                score <= strong_floor
                if self.config.research.plot_lower_is_better
                else score >= strong_floor
            )
        ):
            if support_shape == "well_supported":
                support_shape = "selective_but_credible"
            if retention_risk == "low":
                retention_risk = "moderate"
            failure_mode_hint = "short_window_spike"
            next_move_hint = "pressure_test_horizon"
        best_summary = attempt.get("best_summary")
        if isinstance(best_summary, dict):
            matrix_summary = best_summary.get("matrix_summary")
            if isinstance(matrix_summary, dict):
                cell_count = matrix_summary.get("cell_count", 0)
                if cell_count > 100:
                    edge_shape = "late_breakdown"
                    failure_mode_hint = "range_regime_dependent"
        if score is not None and score < 40:
            next_move_hint = "stop_threshold_tuning"
        elif retention_risk == "high":
            next_move_hint = "contrast_family"
        return {
            "edge_shape": edge_shape,
            "support_shape": support_shape,
            "drawdown_shape": drawdown_shape,
            "retention_risk": retention_risk,
            "failure_mode_hint": failure_mode_hint,
            "next_move_hint": next_move_hint,
        }

    def _format_behavior_digest_text(self, digest: dict[str, Any]) -> str:
        lines = ["Behavior digest:"]
        for key, value in digest.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines)

    def _system_protocol_text(
        self,
        policy: RunPolicy,
        *,
        tool_context: ToolContext | None = None,
        step: int | None = None,
    ) -> str:
        provider_type = str(self.config.provider.provider_type or "").strip().lower()
        if (
            tool_context is not None
            and step is not None
            and self._is_true_opening_step(tool_context, step)
        ):
            base_protocol = LOCAL_OPENING_STEP_PROTOCOL
        elif provider_type == "transformers_local":
            base_protocol = SFT_SYSTEM_PROTOCOL
        else:
            base_protocol = SYSTEM_PROTOCOL
        durable_appendix = self._durable_system_appendix_text()
        if durable_appendix:
            base_protocol = base_protocol + "\n\n" + durable_appendix
        if policy.allow_finish:
            return base_protocol
        return base_protocol + "\n" + SUPERVISED_EXTRA_RULES

    def _uses_local_transformers_provider(self) -> bool:
        return (
            str(self.config.provider.provider_type or "").strip().lower()
            == "transformers_local"
        )

    def _is_true_opening_step(
        self,
        tool_context: ToolContext,
        step: int | None,
    ) -> bool:
        if step != 1:
            return False
        if self._load_recent_step_payloads(tool_context, 1):
            return False
        if load_run_attempts(tool_context.run_dir):
            return False
        if self.last_created_profile_ref:
            return False
        return True

    def _apply_runtime_interventions(
        self,
        tool_context: ToolContext,
        step: int,
        response: dict[str, Any],
        *,
        phase: str,
    ) -> dict[str, Any]:
        updated = response
        if self._is_true_opening_step(tool_context, step):
            opening_grounding = self._local_opening_grounding_prompt_state(tool_context) or {}
            canonicalized = canonicalize_local_opening_step_response(
                updated,
                starter_instruments=list(
                    opening_grounding.get("preferred_initial_instruments") or []
                ),
                candidate_name_hint=str(
                    opening_grounding.get("candidate_name_hint") or "cand1"
                ),
            )
            if canonicalized != updated:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase=phase,
                    status="ok",
                    message="Applied opening-step runtime canonicalization for prepare_profile scaffold.",
                )
                self._append_raw_explorer_payload(
                    tool_context,
                    step=step,
                    phase=phase,
                    event="opening_runtime_canonicalized",
                    source="controller",
                    label="opening_runtime",
                    payload_json=canonicalized,
                )
            updated = canonicalized
        elif isinstance(updated, dict):
            next_action_template = self._followup_next_action_template_prompt_state(
                tool_context
            )
            canonicalized = canonicalize_followup_step_response(
                updated,
                next_action_template=next_action_template,
            )
            if canonicalized != updated:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase=phase,
                    status="ok",
                    message="Applied follow-up runtime canonicalization for deterministic handle-bound action.",
                )
                self._append_raw_explorer_payload(
                    tool_context,
                    step=step,
                    phase=phase,
                    event="followup_runtime_canonicalized",
                    source="controller",
                    label="followup_runtime",
                    payload_json=canonicalized,
                )
            updated = canonicalized
        compat_normalized = self._pathless_response_compatibility(updated)
        if compat_normalized != updated:
            self._trace_runtime(
                tool_context,
                step=step,
                phase=phase,
                status="ok",
                message="Applied pathless contract compatibility normalization.",
            )
            self._append_raw_explorer_payload(
                tool_context,
                step=step,
                phase=phase,
                event="pathless_contract_normalized",
                source="controller",
                label="pathless_contract",
                payload_json=compat_normalized,
            )
        return compat_normalized

    def _pathless_response_compatibility(
        self,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(response, dict):
            return response
        actions = response.get("actions")
        if not isinstance(actions, list):
            return response
        normalized_actions: list[Any] = []
        changed = False
        for action in actions:
            if not isinstance(action, dict):
                normalized_actions.append(action)
                continue
            normalized_action = _pathless_action_from_legacy_fields(action)
            normalized_actions.append(normalized_action)
            if normalized_action != action:
                changed = True
        if not changed:
            return response
        updated = dict(response)
        updated["actions"] = normalized_actions
        return updated

    def _local_repair_hint_lines(
        self,
        actions: list[Any],
        errors: list[str],
        *,
        next_action_template: dict[str, Any] | None = None,
    ) -> list[str]:
        hints: list[str] = []
        error_blob = "\n".join(errors).lower()
        if "unknown instrument(s): 'all'" in error_blob:
            hints.append(
                "Do not use ALL as an instrument. Use the exact starter instrument symbols from the opening state."
            )
        if "evaluate_candidate requires instruments array" in error_blob:
            hints.append(
                "For evaluate_candidate, include instruments as an explicit array of exact symbols."
            )
        if "requires candidate_name or profile_ref" in error_blob:
            hints.append(
                "For draft profiles, use candidate_name. For registered profiles, use profile_ref."
            )
        if "prepare_profile requires mode" in error_blob:
            for action in actions:
                if not isinstance(action, dict):
                    continue
                if str(action.get("tool") or "").strip() != "prepare_profile":
                    continue
                if action.get("source_candidate_name") or action.get("source_profile_ref") or action.get("source_profile_path"):
                    hints.append(
                        "For prepare_profile cloning, set mode to clone_local and use source_candidate_name or source_profile_ref."
                    )
                elif action.get("indicator_ids"):
                    hints.append(
                        "For prepare_profile with indicator_ids, set mode to scaffold_from_seed."
                    )
                else:
                    hints.append(
                        "prepare_profile must include one of: scaffold_from_seed, clone_local, from_template."
                    )
                break
        if isinstance(next_action_template, dict):
            hints.append(
                "If controller state exposes next_action_template, match that action shell and required fields unless fresh tool evidence clearly contradicts it."
            )
        return hints

    def _compact_repair_messages(
        self,
        draft_payload: Any,
        *,
        errors: list[str],
        shape_error: str | None = None,
        opening_step: bool = False,
        next_action_template: dict[str, Any] | None = None,
    ) -> list[ChatMessage]:
        lines = [RESPONSE_REPAIR_PROMPT]
        if opening_step:
            lines.extend(
                [
                    "",
                    "Opening-step repair rules:",
                    "- Return exactly 1 action only.",
                    "- That action must be prepare_profile.",
                    "- For prepare_profile with indicator_ids, set mode to scaffold_from_seed.",
                    "- Allowed prepare_profile fields only: tool, mode, indicator_ids, instruments, candidate_name.",
                    "- Do not use ALL as an instrument. Use the exact starter instrument symbols from the opening state.",
                    "- Use candidate_name only; the controller resolves local draft storage internally.",
                    "- Do not use profile_name or seed_indicators.",
                    "- Do not chain validate_profile, register_profile, or evaluate_candidate.",
                    "- Return raw JSON only with no Markdown fences, duplicate JSON, or suffix text.",
                ]
            )
        else:
            lines.extend(
                [
                    "",
                    "Later-step repair rules:",
                    "- Use candidate_name for local drafts and profile_ref for registered profiles.",
                    "- Do not emit profile_path, destination_path, or source_profile_path.",
                    "- If the tool is evaluate_candidate, include instruments as an explicit array of exact symbols.",
                    "- Return exactly one raw JSON object only.",
                    "- Do not append extra actions unless controller state clearly calls for them.",
                ]
            )
            if isinstance(next_action_template, dict):
                lines.extend(
                    [
                        "- Prefer matching this next_action_template unless fresh tool evidence clearly contradicts it:",
                        json.dumps(next_action_template, ensure_ascii=True),
                    ]
                )
        if shape_error:
            lines.extend(
                [
                    "",
                    "The previous response was valid JSON but had the wrong top-level shape for the controller.",
                    f"Problem:\n- {shape_error}",
                    "Use the same intent, but convert it into controller actions.",
                    "Do not return a raw scoring-profile JSON document as the top-level response.",
                ]
            )
        elif errors:
            lines.extend(["", "Problems:"])
            lines.extend(f"- {error}" for error in errors)
        hint_lines = self._local_repair_hint_lines(
            draft_payload.get("actions") if isinstance(draft_payload, dict) else [],
            errors,
            next_action_template=next_action_template,
        )
        if hint_lines:
            lines.extend(["", "Deterministic hints:"])
            lines.extend(f"- {item}" for item in hint_lines)
        lines.extend(
            [
                "",
                "Invalid draft:",
                json.dumps(draft_payload, ensure_ascii=True),
            ]
        )
        return [
            ChatMessage(
                role="system",
                content=LOCAL_OPENING_STEP_PROTOCOL if opening_step else SFT_SYSTEM_PROTOCOL,
            ),
            ChatMessage(role="user", content="\n".join(lines)),
        ]

    def _normalize_model_response(
        self, payload: dict[str, Any] | list[Any]
    ) -> dict[str, Any]:
        if isinstance(payload, dict):
            reasoning = payload.get("reasoning")
            action_keys = ("actions", "planned_actions", "tool_calls", "steps")
            for key in action_keys:
                candidate_actions = payload.get(key)
                if isinstance(candidate_actions, list) and all(
                    isinstance(item, dict) for item in candidate_actions
                ):
                    return {
                        "reasoning": str(reasoning).strip()
                        if isinstance(reasoning, str)
                        else "",
                        "actions": candidate_actions,
                    }
            if payload.get("tool"):
                action = dict(payload)
                action.pop("reasoning", None)
                return {
                    "reasoning": str(reasoning).strip()
                    if isinstance(reasoning, str)
                    else "",
                    "actions": [action],
                }
        if isinstance(payload, list) and all(
            isinstance(item, dict) for item in payload
        ):
            return {"reasoning": "", "actions": payload}
        raise RuntimeError(f"Model returned invalid actions payload: {payload}")

    def _parse_wall_time(self, value: str) -> time:
        parsed = datetime.strptime(value, "%H:%M")
        return parsed.time()

    def _within_operating_window(self, policy: RunPolicy) -> bool:
        if not policy.window_start or not policy.window_end:
            return True
        tz = ZoneInfo(policy.timezone_name)
        now_local = datetime.now(tz)
        start = self._parse_wall_time(policy.window_start)
        end = self._parse_wall_time(policy.window_end)
        current = now_local.time().replace(tzinfo=None)
        if start == end:
            return True
        if start < end:
            return start <= current < end
        return current >= start or current < end

    def _minutes_until_window_close(self, policy: RunPolicy) -> float | None:
        if not policy.window_start or not policy.window_end:
            return None
        tz = ZoneInfo(policy.timezone_name)
        now_local = datetime.now(tz)
        start = self._parse_wall_time(policy.window_start)
        end = self._parse_wall_time(policy.window_end)
        current = now_local.time().replace(tzinfo=None)
        if start == end:
            return None
        if start < end:
            if not (start <= current < end):
                return None
            end_dt = datetime.combine(now_local.date(), end, tz)
        else:
            if not (current >= start or current < end):
                return None
            end_dt = datetime.combine(now_local.date(), end, tz)
            if current >= start:
                end_dt += timedelta(days=1)
        return max(0.0, (end_dt - now_local).total_seconds() / 60.0)

    def _soft_wrap_note(self, policy: RunPolicy) -> str | None:
        if policy.soft_wrap_minutes <= 0:
            return None
        minutes_remaining = self._minutes_until_window_close(policy)
        if minutes_remaining is None or minutes_remaining > policy.soft_wrap_minutes:
            return None
        rounded = max(1, int(round(minutes_remaining)))
        return (
            f"Schedule note: the supervise window is ending soon and about {rounded} minute(s) remain. "
            "Finish the current line of inquiry cleanly, avoid starting broad new branches or large fresh searches, "
            "and prefer consolidating evidence over opening new exploration."
        )

    def _normalize_cli_args(self, action: dict[str, Any]) -> list[str]:
        executable_names = {
            self.config.fuzzfolio.cli_command.lower(),
            Path(self.config.fuzzfolio.cli_command).name.lower(),
            "fuzzfolio-agent-cli".lower(),
            "fuzzfolio-agent-cli.exe".lower(),
        }
        args = action.get("args")
        if isinstance(args, list) and args:
            normalized = [str(item) for item in args]
            if normalized and any(char.isspace() for char in normalized[0].strip()):
                expanded_head = shlex.split(normalized[0], posix=False)
                normalized = [*expanded_head, *normalized[1:]]
            first = Path(normalized[0]).name.lower()
            if first in executable_names:
                normalized = normalized[1:]
            if not normalized:
                raise ValueError(
                    "run_cli args list only contained the CLI executable name."
                )
            return self._canonicalize_cli_args(normalized)
        if isinstance(args, str) and args.strip():
            command_text = args.strip()
        else:
            command = action.get("command")
            if not isinstance(command, str) or not command.strip():
                raise ValueError(
                    "run_cli requires a non-empty args list or command string."
                )
            command_text = command.strip()
        parts = shlex.split(command_text, posix=False)
        if not parts:
            raise ValueError("run_cli command string did not contain any tokens.")
        first = Path(parts[0]).name.lower()
        if first in executable_names:
            parts = parts[1:]
        if not parts:
            raise ValueError(
                "run_cli command string only contained the CLI executable name."
            )
        return self._canonicalize_cli_args(parts)

    def _canonicalize_cli_args(self, args: list[str]) -> list[str]:
        normalized = [str(item) for item in args]
        if len(normalized) >= 4 and normalized[0] == "sweep":
            subcommand = normalized[1]
            if subcommand in {"validate", "patch"}:
                canonicalized: list[str] = normalized[:2]
                index = 2
                while index < len(normalized):
                    token = normalized[index]
                    if token == "--file":
                        canonicalized.append("--definition")
                    else:
                        canonicalized.append(token)
                    index += 1
                normalized = canonicalized
        return normalized

    def _parse_cli_help_commands(self, help_text: str) -> dict[str, str]:
        commands: dict[str, str] = {}
        in_commands = False
        for raw_line in help_text.splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()
            if not in_commands:
                if stripped == "Commands:":
                    in_commands = True
                continue
            if not stripped:
                if commands:
                    break
                continue
            if re.match(r"^(Options|Arguments):$", stripped):
                break
            match = re.match(r"^\s{2,}([A-Za-z0-9][A-Za-z0-9_-]*)\s{2,}(.*)$", line)
            if not match:
                continue
            commands[match.group(1)] = match.group(2).strip()
        return commands

    def _build_cli_help_catalog(self) -> dict[str, Any]:
        if self._cli_help_catalog_cache is not None:
            return self._cli_help_catalog_cache
        top_level_help = self.cli.help_text()
        top_level_commands = self._parse_cli_help_commands(top_level_help)
        subcommands: dict[str, dict[str, str]] = {}
        for command_name in top_level_commands:
            help_text = self.cli.help_text([command_name])
            parsed = self._parse_cli_help_commands(help_text)
            if parsed:
                subcommands[command_name] = parsed
        self._cli_help_catalog_cache = {
            "top_level": top_level_commands,
            "subcommands": subcommands,
        }
        return self._cli_help_catalog_cache

    def _write_cli_help_catalog(self, run_dir: Path) -> Path:
        path = run_dir / "cli-help-catalog.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            catalog = self._build_cli_help_catalog()
        except Exception:
            path.write_text("{}", encoding="utf-8")
            return path
        path.write_text(
            json.dumps(catalog, ensure_ascii=True, indent=2), encoding="utf-8"
        )
        return path

    def _format_cli_guard_error(
        self,
        *,
        invalid: str,
        message: str,
        valid_choices: list[str],
        suggested_help: list[str],
    ) -> str:
        details = [message]
        if valid_choices:
            details.append("Valid choices: " + ", ".join(valid_choices[:12]))
        if suggested_help:
            rendered_help = " ".join(suggested_help)
            details.append(f"Use `run_cli {rendered_help}` for help.")
        return " ".join(details)

    def _guard_cli_args(self, args: list[str]) -> str | None:
        if not args:
            return "No CLI command tokens were provided."
        first = str(args[0]).strip()
        if not first:
            return "No CLI command tokens were provided."
        instrument_index = 0
        while instrument_index < len(args):
            token = str(args[instrument_index]).strip()
            if token == "--instrument":
                value_index = instrument_index + 1
                if value_index < len(args):
                    raw_value = str(args[value_index]).strip()
                    if "," in raw_value:
                        rendered = " ".join(str(item) for item in args)
                        return (
                            "Invalid multi-instrument syntax. Do not comma-join symbols after "
                            "`--instrument`. Repeat the flag once per symbol, for example: "
                            "`--instrument EURUSD --instrument CADJPY --instrument AUDCHF`. "
                            f"Offending command: `{rendered}`"
                        )
                instrument_index += 2
                continue
            instrument_index += 1
        if first.startswith("-"):
            return None
        try:
            catalog = self._build_cli_help_catalog()
        except Exception:
            return None
        top_level = catalog.get("top_level", {}) if isinstance(catalog, dict) else {}
        subcommands = (
            catalog.get("subcommands", {}) if isinstance(catalog, dict) else {}
        )
        if first not in top_level:
            valid = sorted(str(item) for item in top_level.keys())
            closest = get_close_matches(first, valid, n=3, cutoff=0.45)
            choices = closest or valid
            return self._format_cli_guard_error(
                invalid=first,
                message=f"Invalid CLI command family `{first}`.",
                valid_choices=choices,
                suggested_help=["help"],
            )
        allowed_subcommands = subcommands.get(first, {})
        if not isinstance(allowed_subcommands, dict) or not allowed_subcommands:
            return None
        if len(args) == 1 or str(args[1]).startswith("-"):
            return self._format_cli_guard_error(
                invalid=first,
                message=f"CLI command family `{first}` requires a subcommand.",
                valid_choices=sorted(str(item) for item in allowed_subcommands.keys()),
                suggested_help=["help", first],
            )
        second = str(args[1]).strip()
        if second in {"help", "--help", "-h"}:
            return None
        if second not in allowed_subcommands:
            valid = sorted(str(item) for item in allowed_subcommands.keys())
            closest = get_close_matches(second, valid, n=4, cutoff=0.45)
            choices = closest or valid
            return self._format_cli_guard_error(
                invalid=second,
                message=f"Invalid subcommand `{first} {second}`.",
                valid_choices=choices,
                suggested_help=["help", first],
            )
        return None

    def _strip_cli_flag(self, args: list[str], flag: str) -> list[str]:
        stripped: list[str] = []
        index = 0
        while index < len(args):
            token = str(args[index])
            if token == flag:
                index += 2
                continue
            stripped.append(token)
            index += 1
        return stripped

    def _upsert_cli_flag(self, args: list[str], flag: str, value: str) -> list[str]:
        updated = list(args)
        if flag in updated:
            index = updated.index(flag) + 1
            if index < len(updated):
                updated[index] = value
                return updated
        updated.extend([flag, value])
        return updated

    def _configured_quality_score_preset(self) -> str:
        preset = str(self.config.research.quality_score_preset or "").strip()
        if preset in {"profile-drop", "profile_drop"}:
            return "profile-drop"
        return preset or "profile-drop"

    def _apply_horizon_policy_to_cli_args(
        self,
        args: list[str],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> list[str]:
        if not args:
            return args
        command_head = args[:2]
        horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
        lookback_months = str(horizon_policy["lookback_months"])
        quality_score_preset = self._configured_quality_score_preset()
        if args[0] in {"sensitivity", "sensitivity-basket"}:
            effective = self._strip_cli_flag(list(args), "--bar-limit")
            if "--timeframe" not in effective:
                inferred_timeframe = self._infer_timeframe_for_sensitivity_args(
                    effective
                )
                if inferred_timeframe:
                    effective.extend(["--timeframe", inferred_timeframe])
            if "--lookback-months" not in effective:
                effective.extend(["--lookback-months", lookback_months])
            effective = self._upsert_cli_flag(
                effective, "--quality-score-preset", quality_score_preset
            )
            return effective
        if command_head == ["deep-replay", "submit"]:
            effective = self._strip_cli_flag(list(args), "--bar-limit")
            return self._upsert_cli_flag(
                effective, "--quality-score-preset", quality_score_preset
            )
        if command_head == ["sweep", "scaffold"]:
            return self._upsert_cli_flag(
                list(args), "--quality-score-preset", quality_score_preset
            )
        if args[0] == "package":
            effective = self._strip_cli_flag(list(args), "--bar-limit")
            return self._upsert_cli_flag(
                effective, "--quality-score-preset", quality_score_preset
            )
        if command_head == ["deep-replay", "cell-detail"]:
            return self._strip_cli_flag(list(args), "--bar-limit")
        return args

    def _infer_timeframe_for_sensitivity_args(self, args: list[str]) -> str | None:
        profile_ref = None
        if "--profile-ref" in args:
            index = args.index("--profile-ref") + 1
            if index < len(args):
                profile_ref = str(args[index]).strip()
        if profile_ref:
            profile_path = self.profile_sources.get(profile_ref)
            inferred = self._infer_profile_timeframe_from_file(profile_path)
            if inferred:
                return inferred
        return "M5"

    def _infer_profile_timeframe_from_file(self, path: Path | None) -> str | None:
        if path is None or not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        profile = (
            payload.get("profile")
            if isinstance(payload.get("profile"), dict)
            else payload
        )
        indicators = profile.get("indicators") if isinstance(profile, dict) else None
        if not isinstance(indicators, list):
            return None
        timeframe_order = {
            "M1": 1,
            "M5": 5,
            "M15": 15,
            "M30": 30,
            "H1": 60,
            "H4": 240,
            "D1": 1440,
        }
        timeframes: list[str] = []
        for indicator in indicators:
            if not isinstance(indicator, dict):
                continue
            config = (
                indicator.get("config")
                if isinstance(indicator.get("config"), dict)
                else {}
            )
            if config.get("isActive") is False:
                continue
            timeframe = str(config.get("timeframe") or "").strip().upper()
            if timeframe in timeframe_order:
                timeframes.append(timeframe)
        if not timeframes:
            return None
        return min(timeframes, key=lambda item: timeframe_order.get(item, 999999))

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    def create_run_context(self) -> ToolContext:
        run_id = (
            f"{self._timestamp()}-{self.config.research.label_prefix}-{uuid4().hex[:6]}"
        )
        run_dir = self.config.runs_root / run_id
        attempts_path = attempts_path_for_run_dir(run_dir)
        run_metadata = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "explorer_profile": self.config.llm.explorer_profile,
            "explorer_provider_type": self.config.provider.provider_type,
            "explorer_model": self.config.provider.model,
            "quality_score_preset": self.config.research.quality_score_preset,
            "manager_enabled": self.config.manager.enabled,
            "manager_profiles": list(self.config.manager.profiles),
        }
        profiles_dir = run_dir / "profiles"
        evals_dir = run_dir / "evals"
        notes_dir = run_dir / "notes"
        progress_plot_path = run_dir / "progress.png"
        for path in [profiles_dir, evals_dir, notes_dir]:
            path.mkdir(parents=True, exist_ok=True)
        cli_help_catalog_path = self._write_cli_help_catalog(run_dir)
        run_metadata_path = write_run_metadata(run_dir, run_metadata)
        seed_prompt_path = run_dir / "seed-prompt.json"
        if self.config.research.auto_seed_prompt:
            self.cli.seed_prompt(seed_prompt_path)
        seed_indicator_ids = self._seed_indicator_ids(
            seed_prompt_path if seed_prompt_path.exists() else None
        )
        indicator_catalog_summary = self._indicator_catalog_summary(seed_indicator_ids)
        seed_indicator_parameter_hints = self._seed_indicator_parameter_hints(
            seed_indicator_ids
        )
        instrument_catalog_summary = self._instrument_catalog_summary()
        return ToolContext(
            run_id=run_id,
            run_dir=run_dir,
            attempts_path=attempts_path,
            run_metadata_path=run_metadata_path,
            profiles_dir=profiles_dir,
            evals_dir=evals_dir,
            notes_dir=notes_dir,
            progress_plot_path=progress_plot_path,
            cli_help_catalog_path=cli_help_catalog_path,
            seed_prompt_path=seed_prompt_path if seed_prompt_path.exists() else None,
            profile_template_path=self.profile_template_path,
            indicator_catalog_summary=indicator_catalog_summary,
            seed_indicator_parameter_hints=seed_indicator_parameter_hints,
            instrument_catalog_summary=instrument_catalog_summary,
        )

    def _program_text(self) -> str:
        return self.config.program_path.read_text(encoding="utf-8")

    def _portable_profile_template_note_text(self) -> str:
        return (
            "Portable profile template note:\n"
            "- The controller can scaffold from the portable template through prepare_profile.\n"
            "- Do not hand-author a full profile from this template unless typed tools are unavailable."
        )

    def _tool_reference_text(self) -> str:
        return (
            "Typed tool reference (default lane — prefer these over run_cli):\n"
            "- prepare_profile: mode scaffold_from_seed | clone_local | from_template; use candidate_name for run-owned drafts.\n"
            "- mutate_profile: candidate_name or profile_ref; mutations [{path, value}][]; optional destination_candidate_name.\n"
            "- validate_profile: candidate_name or profile_ref.\n"
            "- register_profile: candidate_name or profile_ref; operation create|update; profile_ref required for update.\n"
            "- evaluate_candidate: profile_ref first; candidate_name is acceptable for local unregistered drafts; include instruments[] plus optional timeframe_policy/timeframe/requested_horizon_months/evaluation_mode.\n"
            "- run_parameter_sweep: profile_ref; axes[] strings; optional instruments[], output_dir, candidate_name_prefix.\n"
            "- inspect_artifact: artifact_dir or attempt_id; view summary|files|curve_meta|request_meta.\n"
            "- compare_artifacts: attempt_ids[] or artifact_dirs[].\n"
            "Workflows:\n"
            "- New candidate: prepare_profile -> validate_profile -> register_profile -> evaluate_candidate.\n"
            "- Iterate: mutate_profile -> validate_profile -> evaluate_candidate.\n"
            "- Sweep: run_parameter_sweep -> inspect_artifact / compare_artifacts.\n"
            "- Read tool envelopes (ok, score, artifact_dir, auto_log, effective windows, warnings) before read_file/list_dir.\n"
            "Controller-managed (do not fight it in tool choice):\n"
            "- Injected lookback when omitted on evals; quality-score preset; bar-limit policy; phase horizon targets (see run packet).\n"
            "Catalog discipline:\n"
            "- Exact indicator meta ids; exact instrument symbols; instruments[] uses one array entry per symbol (no comma-joined tokens); never __BASKET__ as an instrument.\n"
            "Search discipline:\n"
            "- Explore multiple candidates; sweeps are normal (run_parameter_sweep); diversify early, prune weak basket members, do not finish early while budget remains.\n"
            "Artifacts:\n"
            "- sensitivity-response.json / deep-replay-job.json / best-cell-path-detail.json — prefer inspect_artifact and compare_artifacts over manual compare-sensitivity; no summary.json.\n"
            "run_cli fallback:\n"
            "- Help or recovery only, e.g. [\"help\"] or [\"help\",\"profiles\"]. Use typed tools for profiles, evals, and sweeps when available.\n"
            "Extra:\n"
            "- MA_CROSSOVER uses fastperiod, slowperiod, matype—not signalperiod.\n"
            "- If register_profile fails, fix the profile before evaluate_candidate in a later step."
        )

    def _durable_system_appendix_text(self) -> str:
        sections = [
            f"Program:\n{self._program_text()}",
            self._portable_profile_template_note_text(),
            self._artifact_layout_text(),
            f"Tool reference:\n{self._tool_reference_text()}",
        ]
        return "\n\n".join(section for section in sections if str(section).strip())

    def _short_family_id(self, family_id: str | None, *, limit: int = 28) -> str:
        text = str(family_id or "").strip()
        if not text:
            return "none"
        if len(text) <= limit:
            return text
        return text[:limit] + "..."

    def _seed_text(self, tool_context: ToolContext) -> str:
        if (
            not tool_context.seed_prompt_path
            or not tool_context.seed_prompt_path.exists()
        ):
            return "No seed prompt file exists for this run."
        return tool_context.seed_prompt_path.read_text(encoding="utf-8")

    def _recent_attempts_summary(self, tool_context: ToolContext) -> str:
        attempts = load_run_attempts(tool_context.run_dir)
        if not attempts:
            return "No attempts have been logged yet in this run."
        recent = attempts[-self.config.research.recent_attempts_window :]
        lines = []
        for attempt in recent:
            lines.append(
                f"{attempt['sequence']}: {attempt.get('candidate_name')} "
                f"score={attempt.get('composite_score')} basis={attempt.get('score_basis', 'n/a')} "
                f"artifact={attempt.get('artifact_dir')}"
            )
        return "\n".join(lines)

    def _run_attempts(self, run_id: str) -> list[dict[str, Any]]:
        return load_run_attempts(self.config.runs_root / run_id)

    def _render_run_progress(self, tool_context: ToolContext) -> None:
        run_attempts = load_run_attempts(tool_context.run_dir)
        render_progress_artifacts(
            run_attempts,
            tool_context.progress_plot_path,
            run_metadata_path=tool_context.run_metadata_path,
            lower_is_better=self.config.research.plot_lower_is_better,
        )

    def _scored_attempts(self, attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            attempt
            for attempt in attempts
            if attempt.get("composite_score") is not None
        ]

    def _score_better(self, left: float, right: float) -> bool:
        if self.config.research.plot_lower_is_better:
            return left < right
        return left > right

    def _best_attempt(self, attempts: list[dict[str, Any]]) -> dict[str, Any] | None:
        scored = self._scored_attempts(attempts)
        if not scored:
            return None
        return (
            min(
                scored,
                key=lambda attempt: float(attempt.get("composite_score")),
            )
            if self.config.research.plot_lower_is_better
            else max(
                scored,
                key=lambda attempt: float(attempt.get("composite_score")),
            )
        )

    def _best_attempt_for_family(
        self, attempts: list[dict[str, Any]], family_id: str
    ) -> dict[str, Any] | None:
        family_attempts = [
            attempt
            for attempt in self._scored_attempts(attempts)
            if self._family_id_for_profile_ref(
                str(attempt.get("profile_ref") or "").strip() or None
            )
            == family_id
        ]
        if not family_attempts:
            return None
        return (
            min(
                family_attempts,
                key=lambda attempt: float(attempt.get("composite_score")),
            )
            if self.config.research.plot_lower_is_better
            else max(
                family_attempts,
                key=lambda attempt: float(attempt.get("composite_score")),
            )
        )

    def _attempt_effective_window_months(
        self, attempt: dict[str, Any]
    ) -> float | None:
        best_summary = attempt.get("best_summary")
        if isinstance(best_summary, dict):
            market_window = best_summary.get("market_data_window")
            if isinstance(market_window, dict):
                value = self._coerce_float_months(
                    market_window.get("effective_window_months")
                )
                if value is not None and value >= 0:
                    return value
            quality_score_payload = best_summary.get("quality_score_payload")
            if isinstance(quality_score_payload, dict):
                inputs = quality_score_payload.get("inputs")
                if isinstance(inputs, dict):
                    value = self._coerce_float_months(
                        inputs.get("effective_window_months")
                    )
                    if value is not None and value >= 0:
                        return value
        value = self._coerce_float_months(attempt.get("effective_window_months"))
        if value is not None and value >= 0:
            return value
        return None

    def _attempt_has_timeframe_mismatch(self, attempt: dict[str, Any]) -> bool:
        req = str(attempt.get("requested_timeframe") or "").strip()
        eff = str(attempt.get("effective_timeframe") or "").strip()
        if req and eff and req != eff:
            return True
        return bool(attempt.get("timeframe_mismatch"))

    def _attempt_admissibility_snapshot(
        self, attempt: dict[str, Any]
    ) -> dict[str, Any] | None:
        raw_score = attempt.get("composite_score")
        if raw_score is None:
            return None
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            return None
        effective_window_months = self._attempt_effective_window_months(attempt)
        trades_per_month = self._attempt_trades_per_month(attempt)
        support_quality = self._derive_support_quality(attempt)
        validation_outcome = str(attempt.get("validation_outcome") or "").strip() or None
        mismatch = self._attempt_has_timeframe_mismatch(attempt)
        profile_ref = str(attempt.get("profile_ref") or "").strip() or None
        family_id = self._family_id_for_profile_ref(profile_ref)
        branch = self._family_branches.get(family_id) if family_id else None
        promotability_status = (
            branch.promotability_status
            if branch and isinstance(branch.promotability_status, str)
            else None
        )

        validated_min = max(
            1.0, float(self.config.research.validated_leader_min_horizon_months)
        )
        if effective_window_months is None:
            window_factor = 0.55
        else:
            ratio = effective_window_months / validated_min
            if ratio < 0.25:
                window_factor = 0.30
            elif ratio < 0.50:
                window_factor = 0.45
            elif ratio < 0.75:
                window_factor = 0.65
            elif ratio < 1.00:
                window_factor = 0.82
            elif effective_window_months >= float(
                self.config.research.horizon_wrap_up_months
            ):
                window_factor = 1.12
            elif effective_window_months >= float(
                self.config.research.horizon_late_months
            ):
                window_factor = 1.08
            else:
                window_factor = 1.0

        if trades_per_month is None:
            cadence_factor = 0.85
        elif trades_per_month < 1.5:
            cadence_factor = 0.55
        elif trades_per_month < 3.0:
            cadence_factor = 0.75
        elif trades_per_month < 6.0:
            cadence_factor = 0.90
        else:
            cadence_factor = 1.0

        if mismatch:
            validation_factor = 0.55
        elif validation_outcome == vo.VALIDATION_FAILED:
            validation_factor = 0.72
        elif validation_outcome == vo.VALIDATION_UNRESOLVED:
            validation_factor = 0.86
        elif validation_outcome == vo.VALIDATION_PASSED:
            validation_factor = 1.05
        else:
            validation_factor = 1.0

        if promotability_status == vo.PROMOTABILITY_VALIDATED_READY:
            promotability_factor = 1.08
        elif promotability_status == vo.PROMOTABILITY_BLOCKED:
            promotability_factor = 0.72
        elif promotability_status == vo.PROMOTABILITY_RETRY_RECOMMENDED:
            promotability_factor = 0.92
        else:
            promotability_factor = 1.0

        multiplier = (
            window_factor
            * cadence_factor
            * validation_factor
            * promotability_factor
        )
        steering_score = (
            score / max(multiplier, 1e-6)
            if self.config.research.plot_lower_is_better
            else score * multiplier
        )
        penalties: list[str] = []
        if effective_window_months is not None and effective_window_months < validated_min:
            penalties.append(
                f"short_window={self._format_score(effective_window_months)}m"
            )
        if trades_per_month is not None and trades_per_month < 3.0:
            penalties.append(f"cadence={self._format_score(trades_per_month)}/mo")
        if mismatch:
            penalties.append("timeframe_mismatch")
        if validation_outcome == vo.VALIDATION_UNRESOLVED:
            penalties.append("validation_unresolved")
        elif validation_outcome == vo.VALIDATION_FAILED:
            penalties.append("validation_failed")
        if promotability_status == vo.PROMOTABILITY_BLOCKED:
            penalties.append("blocked")
        return {
            "attempt": attempt,
            "family_id": family_id,
            "profile_ref": profile_ref,
            "candidate_name": str(attempt.get("candidate_name") or "").strip() or None,
            "raw_score": score,
            "steering_score": steering_score,
            "admissibility_multiplier": multiplier,
            "effective_window_months": effective_window_months,
            "trades_per_month": trades_per_month,
            "support_quality": support_quality,
            "validation_outcome": validation_outcome,
            "timeframe_mismatch": mismatch,
            "promotability_status": promotability_status,
            "penalties": penalties,
        }

    def _admissible_frontier_snapshot(
        self, attempts: list[dict[str, Any]]
    ) -> dict[str, Any]:
        ranked = [
            snap
            for snap in (
                self._attempt_admissibility_snapshot(attempt)
                for attempt in self._scored_attempts(attempts)
            )
            if isinstance(snap, dict)
        ]
        if not ranked:
            return {
                "best": None,
                "summary": "No admissible frontier points exist yet.",
            }
        best = (
            min(ranked, key=lambda snap: float(snap["steering_score"]))
            if self.config.research.plot_lower_is_better
            else max(ranked, key=lambda snap: float(snap["steering_score"]))
        )
        penalty_text = ", ".join(best["penalties"]) if best["penalties"] else "clean"
        summary = (
            "Best admissible steering frontier: "
            f"candidate={best.get('candidate_name') or 'n/a'} "
            f"family={self._short_family_id(best.get('family_id'))} "
            f"raw_score={self._format_score(best.get('raw_score'))} "
            f"steering_score={self._format_score(best.get('steering_score'))} "
            f"window={self._format_score(best.get('effective_window_months'))}m "
            f"cadence={self._format_score(best.get('trades_per_month'))}/mo "
            f"status={best.get('validation_outcome') or 'n/a'} "
            f"notes={penalty_text}"
        )
        return {
            "best": best,
            "summary": summary,
            "ranked_count": len(ranked),
        }

    def _current_wrap_up_focus_state(
        self, attempts: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        overlay = self._branch_overlay
        focus_family = overlay.validated_leader_family_id
        if not focus_family:
            provisional = overlay.provisional_leader_family_id
            provisional_branch = (
                self._family_branches.get(provisional) if provisional else None
            )
            if (
                provisional
                and provisional_branch
                and not provisional_branch.exploit_dead
                and provisional_branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED
            ):
                focus_family = provisional
        ranked: list[dict[str, Any]] = []
        for snap in (
            self._attempt_admissibility_snapshot(attempt)
            for attempt in self._scored_attempts(attempts)
        ):
            if not isinstance(snap, dict):
                continue
            family_id = str(snap.get("family_id") or "").strip()
            if not family_id:
                continue
            branch = self._family_branches.get(family_id)
            if not branch or branch.exploit_dead or branch.lifecycle_state == bl.LIFECYCLE_COLLAPSED:
                continue
            snap["branch"] = branch
            ranked.append(snap)
        if not ranked:
            return None
        if not focus_family:
            promotability_rank = {
                vo.PROMOTABILITY_VALIDATED_READY: 4,
                vo.PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE: 3,
                vo.PROMOTABILITY_RETRY_RECOMMENDED: 2,
                "unknown": 1,
                vo.PROMOTABILITY_BLOCKED: 0,
            }
            retention_rank = {
                bl.RETENTION_PASSED: 3,
                bl.RETENTION_PENDING: 2,
                bl.RETENTION_UNTESTED: 1,
                bl.RETENTION_FAILED: 0,
            }
            ranked.sort(
                key=lambda snap: (
                    promotability_rank.get(
                        str(snap["branch"].promotability_status or "unknown"), 1
                    ),
                    retention_rank.get(str(snap["branch"].retention_status or ""), 0),
                    1
                    if snap["branch"].lifecycle_state == bl.LIFECYCLE_PROVISIONAL_LEADER
                    else 0,
                    float(snap["steering_score"]),
                ),
                reverse=not self.config.research.plot_lower_is_better,
            )
            if self.config.research.plot_lower_is_better:
                ranked.sort(
                    key=lambda snap: (
                        -promotability_rank.get(
                            str(snap["branch"].promotability_status or "unknown"), 1
                        ),
                        -retention_rank.get(str(snap["branch"].retention_status or ""), 0),
                        -(
                            1
                            if snap["branch"].lifecycle_state
                            == bl.LIFECYCLE_PROVISIONAL_LEADER
                            else 0
                        ),
                        float(snap["steering_score"]),
                    )
                )
            focus_family = str(ranked[0].get("family_id") or "").strip() or None
        if not focus_family:
            return None
        branch = self._family_branches.get(focus_family)
        if not branch:
            return None
        family_attempts = [
            snap for snap in ranked if str(snap.get("family_id") or "").strip() == focus_family
        ]
        if not family_attempts:
            return None
        highest_horizon = max(
            (
                int(snap.get("attempt", {}).get("requested_horizon_months") or 0)
                for snap in family_attempts
            ),
            default=0,
        )
        if highest_horizon > 0:
            family_attempts = [
                snap
                for snap in family_attempts
                if int(snap.get("attempt", {}).get("requested_horizon_months") or 0)
                == highest_horizon
            ] or family_attempts
        best_snap = self._select_best_attempt_snapshot(family_attempts)
        if not isinstance(best_snap, dict):
            return None
        return {
            "family_id": focus_family,
            "candidate_name": best_snap.get("candidate_name"),
            "profile_ref": best_snap.get("profile_ref") or branch.last_profile_ref,
            "selected_attempt_id": (
                str(best_snap.get("attempt", {}).get("attempt_id") or "").strip()
                or branch.best_attempt_id
                or branch.latest_attempt_id
            ),
            "latest_attempt_id": branch.latest_attempt_id,
            "lifecycle_state": branch.lifecycle_state,
            "promotability_status": branch.promotability_status,
            "retention_status": branch.retention_status,
            "raw_score": best_snap.get("raw_score"),
            "steering_score": best_snap.get("steering_score"),
            "effective_window_months": best_snap.get("effective_window_months"),
            "trades_per_month": best_snap.get("trades_per_month"),
            "requested_horizon_months": best_snap.get("attempt", {}).get(
                "requested_horizon_months"
            ),
            "requested_timeframe": best_snap.get("attempt", {}).get(
                "requested_timeframe"
            ),
            "effective_timeframe": best_snap.get("attempt", {}).get(
                "effective_timeframe"
            ),
            "reason": (
                "validated_leader"
                if overlay.validated_leader_family_id == focus_family
                else "provisional_leader"
                if overlay.provisional_leader_family_id == focus_family
                else "best_retryable_provisional"
            ),
        }

    def _select_best_attempt_snapshot(
        self,
        snapshots: list[dict[str, Any]],
        *,
        prefer_highest_horizon: bool = False,
    ) -> dict[str, Any] | None:
        ranked = [snap for snap in snapshots if isinstance(snap, dict)]
        if not ranked:
            return None
        if prefer_highest_horizon:
            highest_horizon = max(
                (
                    int(snap.get("attempt", {}).get("requested_horizon_months") or 0)
                    for snap in ranked
                ),
                default=0,
            )
            if highest_horizon > 0:
                ranked = [
                    snap
                    for snap in ranked
                    if int(snap.get("attempt", {}).get("requested_horizon_months") or 0)
                    == highest_horizon
                ] or ranked
        return (
            min(
                ranked,
                key=lambda snap: (
                    float(snap.get("steering_score") or 0.0),
                    float(snap.get("raw_score") or 0.0),
                ),
            )
            if self.config.research.plot_lower_is_better
            else max(
                ranked,
                key=lambda snap: (
                    float(snap.get("steering_score") or 0.0),
                    float(snap.get("raw_score") or 0.0),
                ),
            )
        )

    def _outcome_entry_for_snapshot(
        self,
        snap: dict[str, Any] | None,
        *,
        branch: bl.FamilyBranchState | None = None,
        reason: str,
    ) -> dict[str, Any] | None:
        if not isinstance(snap, dict):
            return None
        branch = branch or snap.get("branch")
        attempt = snap.get("attempt") if isinstance(snap.get("attempt"), dict) else {}
        family_id = str(snap.get("family_id") or "").strip() or None
        return {
            "family_id": family_id,
            "candidate_name": snap.get("candidate_name"),
            "profile_ref": snap.get("profile_ref"),
            "attempt_id": str(attempt.get("attempt_id") or "").strip() or None,
            "raw_score": snap.get("raw_score"),
            "steering_score": snap.get("steering_score"),
            "effective_window_months": snap.get("effective_window_months"),
            "trades_per_month": snap.get("trades_per_month"),
            "requested_horizon_months": attempt.get("requested_horizon_months"),
            "requested_timeframe": attempt.get("requested_timeframe"),
            "effective_timeframe": attempt.get("effective_timeframe"),
            "validation_outcome": snap.get("validation_outcome"),
            "promotability_status": (
                branch.promotability_status if branch else snap.get("promotability_status")
            ),
            "retention_status": branch.retention_status if branch else None,
            "lifecycle_state": branch.lifecycle_state if branch else None,
            "promotion_level": branch.promotion_level if branch else None,
            "currently_live": bool(
                branch
                and not branch.exploit_dead
                and branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED
            ),
            "currently_collapsed": bool(
                branch and branch.lifecycle_state == bl.LIFECYCLE_COLLAPSED
            ),
            "reason": reason,
        }

    def _run_outcome_snapshot(
        self, attempts: list[dict[str, Any]]
    ) -> dict[str, Any]:
        overlay = self._branch_overlay
        ranked: list[dict[str, Any]] = []
        for snap in (
            self._attempt_admissibility_snapshot(attempt)
            for attempt in self._scored_attempts(attempts)
        ):
            if not isinstance(snap, dict):
                continue
            family_id = str(snap.get("family_id") or "").strip()
            if not family_id:
                continue
            branch = self._family_branches.get(family_id)
            if branch:
                snap["branch"] = branch
            ranked.append(snap)

        wrap_up_focus = self._current_wrap_up_focus_state(attempts)
        selected_focus_attempt_id = (
            str(wrap_up_focus.get("selected_attempt_id") or "").strip()
            if isinstance(wrap_up_focus, dict)
            else ""
        )
        best_live_focus: dict[str, Any] | None = None
        if selected_focus_attempt_id:
            focus_snap = next(
                (
                    snap
                    for snap in ranked
                    if str(snap.get("attempt", {}).get("attempt_id") or "").strip()
                    == selected_focus_attempt_id
                ),
                None,
            )
            best_live_focus = self._outcome_entry_for_snapshot(
                focus_snap,
                reason="best_live_focus",
            )

        validated_snaps = [
            snap
            for snap in ranked
            if isinstance(snap.get("branch"), bl.FamilyBranchState)
            and snap["branch"].promotion_level == bl.PROMOTION_VALIDATED
        ]
        best_historical_validated = self._outcome_entry_for_snapshot(
            self._select_best_attempt_snapshot(
                validated_snaps,
                prefer_highest_horizon=True,
            ),
            reason="best_historical_validated",
        )

        retryable_snaps = [
            snap
            for snap in ranked
            if isinstance(snap.get("branch"), bl.FamilyBranchState)
            and not snap["branch"].exploit_dead
            and snap["branch"].lifecycle_state != bl.LIFECYCLE_COLLAPSED
            and str(snap["branch"].promotability_status or "")
            in {
                vo.PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE,
                vo.PROMOTABILITY_RETRY_RECOMMENDED,
            }
        ]
        best_historical_retryable = self._outcome_entry_for_snapshot(
            self._select_best_attempt_snapshot(
                retryable_snaps,
                prefer_highest_horizon=True,
            ),
            reason="best_historical_retryable",
        )

        official_winner: dict[str, Any] | None = None
        official_winner_type = "none"
        rationale: str
        if overlay.validated_leader_family_id and best_live_focus:
            official_winner = dict(best_live_focus)
            official_winner["reason"] = "official_validated_winner"
            official_winner_type = "validated_leader"
            rationale = (
                "A validated leader remains authoritative at the end of the run, so it is "
                "the official winner."
            )
        elif best_historical_validated:
            rationale = (
                "No official winner: the run previously produced validated evidence, but the "
                "validated family did not remain authoritative through the end state."
            )
        elif best_live_focus:
            rationale = (
                "No official winner: the run ended with only a provisional live focus and no "
                "validated leader."
            )
        else:
            rationale = (
                "No official winner: the run ended without a validated leader or a live "
                "focus candidate."
            )

        return {
            "official_winner_type": official_winner_type,
            "official_winner": official_winner,
            "best_live_focus": best_live_focus,
            "best_historical_validated": best_historical_validated,
            "best_historical_retryable": best_historical_retryable,
            "rationale": rationale,
        }

    def _current_gut_check_state(
        self,
        attempts: list[dict[str, Any]],
        *,
        phase_name: str,
    ) -> dict[str, Any] | None:
        overlay = self._branch_overlay
        admissible = self._admissible_frontier_snapshot(attempts).get("best")
        focus_family = (
            overlay.validated_leader_family_id
            or overlay.provisional_leader_family_id
            or (admissible.get("family_id") if isinstance(admissible, dict) else None)
        )
        if not focus_family:
            return None
        focus_attempt = self._best_attempt_for_family(attempts, focus_family)
        if not isinstance(focus_attempt, dict):
            return None
        try:
            raw_score = float(focus_attempt.get("composite_score"))
        except (TypeError, ValueError):
            return None
        strong_floor = max(
            float(self.config.research.retention_strong_candidate_threshold),
            float(self.config.research.validated_leader_min_score),
        )
        if self.config.research.plot_lower_is_better:
            is_strong = raw_score <= strong_floor
        else:
            is_strong = raw_score >= strong_floor
        if not is_strong:
            return None
        clean_horizons: list[int] = []
        for attempt in attempts:
            if self._family_id_for_profile_ref(
                str(attempt.get("profile_ref") or "").strip() or None
            ) != focus_family:
                continue
            if self._attempt_has_timeframe_mismatch(attempt):
                continue
            try:
                horizon = int(attempt.get("requested_horizon_months"))
            except (TypeError, ValueError):
                continue
            if horizon > 0:
                clean_horizons.append(horizon)
        max_clean_horizon = max(clean_horizons) if clean_horizons else 0
        ladder: list[int] = []
        for horizon in (
            int(self.config.research.validated_leader_min_horizon_months),
            int(self.config.research.horizon_late_months),
            int(self.config.research.horizon_wrap_up_months),
        ):
            if horizon not in ladder:
                ladder.append(horizon)
        target_horizon: int | None = None
        for horizon in ladder:
            if max_clean_horizon < horizon:
                if horizon == int(self.config.research.validated_leader_min_horizon_months):
                    target_horizon = horizon
                    break
                if horizon == int(self.config.research.horizon_late_months) and phase_name in {
                    "mid",
                    "late",
                    "wrap_up",
                }:
                    target_horizon = horizon
                    break
                if horizon == int(self.config.research.horizon_wrap_up_months) and phase_name == "wrap_up":
                    target_horizon = horizon
                    break
        if target_horizon is None:
            return None
        return {
            "family_id": focus_family,
            "candidate_name": str(focus_attempt.get("candidate_name") or "").strip()
            or None,
            "profile_ref": str(focus_attempt.get("profile_ref") or "").strip() or None,
            "raw_score": raw_score,
            "effective_window_months": self._attempt_effective_window_months(
                focus_attempt
            ),
            "trades_per_month": self._attempt_trades_per_month(focus_attempt),
            "target_horizon_months": target_horizon,
            "max_clean_horizon_months": max_clean_horizon,
            "phase_name": phase_name,
        }

    def _format_score(self, value: Any) -> str:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return "n/a"
        text = f"{number:.3f}"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    def _run_phase_info(
        self, step: int, step_limit: int, policy: RunPolicy
    ) -> dict[str, Any]:
        wrap_up_steps = max(1, min(step_limit, self.config.research.run_wrap_up_steps))
        wrap_up_start = max(1, step_limit - wrap_up_steps + 1)
        if step >= wrap_up_start:
            return {
                "name": "wrap_up",
                "wrap_up_start": wrap_up_start,
                "finish_enabled": policy.allow_finish,
                "summary": (
                    f"Wrap-up phase: use the remaining {step_limit - step + 1} step(s) to validate the likely winner "
                    f"over the longest horizon and close obvious evidence gaps."
                ),
            }
        exploration_steps = max(1, wrap_up_start - 1)
        if exploration_steps <= 1:
            phase_name = "mid"
        else:
            progress = (step - 1) / max(1, exploration_steps - 1)
            early_cutoff = min(max(self.config.research.phase_early_ratio, 0.05), 0.9)
            late_cutoff = min(
                max(self.config.research.phase_late_ratio, early_cutoff + 0.05), 0.98
            )
            if progress < early_cutoff:
                phase_name = "early"
            elif progress < late_cutoff:
                phase_name = "mid"
            else:
                phase_name = "late"
        summaries = {
            "early": (
                f"Early phase: branch broadly, reject weak ideas cheaply, and prioritize fresh contrasts until step {wrap_up_start}. "
                "Use permissive screening first, include at least one bounded sweep around a promising family before locking into manual tweaks only, and test multiple distinct instruments or small instrument groups before narrowing hard."
            ),
            "mid": (
                f"Mid phase: narrow onto the strongest families, deepen evidence, and prefer systematic follow-up over random wandering before wrap-up at step {wrap_up_start}. "
                "Targeted sweeps should be a normal part of refinement in this phase."
            ),
            "late": (
                f"Late phase: stop spraying branches, focus on one or two survivors, and pressure-test them before wrap-up begins at step {wrap_up_start}. "
                "Use surgical sweeps around the surviving profile when manual patching alone is no longer yielding much."
            ),
        }
        return {
            "name": phase_name,
            "wrap_up_start": wrap_up_start,
            "finish_enabled": False,
            "summary": summaries[phase_name],
        }

    def _horizon_policy_snapshot(
        self,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        phase_info = self._run_phase_info(step, step_limit, policy)
        phase_name = str(phase_info.get("name") or "mid")
        phase_months = {
            "early": self.config.research.horizon_early_months,
            "mid": self.config.research.horizon_mid_months,
            "late": self.config.research.horizon_late_months,
            "wrap_up": self.config.research.horizon_wrap_up_months,
            "managed": self.config.research.horizon_mid_months,
        }
        months = int(
            phase_months.get(phase_name, self.config.research.horizon_mid_months)
        )
        if phase_name == "early":
            rationale = "cheap early screening: test broad branches over a shorter horizon before spending more compute"
            guidance = f"Target about {months} months of evidence. Favor cheap branch-heavy screening and reject weak ideas quickly."
        elif phase_name == "mid":
            rationale = (
                "deepen evidence on the strongest branches before full pressure testing"
            )
            guidance = f"Target about {months} months of evidence. Narrow onto top branches and start validating that the edge persists."
        elif phase_name == "late":
            rationale = (
                "pressure-test one or two survivors over longer history before wrap-up"
            )
            guidance = f"Target about {months} months of evidence. Prefer robustness, portability, and structured follow-up over novelty."
        else:
            rationale = "final validation should use the longest horizon in the session"
            guidance = f"Target about {months} months of evidence. Use the last steps to validate the likely winner over the longest believable horizon."
        return {
            "phase": phase_name,
            "lookback_months": months,
            "summary": (
                f"Controller horizon target: use about {months} months of history in this phase. "
                "Think in weeks/months/years, not bars."
            ),
            "guidance": guidance,
            "rationale": rationale,
        }

    def _score_target_snapshot(self, tool_context: ToolContext) -> dict[str, Any]:
        attempts = self._run_attempts(tool_context.run_id)
        run_best = self._best_attempt(attempts)
        admissible = self._admissible_frontier_snapshot(attempts)
        steering_best = admissible.get("best") if isinstance(admissible, dict) else None

        current_score = (
            float(steering_best.get("steering_score"))
            if isinstance(steering_best, dict)
            and steering_best.get("steering_score") is not None
            else None
        )
        raw_best_score = (
            float(run_best.get("composite_score"))
            if isinstance(run_best, dict)
            and run_best.get("composite_score") is not None
            else None
        )

        target_score: float | None = None
        rationale: str
        if current_score is not None:
            delta = max(3.0, abs(current_score) * 0.05)
            target_score = (
                current_score - delta
                if self.config.research.plot_lower_is_better
                else current_score + delta
            )
            rationale = (
                "push past the current admissible leader with one believable improvement"
            )
        else:
            rationale = (
                "log the first credible scored candidate before chasing higher targets"
            )

        if target_score is None:
            summary = (
                "Next target: log the first credible scored candidate for this run."
            )
        elif self.config.research.plot_lower_is_better:
            summary = (
                f"Next target: get admissible quality_score <= {self._format_score(target_score)}. "
                f"Current admissible best={self._format_score(current_score)}."
            )
        else:
            summary = (
                f"Next target: get admissible quality_score >= {self._format_score(target_score)}. "
                f"Current admissible best={self._format_score(current_score)}."
            )
        if current_score is not None and raw_best_score is not None:
            summary += (
                f" Raw run best={self._format_score(raw_best_score)} remains informational only."
            )

        return {
            "target_score": target_score,
            "current_run_best_score": current_score,
            "current_run_best_candidate": steering_best.get("candidate_name")
            if isinstance(steering_best, dict)
            else None,
            "raw_run_best_score": raw_best_score,
            "raw_run_best_candidate": run_best.get("candidate_name")
            if isinstance(run_best, dict)
            else None,
            "admissible_frontier": steering_best,
            "global_best_score": None,
            "global_best_candidate": None,
            "summary": summary,
            "rationale": rationale,
        }

    def _frontier_snapshot_text(self, tool_context: ToolContext) -> str:
        attempts = load_run_attempts(tool_context.run_dir)
        valid = [
            attempt
            for attempt in attempts
            if attempt.get("composite_score") is not None
        ]
        if not valid:
            return "No scored frontier points exist yet in this run."

        frontier, _ = compute_frontier(
            valid,
            lower_is_better=self.config.research.plot_lower_is_better,
        )
        if not frontier:
            return "No scored frontier points exist yet in this run."

        lines: list[str] = []
        current_best = frontier[-1]
        best_summary = (
            current_best.get("best_summary")
            if isinstance(current_best.get("best_summary"), dict)
            else {}
        )
        current_metrics = (
            current_best.get("metrics")
            if isinstance(current_best.get("metrics"), dict)
            else {}
        )
        best_cell = (
            best_summary.get("best_cell")
            if isinstance(best_summary.get("best_cell"), dict)
            else {}
        )
        effective_window_months = self._attempt_effective_window_months(current_best)
        trades_per_month = self._attempt_trades_per_month(current_best)
        positive_ratio = None
        matrix_summary = (
            best_summary.get("matrix_summary")
            if isinstance(best_summary.get("matrix_summary"), dict)
            else {}
        )
        if matrix_summary:
            positive_ratio = matrix_summary.get("positive_cell_ratio")

        lines.append("Current best run-local frontier point:")
        lines.append(
            f"- seq={current_best.get('sequence')} score={current_best.get('composite_score')} "
            f"candidate={current_best.get('candidate_name')} profile_ref={current_best.get('profile_ref') or 'n/a'} "
            f"basis={current_best.get('score_basis', 'n/a')} dsr={current_metrics.get('dsr', 'n/a')} "
            f"psr={current_metrics.get('psr', 'n/a')} resolved_trades={best_cell.get('resolved_trades', 'n/a')} "
            f"effective_window_months={self._format_score(effective_window_months)} "
            f"trades_per_month={self._format_score(trades_per_month)} "
            f"positive_cell_ratio={positive_ratio if positive_ratio is not None else 'n/a'}"
        )

        lines.append("Recent frontier points:")
        for attempt in frontier[-10:]:
            summary = (
                attempt.get("best_summary")
                if isinstance(attempt.get("best_summary"), dict)
                else {}
            )
            metrics = (
                attempt.get("metrics")
                if isinstance(attempt.get("metrics"), dict)
                else {}
            )
            cell = (
                summary.get("best_cell")
                if isinstance(summary.get("best_cell"), dict)
                else {}
            )
            eff_months = self._attempt_effective_window_months(attempt)
            tpm = self._attempt_trades_per_month(attempt)
            lines.append(
                f"- seq={attempt.get('sequence')} score={attempt.get('composite_score')} "
                f"basis={attempt.get('score_basis', 'n/a')} dsr={metrics.get('dsr', 'n/a')} "
                f"psr={metrics.get('psr', 'n/a')} candidate={attempt.get('candidate_name')} "
                f"trades={cell.get('resolved_trades', 'n/a')} "
                f"effective_window_months={self._format_score(eff_months)} "
                f"trades_per_month={self._format_score(tpm)} "
                f"artifact={attempt.get('artifact_dir')}"
            )

        if len(frontier) < 5:
            scored = sorted(
                valid,
                key=lambda attempt: float(
                    attempt.get("composite_score", float("-inf"))
                ),
                reverse=not self.config.research.plot_lower_is_better,
            )
            lines.append("Top scored attempts fallback:")
            for attempt in scored[:5]:
                metrics = (
                    attempt.get("metrics")
                    if isinstance(attempt.get("metrics"), dict)
                    else {}
                )
                lines.append(
                    f"- seq={attempt.get('sequence')} score={attempt.get('composite_score')} "
                    f"basis={attempt.get('score_basis', 'n/a')} dsr={metrics.get('dsr', 'n/a')} "
                    f"psr={metrics.get('psr', 'n/a')} candidate={attempt.get('candidate_name')} "
                    f"artifact={attempt.get('artifact_dir')}"
                )

        return "\n".join(lines)

    def _seed_indicator_ids(self, seed_prompt_path: Path | None) -> list[str]:
        if not seed_prompt_path or not seed_prompt_path.exists():
            return []
        try:
            payload = json.loads(seed_prompt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        indicators = payload.get("indicators")
        if not isinstance(indicators, list):
            return []
        result: list[str] = []
        for item in indicators:
            if isinstance(item, str) and item.strip():
                result.append(item.strip())
        return result

    def _indicator_catalog_summary(self, seed_indicator_ids: list[str]) -> str:
        result = self.cli.run(["indicators", "--mode", "index"], check=False)
        if result.returncode != 0 or not isinstance(result.parsed_json, dict):
            return "Indicator catalog snapshot unavailable."
        data = result.parsed_json.get("data")
        if not isinstance(data, dict):
            return "Indicator catalog snapshot unavailable."
        timeframes = (
            data.get("timeframes") if isinstance(data.get("timeframes"), list) else []
        )
        tf_values = [
            str(item.get("value"))
            for item in timeframes
            if isinstance(item, dict) and item.get("value")
        ]
        timeframe_preview = ", ".join(tf_values) if tf_values else "unavailable"
        seed_preview = ", ".join(seed_indicator_ids) if seed_indicator_ids else "none"
        return (
            f"Supported timeframes: {timeframe_preview}\n"
            "Only use exact ids from the current seed hand in indicator.meta.id. Do not invent ids from seed wording.\n"
            f"Seeded indicator ids for this run: {seed_preview}"
        )

    def _seed_indicator_parameter_hints(self, seed_indicator_ids: list[str]) -> str:
        if not seed_indicator_ids:
            return "No seeded indicator ids were found for this run."
        args = ["indicators", "--mode", "detail"]
        for indicator_id in seed_indicator_ids:
            args.extend(["--id", indicator_id])
        result = self.cli.run(args, check=False)
        if result.returncode != 0 or not isinstance(result.parsed_json, dict):
            return "Seeded indicator parameter hints unavailable."
        data = result.parsed_json.get("data")
        if not isinstance(data, dict):
            return "Seeded indicator parameter hints unavailable."
        indicators = data.get("indicators")
        if not isinstance(indicators, list) or not indicators:
            return "Seeded indicator parameter hints unavailable."
        lines: list[str] = []
        for item in indicators:
            if not isinstance(item, dict):
                continue
            indicator_id = str(
                item.get("id") or item.get("meta", {}).get("id") or ""
            ).strip()
            if not indicator_id:
                continue
            meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
            defaults = (
                item.get("configDefaults")
                if isinstance(item.get("configDefaults"), dict)
                else {}
            )
            talib_meta = (
                meta.get("talibMeta") if isinstance(meta.get("talibMeta"), list) else []
            )
            talib_parts: list[str] = []
            for param in talib_meta[:8]:
                if not isinstance(param, dict):
                    continue
                name = str(param.get("name", "")).strip()
                if not name:
                    continue
                default = param.get("default")
                if default is None:
                    talib_parts.append(name)
                else:
                    talib_parts.append(f"{name}={default}")
            ranges = (
                defaults.get("ranges")
                if isinstance(defaults.get("ranges"), dict)
                else {}
            )
            buy_range = ranges.get("buy")
            sell_range = ranges.get("sell")
            range_text = ""
            if isinstance(buy_range, list) and isinstance(sell_range, list):
                range_text = f" | default ranges buy={buy_range} sell={sell_range}"
            timeframe = defaults.get("timeframe")
            description = str(meta.get("description", "")).strip()
            if len(description) > 140:
                description = description[:137] + "..."
            lines.append(
                f"- {indicator_id}: tf_default={timeframe or 'n/a'}"
                f" | talib={', '.join(talib_parts) if talib_parts else 'none'}"
                f"{range_text}"
                f" | note={description or 'n/a'}"
            )
        if not lines:
            return "Seeded indicator parameter hints unavailable."
        return "\n".join(lines)

    def _instrument_catalog_summary(self) -> str:
        result = self.cli.run(["instruments", "--mode", "index"], check=False)
        if result.returncode != 0 or not isinstance(result.parsed_json, dict):
            return "Instrument catalog snapshot unavailable."
        data = result.parsed_json.get("data")
        if not isinstance(data, dict):
            return "Instrument catalog snapshot unavailable."
        symbols = data.get("symbols") if isinstance(data.get("symbols"), list) else []
        asset_classes = (
            data.get("asset_classes")
            if isinstance(data.get("asset_classes"), list)
            else []
        )
        fx_jpy = [
            str(symbol)
            for symbol in symbols
            if isinstance(symbol, str) and symbol.endswith("JPY")
        ]
        coverage_lines: list[str] = []
        coverage_result = self.cli.run(
            [
                "market",
                "coverage",
                "--timeframe",
                self.config.research.coverage_reference_timeframe,
            ],
            check=False,
        )
        if coverage_result.returncode == 0 and isinstance(
            coverage_result.parsed_json, dict
        ):
            coverage_data = coverage_result.parsed_json.get("data")
            if isinstance(coverage_data, dict):
                eligible_mid: list[str] = []
                eligible_wrap: list[str] = []
                for symbol, payload in coverage_data.items():
                    if not isinstance(symbol, str) or not isinstance(payload, dict):
                        continue
                    months = payload.get("effective_window_months")
                    if not isinstance(months, (int, float)):
                        continue
                    if float(months) >= float(
                        self.config.research.coverage_min_mid_months
                    ):
                        eligible_mid.append(symbol)
                    if float(months) >= float(
                        self.config.research.coverage_min_wrap_up_months
                    ):
                        eligible_wrap.append(symbol)
                if eligible_mid or eligible_wrap:
                    coverage_lines.append(
                        f"Coverage-qualified symbols ({self.config.research.coverage_reference_timeframe} reference): "
                        f">= {self.config.research.coverage_min_mid_months} months: "
                        f"{', '.join(sorted(eligible_mid)[:20]) if eligible_mid else 'none'}"
                    )
                    coverage_lines.append(
                        f"Long-horizon symbols ({self.config.research.coverage_reference_timeframe} reference): "
                        f">= {self.config.research.coverage_min_wrap_up_months} months: "
                        f"{', '.join(sorted(eligible_wrap)[:20]) if eligible_wrap else 'none'}"
                    )
                    coverage_lines.append(
                        "Prefer coverage-qualified symbols first so late-phase horizon checks are less likely to be silently truncated."
                    )
        coverage_block = ("\n".join(coverage_lines) + "\n") if coverage_lines else ""
        return (
            f"Asset classes: {', '.join(str(item) for item in asset_classes)}\n"
            f"JPY-related exact symbols: {', '.join(fx_jpy[:8]) if fx_jpy else 'none'}\n"
            f"{coverage_block}"
            "Use exact symbols from the catalog. Do not assume aliases like JPY are valid instruments."
        )

    def _checkpoint_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "checkpoint-summary.txt"

    def _approx_token_count(self, text: str) -> int:
        compact = " ".join(text.split())
        return max(1, len(compact) // 4)

    def _approx_message_tokens(self, messages: list[ChatMessage]) -> int:
        total = 0
        for message in messages:
            total += self._approx_token_count(message.content) + 8
        return total

    def _profile_template_text(self, tool_context: ToolContext) -> str:
        if not tool_context.profile_template_path.exists():
            return "Portable profile template unavailable."
        return tool_context.profile_template_path.read_text(encoding="utf-8")

    def _artifact_layout_text(self) -> str:
        return (
            "Sensitivity artifact layout (on disk after evaluations):\n"
            "- sensitivity-response.json, deep-replay-job.json, best-cell-path-detail.json when available\n"
            "Prefer inspect_artifact / compare_artifacts for summaries and scores. Do not expect summary.json.\n"
            "Drop to read_file only when those tools lack the detail you need."
        )

    def _seed_to_catalog_hints_text(self, seed_indicator_ids: list[str]) -> str:
        if not seed_indicator_ids:
            return (
                "Seed indicator guidance:\n"
                "- No seeded indicator ids were available for this run.\n"
                "- If the seed hand lacks explicit ids, read the seed file only if needed, then use prepare_profile with catalog ids from context."
            )
        return (
            "Seed indicator guidance:\n"
            f"- Use only these exact seeded indicator ids unless the user explicitly expands scope: {', '.join(seed_indicator_ids)}\n"
            "- Seed concepts are not alternate ids; indicator.meta.id must match one of the exact seeded ids.\n"
            "- Parameter hints below are only for the seeded ids in this run."
        )

    def _run_owned_profiles_summary(self, tool_context: ToolContext) -> str:
        lines: list[str] = []
        for created_file in sorted(tool_context.profiles_dir.glob("*.created.json"))[
            :24
        ]:
            try:
                payload = json.loads(created_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            profile_ref = str(data.get("id", "")).strip()
            profile = (
                data.get("profile") if isinstance(data.get("profile"), dict) else {}
            )
            name = str(profile.get("name", created_file.stem)).strip()
            if profile_ref:
                lines.append(f"- {profile_ref}: {name}")
        if not lines:
            return "No run-owned profiles created yet."
        return "\n".join(lines)

    def _profile_files_summary(self, tool_context: ToolContext) -> str:
        files = sorted(tool_context.profiles_dir.glob("*.json"))
        if not files:
            return "No profile JSON files exist yet."
        lines: list[str] = []
        for path in files[:40]:
            suffix = ""
            if path.name.endswith(".created.json"):
                suffix = " (created metadata)"
            lines.append(f"- {path}{suffix}")
        return "\n".join(lines)

    def _manager_guidance_text(self, step: int) -> str:
        runtime = self._manager_runtime
        lines = ["Recent manager branch guidance (authoritative for branch-control state):"]
        if not runtime.last_hook:
            lines.append("- No manager intervention has fired yet in this run.")
            lines.append(
                "- Until a manager hook fires, follow the controller priority and branch lifecycle packet."
            )
            return "\n".join(lines)
        lines.append(
            f"- latest_hook: {runtime.last_hook} at step {runtime.last_hook_step}"
        )
        if runtime.last_rationale:
            lines.append(f"- latest_rationale: {runtime.last_rationale}")
        if runtime.last_actions_applied:
            action_kinds = [
                str(item.get("kind") or "unknown")
                for item in runtime.last_actions_applied
                if isinstance(item, dict)
            ]
            if action_kinds:
                lines.append("- latest_actions: " + ", ".join(action_kinds[:8]))
        if runtime.manager_notes:
            lines.append(f"- latest_note: {runtime.manager_notes[-1]}")
        if runtime.last_hook_step is not None and step > runtime.last_hook_step:
            lines.append(
                f"- recency: this guidance is {step - runtime.last_hook_step} step(s) old; follow it unless new evidence clearly changes branch state."
            )
        if runtime.invocation_incomplete:
            lines.append(
                f"- manager_status: incomplete ({runtime.last_error or 'unknown_error'})"
            )
        return "\n".join(lines)

    def _latest_successful_step_result(
        self,
        tool_context: ToolContext,
        *,
        tool_names: set[str],
        limit: int = 12,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        payloads = self._load_recent_step_payloads(tool_context, max(1, limit))
        for payload in reversed(payloads):
            results = payload.get("results")
            if not isinstance(results, list):
                continue
            for result in reversed(results):
                if not isinstance(result, dict):
                    continue
                tool = str(result.get("tool") or "").strip()
                if tool not in tool_names:
                    continue
                if result.get("ok") is False or result.get("error"):
                    continue
                return result, payload
        return None, None

    @staticmethod
    def _summarize_candidate_handle(result: dict[str, Any]) -> str | None:
        candidate = (
            result.get("candidate_summary")
            if isinstance(result.get("candidate_summary"), dict)
            else {}
        )
        parts: list[str] = []
        candidate_name = str(
            candidate.get("candidate_name")
            or result.get("candidate_name")
            or candidate.get("draft_name")
            or candidate.get("profile_name")
            or ""
        ).strip()
        if candidate_name:
            parts.append(f"candidate_name={candidate_name}")
        profile_ref = str(
            result.get("profile_ref")
            or result.get("created_profile_ref")
            or candidate.get("profile_ref")
            or ""
        ).strip()
        if profile_ref:
            parts.append(f"profile_ref={profile_ref}")
        next_action = str(result.get("next_recommended_action") or "").strip()
        if next_action:
            parts.append(f"next={next_action}")
        if not parts:
            return None
        return ", ".join(parts[:5])

    @staticmethod
    def _summarize_eval_handle(result: dict[str, Any]) -> str | None:
        parts: list[str] = []
        attempt_id = str(result.get("attempt_id") or "").strip()
        if attempt_id:
            parts.append(f"attempt_id={attempt_id}")
        profile_ref = str(result.get("profile_ref") or "").strip()
        if profile_ref:
            parts.append(f"profile_ref={profile_ref}")
        score = result.get("score")
        if score is not None:
            parts.append(f"score={score}")
        artifact_dir = str(result.get("artifact_dir") or "").strip()
        if artifact_dir:
            parts.append(f"artifact_dir={artifact_dir}")
        next_action = str(result.get("next_recommended_action") or "").strip()
        if next_action:
            parts.append(f"next={next_action}")
        if not parts:
            return None
        return ", ".join(parts[:5])

    @staticmethod
    def _summarize_sweep_handle(result: dict[str, Any]) -> str | None:
        parts: list[str] = []
        inspect_ref = str(result.get("inspect_ref") or "").strip()
        if inspect_ref:
            parts.append(f"inspect_ref={inspect_ref}")
        artifact_dir = str(result.get("artifact_dir") or "").strip()
        if artifact_dir:
            parts.append(f"artifact_dir={artifact_dir}")
        preset = str(result.get("quality_score_preset") or "").strip()
        if preset:
            parts.append(f"score_preset={preset}")
        next_action = str(result.get("next_recommended_action") or "").strip()
        if next_action:
            parts.append(f"next={next_action}")
        if not parts:
            return None
        return ", ".join(parts[:5])

    def _run_outcome_text(self, tool_context: ToolContext) -> str:
        outcome = self._run_outcome_snapshot(self._run_attempts(tool_context.run_id))
        lines = ["Run outcome state (controller-owned; separates live focus from historical bests):"]
        official_type = str(outcome.get("official_winner_type") or "none")
        lines.append(f"- official_winner_type: {official_type}")
        official = outcome.get("official_winner")
        if isinstance(official, dict):
            lines.append(
                "- official_winner: "
                f"attempt={official.get('attempt_id') or 'n/a'} "
                f"family={self._short_family_id(official.get('family_id'))} "
                f"score={self._format_score(official.get('raw_score'))} "
                f"window={self._format_score(official.get('effective_window_months'))}m "
                f"cadence={self._format_score(official.get('trades_per_month'))}/mo"
            )
        else:
            lines.append("- official_winner: none yet")
        for key in (
            "best_live_focus",
            "best_historical_validated",
            "best_historical_retryable",
        ):
            item = outcome.get(key)
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {key}: "
                f"attempt={item.get('attempt_id') or 'n/a'} "
                f"family={self._short_family_id(item.get('family_id'))} "
                f"score={self._format_score(item.get('raw_score'))} "
                f"window={self._format_score(item.get('effective_window_months'))}m "
                f"status={item.get('lifecycle_state') or 'n/a'} "
                f"retention={item.get('retention_status') or 'n/a'} "
                f"live={item.get('currently_live')}"
            )
        rationale = str(outcome.get("rationale") or "").strip()
        if rationale:
            lines.append(f"- rationale: {rationale}")
        return "\n".join(lines)

    def _working_memory_text(self, tool_context: ToolContext) -> str:
        lines = ["Pinned working memory (operational handles; prefer these over guessing ids):"]
        ov = self._branch_overlay
        wrap_up_focus = self._current_wrap_up_focus_state(
            self._run_attempts(tool_context.run_id)
        )
        if ov.validated_leader_family_id:
            branch = self._family_branches.get(ov.validated_leader_family_id)
            parts = [
                f"family={ov.validated_leader_family_id}",
            ]
            if branch:
                if branch.last_profile_ref:
                    parts.append(f"profile_ref={branch.last_profile_ref}")
                if branch.latest_attempt_id:
                    parts.append(f"latest_attempt_id={branch.latest_attempt_id}")
                if branch.best_score is not None:
                    parts.append(f"best_score={self._format_score(branch.best_score)}")
            lines.append("- validated_leader: " + ", ".join(parts[:5]))
        elif ov.provisional_leader_family_id:
            branch = self._family_branches.get(ov.provisional_leader_family_id)
            parts = [
                f"family={ov.provisional_leader_family_id}",
            ]
            if branch:
                if branch.last_profile_ref:
                    parts.append(f"profile_ref={branch.last_profile_ref}")
                if branch.latest_attempt_id:
                    parts.append(f"latest_attempt_id={branch.latest_attempt_id}")
                if branch.latest_score is not None:
                    parts.append(f"latest_score={self._format_score(branch.latest_score)}")
            lines.append("- provisional_leader: " + ", ".join(parts[:5]))
        latest_candidate_result, _ = self._latest_successful_step_result(
            tool_context,
            tool_names={
                "register_profile",
                "validate_profile",
                "mutate_profile",
                "prepare_profile",
            },
        )
        candidate_summary = (
            self._summarize_candidate_handle(latest_candidate_result)
            if isinstance(latest_candidate_result, dict)
            else None
        )
        if candidate_summary:
            lines.append("- current_candidate: " + candidate_summary)
        latest_eval_result, _ = self._latest_successful_step_result(
            tool_context,
            tool_names={"evaluate_candidate"},
        )
        eval_summary = (
            self._summarize_eval_handle(latest_eval_result)
            if isinstance(latest_eval_result, dict)
            else None
        )
        if eval_summary:
            lines.append("- latest_eval: " + eval_summary)
        latest_sweep_result, _ = self._latest_successful_step_result(
            tool_context,
            tool_names={"run_parameter_sweep"},
        )
        sweep_summary = (
            self._summarize_sweep_handle(latest_sweep_result)
            if isinstance(latest_sweep_result, dict)
            else None
        )
        if sweep_summary:
            lines.append("- latest_sweep: " + sweep_summary)
            lines.append(
                "- sweep_rule: sweep outputs are not attempt_ids; inspect via artifact_dir or inspect_ref."
            )
        if isinstance(wrap_up_focus, dict):
            parts = [f"family={wrap_up_focus.get('family_id')}"]
            if wrap_up_focus.get("profile_ref"):
                parts.append(f"profile_ref={wrap_up_focus.get('profile_ref')}")
            if wrap_up_focus.get("selected_attempt_id"):
                parts.append(
                    f"selected_attempt_id={wrap_up_focus.get('selected_attempt_id')}"
                )
            if wrap_up_focus.get("latest_attempt_id"):
                parts.append(f"latest_attempt_id={wrap_up_focus.get('latest_attempt_id')}")
            if wrap_up_focus.get("promotability_status"):
                parts.append(
                    f"promotability={wrap_up_focus.get('promotability_status')}"
                )
            lines.append("- wrap_up_focus: " + ", ".join(parts[:5]))
        if self.last_created_profile_ref:
            lines.append(f"- last_created_profile_ref: {self.last_created_profile_ref}")
        if len(lines) == 1:
            lines.append("- No live handles are pinned yet; use typed tool results from this step.")
        return "\n".join(lines)

    def _current_research_priority_text(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> str:
        ov = self._branch_overlay
        attempts = self._run_attempts(tool_context.run_id)
        raw_best = self._best_attempt(attempts)
        admissible = self._admissible_frontier_snapshot(attempts)
        admissible_best = admissible.get("best") if isinstance(admissible, dict) else None
        raw_best_score = None
        raw_best_candidate = None
        raw_best_family = None
        if isinstance(raw_best, dict):
            raw_best_candidate = str(raw_best.get("candidate_name") or "").strip() or None
            try:
                raw_best_score = (
                    float(raw_best.get("composite_score"))
                    if raw_best.get("composite_score") is not None
                    else None
                )
            except (TypeError, ValueError):
                raw_best_score = None
            raw_best_family = self._family_id_for_profile_ref(
                str(raw_best.get("profile_ref") or "").strip() or None
            )
        validation_digest = (
            ov.last_scored_validation_digest
            if isinstance(ov.last_scored_validation_digest, dict)
            else {}
        )
        validation_evidence = (
            validation_digest.get("validation_evidence")
            if isinstance(validation_digest.get("validation_evidence"), dict)
            else {}
        )
        unresolved_validation = (
            validation_evidence.get("outcome") == vo.VALIDATION_UNRESOLVED
        )
        contrast_required = any(
            branch.structural_contrast_required for branch in self._family_branches.values()
        )
        authoritative_family = (
            ov.validated_leader_family_id or ov.provisional_leader_family_id or None
        )
        phase_name = str(self._run_phase_info(step, step_limit, policy).get("name") or "")
        gut_check = self._current_gut_check_state(attempts, phase_name=phase_name)
        wrap_up_focus = self._current_wrap_up_focus_state(attempts)
        lines = [
            "Current controller priority (authoritative; follow this over raw frontier score when they conflict):"
        ]
        if isinstance(admissible_best, dict):
            lines.append(
                "- Admissible frontier anchor: "
                f"{admissible_best.get('candidate_name') or 'n/a'} "
                f"family={self._short_family_id(admissible_best.get('family_id'))} "
                f"raw_score={self._format_score(admissible_best.get('raw_score'))} "
                f"steering_score={self._format_score(admissible_best.get('steering_score'))} "
                f"window={self._format_score(admissible_best.get('effective_window_months'))}m "
                f"cadence={self._format_score(admissible_best.get('trades_per_month'))}/mo."
            )
        if isinstance(gut_check, dict):
            lines.append(
                "- Immediate pressure test pending: "
                f"{self._short_family_id(gut_check.get('family_id'))} "
                f"must be re-evaluated at {gut_check.get('target_horizon_months')}m "
                "before further frontier chasing. "
                f"Current clean horizon={gut_check.get('max_clean_horizon_months')}m, "
                f"score={self._format_score(gut_check.get('raw_score'))}, "
                f"effective_window={self._format_score(gut_check.get('effective_window_months'))}m, "
                f"cadence={self._format_score(gut_check.get('trades_per_month'))}/mo."
            )
        if ov.validated_leader_family_id:
            lines.append(
                f"- Primary objective: protect and pressure-test the validated leader {self._short_family_id(ov.validated_leader_family_id)}."
            )
            lines.append(
                "- Preferred next moves: validate or inspect the validated leader path, compare close contrasts, and avoid broad frontier chasing unless the branch packet explicitly reopens search."
            )
        elif ov.provisional_leader_family_id and unresolved_validation:
            lines.append(
                f"- Primary objective: resolve provisional leader validation for {self._short_family_id(ov.provisional_leader_family_id)} before chasing raw frontier winners."
            )
            stale_text = ""
            threshold = self.config.research.reseed_after_stale_validation_steps
            if threshold > 0 and self._validation_stale_without_validated > 0:
                stale_text = (
                    f" Validation has been stale for {self._validation_stale_without_validated}/{threshold} controller steps."
                )
            lines.append(
                "- Preferred next moves: inspect the latest provisional-leader artifact, run disciplined longer-horizon validation or close contrast, and improve promotability evidence."
                + stale_text
            )
        elif ov.provisional_leader_family_id:
            lines.append(
                f"- Primary objective: either promote or replace the provisional leader {self._short_family_id(ov.provisional_leader_family_id)} using direct validation evidence."
            )
            lines.append(
                "- Preferred next moves: validation-centric follow-up or a disciplined nearby contrast that clarifies whether this family deserves leadership."
            )
        elif ov.budget_mode == bl.BUDGET_COLLAPSE_RECOVERY or contrast_required:
            lines.append(
                "- Primary objective: structural contrast. Do not keep exploiting blocked or exhausted families."
            )
            lines.append(
                "- Preferred next moves: prepare a fresh scaffold/clone on a different family, instrument cluster, timeframe architecture, or directional logic."
            )
        elif ov.budget_mode == bl.BUDGET_WRAP_UP:
            if isinstance(wrap_up_focus, dict):
                lines.append(
                    "- Primary objective: convert remaining budget into decisive evidence on wrap-up focus "
                    + self._short_family_id(wrap_up_focus.get("family_id"))
                    + ", not broad exploration."
                )
                lines.append(
                    "- Preferred next moves: validate, inspect, or compare the wrap-up focus path directly; avoid unrelated family tuning unless that focus is clearly dead."
                )
            else:
                lines.append(
                    "- Primary objective: convert remaining budget into decisive validation evidence, not broad exploration."
                )
                lines.append(
                    "- Preferred next moves: validate survivors, inspect artifacts, compare contenders, and only open new branches if no leader path can be resolved."
                )
        else:
            lines.append(
                "- Primary objective: establish the next credible leader through diverse scored evidence."
            )
            lines.append(
                "- Preferred next moves: prepare/validate/register/evaluate candidates across distinct families or instrument clusters."
            )
        if authoritative_family and raw_best_family and authoritative_family != raw_best_family:
            raw_best_bits = [self._short_family_id(raw_best_family)]
            if raw_best_candidate:
                raw_best_bits.append(raw_best_candidate)
            if raw_best_score is not None:
                raw_best_bits.append(f"score={self._format_score(raw_best_score)}")
            lines.append(
                "- Conflict note: raw frontier best ("
                + ", ".join(raw_best_bits)
                + ") does not override current branch authority "
                + self._short_family_id(authoritative_family)
                + "."
            )
        elif isinstance(admissible_best, dict):
            admissible_family = str(admissible_best.get("family_id") or "").strip() or None
            if admissible_family and raw_best_family and admissible_family != raw_best_family:
                lines.append(
                    "- Conflict note: raw frontier best "
                    + self._short_family_id(raw_best_family)
                    + " is not the current admissible leader. Follow admissibility and branch state over raw spikes."
                )
        if (
            ov.budget_mode == bl.BUDGET_VALIDATION
            and ov.provisional_leader_family_id
            and not ov.validated_leader_family_id
        ):
            lines.append(
                "- Planning rule: treat validation resolution as the default path; do not pivot to unrelated broad search without a concrete reason from the branch packet."
            )
        if phase_name == "wrap_up":
            lines.append(
                "- Phase note: wrap_up is active, so extra exploration needs strong justification."
            )
        return "\n".join(lines)

    def _step_log_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "controller-log.jsonl"

    def _run_state_prompt(
        self,
        tool_context: ToolContext,
        policy: RunPolicy,
        *,
        step: int | None = None,
        step_limit: int | None = None,
    ) -> str:
        if self._uses_local_transformers_provider():
            return self._local_compact_run_state_prompt(
                tool_context,
                policy,
                step=step,
                step_limit=step_limit,
            )
        checkpoint_path = self._checkpoint_path(tool_context)
        checkpoint = (
            checkpoint_path.read_text(encoding="utf-8")
            if checkpoint_path.exists()
            else "No checkpoint summary exists yet."
        )
        effective_step = step or 1
        effective_step_limit = step_limit or self.config.research.max_steps
        phase_info = self._run_phase_info(effective_step, effective_step_limit, policy)
        horizon_policy = self._horizon_policy_snapshot(
            effective_step, effective_step_limit, policy
        )
        score_target = self._score_target_snapshot(tool_context)
        next_action_template_text = self._followup_next_action_template_text(tool_context)
        soft_wrap_note = self._soft_wrap_note(policy)
        return (
            f"Mode: {policy.mode_name}\n"
            f"Run id: {tool_context.run_id}\n"
            "Auth status: already verified by controller at run start.\n"
            f"Allow finish: {policy.allow_finish}\n"
            f"Step: {effective_step}/{effective_step_limit}\n"
            f"Run phase: {phase_info['name']}\n"
            f"Phase guidance: {phase_info['summary']}\n"
            f"Horizon target: {horizon_policy['summary']}\n"
            f"Horizon guidance: {horizon_policy['guidance']}\n"
            f"Horizon rationale: {horizon_policy['rationale']}\n"
            f"Score target: {score_target['summary']}\n"
            f"Score target rationale: {score_target['rationale']}\n"
            f"Next action template: {next_action_template_text}\n"
            f"Operating window: {policy.window_start or 'none'} -> {policy.window_end or 'none'} ({policy.timezone_name})\n"
            f"{soft_wrap_note + chr(10) if soft_wrap_note else ''}"
            f"{self._current_research_priority_text(tool_context, effective_step, effective_step_limit, policy)}\n\n"
            f"{self._manager_guidance_text(effective_step)}\n\n"
            f"{self._run_outcome_text(tool_context)}\n\n"
            f"{self._working_memory_text(tool_context)}\n\n"
            f"{self._branch_lifecycle_run_packet_text(tool_context, effective_step, effective_step_limit)}\n\n"
            f"{self._retention_and_exploit_status_text(tool_context)}\n\n"
            f"{self._timeframe_mismatch_status_text()}\n\n"
            f"Current seed hand:\n{self._seed_text(tool_context)}\n\n"
            f"Sticky indicator context:\n{tool_context.indicator_catalog_summary or 'Unavailable'}\n\n"
            f"Seeded indicator parameter hints:\n{tool_context.seed_indicator_parameter_hints or 'Unavailable'}\n\n"
            f"Sticky instrument context:\n{tool_context.instrument_catalog_summary or 'Unavailable'}\n\n"
            f"{self._seed_to_catalog_hints_text(self._seed_indicator_ids(tool_context.seed_prompt_path))}\n\n"
            f"Run-owned profiles so far:\n{self._run_owned_profiles_summary(tool_context)}\n\n"
            f"Checkpoint summary:\n{checkpoint}\n\n"
            f"Recent attempts:\n{self._recent_attempts_summary(tool_context)}\n\n"
            f"Raw frontier snapshot (informational only; not leadership authority):\n{self._frontier_snapshot_text(tool_context)}\n\n"
            f"{self._recent_behavior_digest_text(tool_context)}\n"
        )

    def _local_seed_context_prompt_state(
        self,
        tool_context: ToolContext,
    ) -> dict[str, Any] | None:
        if not tool_context.seed_prompt_path or not tool_context.seed_prompt_path.exists():
            return None
        try:
            payload = json.loads(tool_context.seed_prompt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        exploration_goal = (
            payload.get("exploration_goal")
            if isinstance(payload.get("exploration_goal"), dict)
            else {}
        )
        worker_split = (
            exploration_goal.get("worker_split")
            if isinstance(exploration_goal.get("worker_split"), list)
            else []
        )
        seed_context = {
            "exploration_goal_id": exploration_goal.get("id"),
            "exploration_goal_summary": exploration_goal.get("summary"),
            "seed_indicators": list(payload.get("indicators") or [])[:6],
            "timeframes": list(payload.get("timeframes") or [])[:6],
            "worker_split": [
                {
                    "branch": item.get("branch"),
                    "goal": item.get("goal"),
                }
                for item in worker_split[:2]
                if isinstance(item, dict)
            ],
        }
        return {
            key: value
            for key, value in seed_context.items()
            if value not in (None, "", [], {})
        } or None

    def _local_opening_seed_payload(
        self,
        tool_context: ToolContext,
    ) -> dict[str, Any]:
        if not tool_context.seed_prompt_path or not tool_context.seed_prompt_path.exists():
            return {}
        try:
            payload = json.loads(tool_context.seed_prompt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _local_opening_prefers_basket_start(
        self,
        tool_context: ToolContext,
    ) -> bool:
        payload = self._local_opening_seed_payload(tool_context)
        exploration_goal = (
            payload.get("exploration_goal")
            if isinstance(payload.get("exploration_goal"), dict)
            else {}
        )
        texts: list[str] = []
        for candidate in (
            exploration_goal.get("summary"),
            exploration_goal.get("id"),
        ):
            if isinstance(candidate, str) and candidate.strip():
                texts.append(candidate.lower())
        worker_split = (
            exploration_goal.get("worker_split")
            if isinstance(exploration_goal.get("worker_split"), list)
            else []
        )
        for item in worker_split:
            if not isinstance(item, dict):
                continue
            goal_text = item.get("goal")
            if isinstance(goal_text, str) and goal_text.strip():
                texts.append(goal_text.lower())
        joined = "\n".join(texts)
        if any(keyword in joined for keyword in LOCAL_OPENING_NARROW_GOAL_KEYWORDS):
            return False
        return any(keyword in joined for keyword in LOCAL_OPENING_BROAD_GOAL_KEYWORDS)

    def _local_opening_coverage_symbols(self) -> list[str]:
        coverage_result = self.cli.run(
            [
                "market",
                "coverage",
                "--timeframe",
                self.config.research.coverage_reference_timeframe,
            ],
            check=False,
        )
        if coverage_result.returncode != 0 or not isinstance(
            coverage_result.parsed_json, dict
        ):
            return []
        coverage_data = coverage_result.parsed_json.get("data")
        if not isinstance(coverage_data, dict):
            return []
        eligible: list[str] = []
        for symbol, payload in coverage_data.items():
            if not isinstance(symbol, str) or not isinstance(payload, dict):
                continue
            months = payload.get("effective_window_months")
            if not isinstance(months, (int, float)):
                continue
            if float(months) >= float(self.config.research.coverage_min_mid_months):
                eligible.append(symbol)
        return sorted(set(eligible))

    def _local_opening_starter_instruments(
        self,
        tool_context: ToolContext,
    ) -> tuple[list[str], str]:
        symbols = self._local_opening_coverage_symbols()
        if not symbols:
            return [], "coverage_unavailable"
        preferred = [
            symbol
            for symbol in LOCAL_OPENING_PRIORITY_INSTRUMENTS
            if symbol in symbols
        ]
        pool = preferred or symbols
        if self._local_opening_prefers_basket_start(tool_context):
            return pool[: min(3, len(pool))], "broad_coverage_default"
        return pool[:1], "single_symbol_default"

    def _local_opening_grounding_prompt_state(
        self,
        tool_context: ToolContext,
    ) -> dict[str, Any] | None:
        starter_instruments, starter_rule = self._local_opening_starter_instruments(
            tool_context
        )
        candidate_name_hint = "cand1"
        grounding = {
            "allowed_seed_instruments": starter_instruments,
            "preferred_initial_instruments": starter_instruments,
            "preferred_initial_instrument_rule": starter_rule,
            "candidate_name_hint": candidate_name_hint,
        }
        return {
            key: value
            for key, value in grounding.items()
            if value not in (None, "", [], {})
        } or None

    def _local_recent_step_window_prompt_state(
        self,
        tool_context: ToolContext,
    ) -> list[dict[str, Any]]:
        payloads = self._load_recent_step_payloads(
            tool_context,
            max(1, int(self.config.research.recent_step_window_steps)),
        )
        items: list[dict[str, Any]] = []
        for payload in payloads[-self.config.research.recent_step_window_steps :]:
            if not isinstance(payload, dict):
                continue
            actions = payload.get("actions") if isinstance(payload.get("actions"), list) else []
            results = payload.get("results") if isinstance(payload.get("results"), list) else []
            items.append(
                {
                    "step": payload.get("step"),
                    "action_signatures": [
                        self._prompt_visible_action_signature(action)
                        for action in actions
                        if isinstance(action, dict)
                    ],
                    "result_summary": [
                        self._history_result_summary(result)
                        for result in results
                        if isinstance(result, dict)
                    ],
                }
            )
        return items

    def _recent_known_instruments_for_handle(
        self,
        tool_context: ToolContext,
        *,
        candidate_name: str | None = None,
        profile_ref: str | None = None,
        limit: int = 8,
    ) -> list[str] | None:
        candidate_name = str(candidate_name or "").strip() or None
        profile_ref = str(profile_ref or "").strip() or None
        observed_sets: list[tuple[str, ...]] = []
        payloads = self._load_recent_step_payloads(tool_context, max(1, limit))
        for payload in reversed(payloads):
            actions = payload.get("actions")
            if not isinstance(actions, list):
                continue
            for action in reversed(actions):
                if not isinstance(action, dict):
                    continue
                action = _pathless_action_from_legacy_fields(action)
                matches_handle = False
                if candidate_name and str(action.get("candidate_name") or "").strip() == candidate_name:
                    matches_handle = True
                if profile_ref and str(action.get("profile_ref") or "").strip() == profile_ref:
                    matches_handle = True
                if not matches_handle:
                    continue
                instruments = _normalize_instrument_list(action.get("instruments"))
                if not instruments or _uses_forbidden_opening_instrument(instruments):
                    continue
                token = tuple(instruments)
                if token not in observed_sets:
                    observed_sets.append(token)
        if len(observed_sets) == 1:
            return list(observed_sets[0])
        if len(observed_sets) > 1:
            return None
        profile_path = self._resolve_local_profile_path(
            tool_context,
            candidate_name=candidate_name,
            profile_ref=profile_ref,
            require_exists=True,
        )
        if profile_path is None:
            return None
        instruments = _extract_profile_instruments_from_payload(
            _read_json_if_exists(profile_path)
        )
        if not instruments or _uses_forbidden_opening_instrument(instruments):
            return None
        return instruments

    def _followup_next_action_template_prompt_state(
        self,
        tool_context: ToolContext,
    ) -> dict[str, Any] | None:
        latest_result, _payload = self._latest_successful_step_result(
            tool_context,
            tool_names={
                "prepare_profile",
                "mutate_profile",
                "validate_profile",
                "register_profile",
            },
        )
        if not isinstance(latest_result, dict):
            return None
        tool = str(latest_result.get("tool") or "").strip()
        candidate_summary = (
            latest_result.get("candidate_summary")
            if isinstance(latest_result.get("candidate_summary"), dict)
            else {}
        )
        candidate_name = str(
            candidate_summary.get("candidate_name")
            or latest_result.get("candidate_name")
            or candidate_summary.get("draft_name")
            or candidate_summary.get("profile_name")
            or ""
        ).strip()
        profile_ref = str(
            latest_result.get("profile_ref")
            or latest_result.get("created_profile_ref")
            or candidate_summary.get("profile_ref")
            or ""
        ).strip()
        if tool in {"prepare_profile", "mutate_profile"} and candidate_name:
            return {
                "tool": "validate_profile",
                "candidate_name": candidate_name,
            }
        if (
            tool == "validate_profile"
            and bool(latest_result.get("ready_for_registration"))
            and candidate_name
        ):
            return {
                "tool": "register_profile",
                "candidate_name": candidate_name,
            }
        if tool == "register_profile" and (
            bool(latest_result.get("ready_to_evaluate")) or profile_ref or candidate_name
        ):
            instruments = self._recent_known_instruments_for_handle(
                tool_context,
                candidate_name=candidate_name or None,
                profile_ref=profile_ref or None,
            )
            if not instruments:
                return None
            template: dict[str, Any] = {
                "tool": "evaluate_candidate",
                "instruments": instruments,
                "timeframe_policy": "profile_default",
                "evaluation_mode": "screen",
            }
            if profile_ref:
                template["profile_ref"] = profile_ref
            elif candidate_name:
                template["candidate_name"] = candidate_name
            else:
                return None
            return template
        return None

    def _followup_next_action_template_text(
        self,
        tool_context: ToolContext,
    ) -> str:
        template = self._followup_next_action_template_prompt_state(tool_context)
        if not isinstance(template, dict):
            return "No deterministic next_action_template is active."
        return json.dumps(template, ensure_ascii=True)

    def _local_compact_run_state_prompt(
        self,
        tool_context: ToolContext,
        policy: RunPolicy,
        *,
        step: int | None = None,
        step_limit: int | None = None,
    ) -> str:
        effective_step = step or 1
        effective_step_limit = step_limit or self.config.research.max_steps
        phase_info = self._run_phase_info(effective_step, effective_step_limit, policy)
        horizon_policy = self._horizon_policy_snapshot(
            effective_step, effective_step_limit, policy
        )
        score_target = self._score_target_snapshot(tool_context)
        prompt_state = {
            "run": {
                "run_id": tool_context.run_id,
                "run_dir": str(tool_context.run_dir),
            },
            "controller": {
                "step": effective_step,
                "phase": phase_info.get("name"),
                "horizon_target": horizon_policy.get("summary"),
                "score_target": score_target.get("summary"),
            },
            "seed_context": self._local_seed_context_prompt_state(tool_context),
            "opening_grounding": self._local_opening_grounding_prompt_state(
                tool_context
            ),
            "timeframe_status": self._get_timeframe_mismatch_status(),
            "next_action_template": self._followup_next_action_template_prompt_state(
                tool_context
            ),
            "recent_step_window": self._local_recent_step_window_prompt_state(
                tool_context
            ),
            "recent_attempts": list(
                load_run_attempts(tool_context.run_dir)[
                    -self.config.research.recent_attempts_window :
                ]
            ),
        }
        compact_v2 = build_prompt_variants({"prompt_state": prompt_state}).get(
            "compact_v2", {}
        )
        return json.dumps(compact_v2, ensure_ascii=True)

    def _retention_and_exploit_status_text(self, tool_context: ToolContext) -> str:
        exploit_status = self._get_same_family_exploit_status()
        lines = ["Retention and exploit pacing status:"]
        exploit_msg = exploit_status.get("message")
        if exploit_msg:
            lines.append(f"- exploit_cap: {exploit_msg}")
        else:
            lines.append(
                f"- exploit steps: {exploit_status.get('consecutive_exploit_steps', 0)}/{exploit_status.get('exploit_cap', 3)} (no cap triggered)"
            )
        family_states = []
        for family_id, br in self._family_branches.items():
            if self._family_mutation_counts.get(family_id, 0) == 0 and not (
                br.retention_check_done or br.retention_check_passed
            ):
                continue
            passed = br.retention_check_passed
            done = br.retention_check_done
            support = br.retention_support_quality or "unknown"
            mutations = self._family_mutation_counts.get(family_id, 0)
            short_family = family_id[:16] + "..." if len(family_id) > 16 else family_id
            if done:
                if passed is True:
                    family_states.append(
                        f"{short_family}: retention PASSED (support={support}, mutations={mutations})"
                    )
                else:
                    family_states.append(
                        f"{short_family}: retention FAILED (support={support})"
                    )
            else:
                family_states.append(
                    f"{short_family}: pending retention check (support={support}, mutations={mutations})"
                )
        if family_states:
            lines.append("- Family retention states:")
            for state in family_states[:5]:
                lines.append(f"  - {state}")
        return "\n".join(lines)

    def _timeframe_mismatch_status_text(self) -> str:
        status = self._get_timeframe_mismatch_status()
        if not status.get("has_mismatch"):
            return "Timeframe intent status: No auto-adjustments detected."
        lines = ["Timeframe intent status:"]
        latest = status.get("latest", {})
        lines.append(
            f"- Latest mismatch: requested={latest.get('requested')} effective={latest.get('effective')}"
        )
        lines.append(f"- Total mismatches: {status.get('total_mismatches', 0)}")
        msg = status.get("message")
        if msg:
            lines.append(f"- Warning: {msg}")
        if status.get("repeat_blocked"):
            lines.append(
                "- BLOCKED: Repeated requests for same mismatched timeframe are blocked."
            )
        return "\n".join(lines)

    def _recent_behavior_digest_text(self, tool_context: ToolContext) -> str:
        attempts = self._run_attempts(tool_context.run_id)
        if not attempts:
            return "Behavior digest: No evaluated attempts yet."
        recent_attempts = [a for a in attempts if a.get("composite_score") is not None]
        if not recent_attempts:
            return "Behavior digest: No scored attempts yet."
        last_attempt = recent_attempts[-1]
        digest = self._generate_behavior_digest(last_attempt)
        candidate_name = last_attempt.get("candidate_name", "unknown")
        score = last_attempt.get("composite_score", "n/a")
        return (
            f"Most recent behavior digest (seq={last_attempt.get('sequence')}, candidate={candidate_name}, score={score}):\n"
            + self._format_behavior_digest_text(digest)
        )

    def _serialize_tool_result(self, result: Any) -> str:
        if isinstance(result, CommandResult):
            parsed_json_preview: dict[str, Any] | list[Any] | None
            if result.parsed_json is None:
                parsed_json_preview = None
            else:
                parsed_text = json.dumps(result.parsed_json, ensure_ascii=True)
                if len(parsed_text) <= 2500:
                    parsed_json_preview = result.parsed_json
                else:
                    parsed_json_preview = {
                        "preview": parsed_text[:2500],
                        "truncated": True,
                    }
            payload = {
                "argv": result.argv,
                "cwd": str(result.cwd),
                "returncode": result.returncode,
                "stdout": result.stdout[:4000],
                "stderr": result.stderr[:2000],
                "parsed_json": parsed_json_preview,
            }
            return json.dumps(payload, ensure_ascii=True)
        if isinstance(result, (dict, list)):
            return json.dumps(result, ensure_ascii=True)
        return str(result)

    def _history_action_summary(self, action: dict[str, Any]) -> str:
        tool = str(action.get("tool", "unknown"))
        if tool == "write_file":
            return f"write_file path={action.get('path', '')}"
        if tool == "run_cli":
            args = action.get("args")
            if isinstance(args, list):
                return "run_cli " + " ".join(str(item) for item in args[:20])
            command = action.get("command")
            if isinstance(command, str):
                return f"run_cli {command[:400]}"
        if tool in {"read_file", "list_dir", "log_attempt", "finish"}:
            return json.dumps(
                {key: value for key, value in action.items() if key != "content"},
                ensure_ascii=True,
            )
        if tool in {
            "prepare_profile",
            "mutate_profile",
            "validate_profile",
            "register_profile",
            "evaluate_candidate",
        }:
            return json.dumps(
                self._prompt_visible_action_signature(action),
                ensure_ascii=True,
            )
        return json.dumps(
            {key: value for key, value in action.items() if key != "content"},
            ensure_ascii=True,
        )

    def _history_result_summary(self, result: dict[str, Any]) -> dict[str, Any]:
        tool = str(result.get("tool", "unknown"))
        if result.get("error") and tool not in {"yield_guard", "finish"}:
            return {
                "tool": tool,
                "error": str(result.get("error"))[:500],
            }
        if tool == "run_cli" or tool in tt.TYPED_TOOLS_CLI_WRAPPER:
            payload = (
                result.get("result") if isinstance(result.get("result"), dict) else {}
            )
            summarized: dict[str, Any] = {
                "tool": tool,
                "ok": bool(result.get("ok")),
            }
            candidate_summary = (
                result.get("candidate_summary")
                if isinstance(result.get("candidate_summary"), dict)
                else None
            )
            candidate_name = str(
                result.get("candidate_name")
                or (candidate_summary or {}).get("candidate_name")
                or ""
            ).strip()
            if candidate_name:
                summarized["candidate_name"] = candidate_name
            profile_ref = str(
                result.get("profile_ref")
                or result.get("created_profile_ref")
                or (candidate_summary or {}).get("profile_ref")
                or ""
            ).strip()
            if profile_ref:
                summarized["profile_ref"] = profile_ref
            if result.get("controller_hint"):
                summarized["controller_hint"] = str(result.get("controller_hint"))[:220]
            if isinstance(payload, dict):
                argv = payload.get("argv")
                cli_args = argv if isinstance(argv, list) else []
                stdout = payload.get("stdout")
                stderr = payload.get("stderr")
                parsed = payload.get("parsed_json")
                if isinstance(parsed, dict):
                    if len(cli_args) >= 1:
                        if "compare-sensitivity" in cli_args:
                            compare_summary = _compact_compare_summary_for_prompt(parsed)
                            if compare_summary:
                                summarized["compare_summary"] = compare_summary
                if bool(result.get("timeframe_auto_adjusted")):
                    summarized["timeframe_auto_adjusted"] = True
                if isinstance(result.get("timeframe_mismatch"), dict):
                    summarized["timeframe_mismatch"] = result.get("timeframe_mismatch")
                if (
                    isinstance(stderr, str)
                    and stderr.strip()
                    and not bool(result.get("ok"))
                ):
                    summarized["stderr"] = stderr[:500]
            if tool in {"prepare_profile", "mutate_profile", "validate_profile"}:
                summarized.update(
                    _prompt_visible_candidate_fields(
                        candidate_summary,
                        include_strategy=True,
                        include_profile_ref=False,
                    )
                )
                if tool == "mutate_profile" and result.get("mutation_summary"):
                    summarized["mutation_summary"] = str(result.get("mutation_summary"))[:120]
            elif tool == "register_profile":
                summarized.update(
                    _prompt_visible_candidate_fields(
                        candidate_summary,
                        include_strategy=False,
                        include_profile_ref=True,
                    )
                )
            elif tool == "evaluate_candidate":
                attempt_id = str(result.get("attempt_id") or "").strip()
                if attempt_id:
                    summarized["attempt_id"] = attempt_id
                if result.get("score") is not None:
                    summarized["score"] = result.get("score")
                if result.get("effective_window_months") is not None:
                    summarized["effective_window_months"] = result.get(
                        "effective_window_months"
                    )
                if result.get("trades_per_month") is not None:
                    summarized["trades_per_month"] = result.get("trades_per_month")
                if result.get("resolved_trades") is not None:
                    summarized["resolved_trades"] = result.get("resolved_trades")
                if result.get("artifact_dir"):
                    summarized["artifact_dir"] = result.get("artifact_dir")
                requested_timeframe = str(result.get("requested_timeframe") or "").strip()
                effective_timeframe = str(result.get("effective_timeframe") or "").strip()
                if requested_timeframe and effective_timeframe:
                    summarized["timeframes"] = {
                        "requested": requested_timeframe,
                        "effective": effective_timeframe,
                    }
            elif tool == "run_parameter_sweep":
                if result.get("inspect_ref"):
                    summarized["inspect_ref"] = str(result.get("inspect_ref"))[:120]
                if result.get("artifact_dir"):
                    summarized["artifact_dir"] = str(result.get("artifact_dir"))[:260]
                if result.get("best_score") is not None:
                    summarized["best_score"] = result.get("best_score")
                if result.get("quality_score_preset"):
                    summarized["quality_score_preset"] = str(
                        result.get("quality_score_preset")
                    )[:80]
            if result.get("ready_for_registration") is not None:
                summarized["ready_for_registration"] = bool(
                    result.get("ready_for_registration")
                )
            if result.get("ready_to_evaluate") is not None:
                summarized["ready_to_evaluate"] = bool(result.get("ready_to_evaluate"))
            if result.get("material_changes") is not None:
                summarized["material_changes"] = bool(result.get("material_changes"))
            return summarized
        if tool == "inspect_artifact":
            summarized = {
                "tool": tool,
                "ok": bool(result.get("ok", True)),
                "artifact_dir": result.get("artifact_dir"),
            }
            view = str(result.get("view") or "").strip()
            if view and view != "summary":
                summarized["view"] = view
            if result.get("artifact_kind"):
                summarized["artifact_kind"] = result.get("artifact_kind")
            if isinstance(result.get("sweep_summary"), dict):
                compact_sweep = _compact_sweep_summary_for_prompt(
                    result.get("sweep_summary")
                )
                if compact_sweep:
                    summarized["sweep_summary"] = compact_sweep
            if isinstance(result.get("compare_summary"), dict):
                compact_compare = _compact_compare_summary_for_prompt(
                    result.get("compare_summary")
                )
                if compact_compare:
                    summarized["compare_summary"] = compact_compare
            if result.get("effective_window_months_hint") is not None:
                summarized["effective_window_months_hint"] = result.get(
                    "effective_window_months_hint"
                )
            attempt_hint = (
                result.get("attempt_ledger_hint")
                if isinstance(result.get("attempt_ledger_hint"), dict)
                else None
            )
            if attempt_hint:
                compact_attempt_hint = {
                    key: attempt_hint.get(key)
                    for key in (
                        "attempt_id",
                        "composite_score",
                        "effective_window_months",
                        "requested_timeframe",
                        "effective_timeframe",
                        "validation_outcome",
                    )
                    if attempt_hint.get(key) is not None
                }
                if compact_attempt_hint:
                    summarized["attempt"] = compact_attempt_hint
            if result.get("controller_hint"):
                summarized["controller_hint"] = str(result.get("controller_hint"))[:220]
            return summarized
        if tool == "compare_artifacts":
            summarized = {
                "tool": tool,
                "ok": bool(result.get("ok", True)),
            }
            ranked_preview = _compact_ranked_comparison_for_prompt(
                result.get("ranked_comparison")
            )
            if ranked_preview:
                summarized["ranked_preview"] = ranked_preview
            dominant_deltas = result.get("dominant_deltas")
            if isinstance(dominant_deltas, list) and dominant_deltas:
                summarized["dominant_deltas"] = dominant_deltas[:3]
            suggested = str(
                result.get("suggested_next_move")
                or result.get("next_recommended_action")
                or ""
            ).strip()
            if suggested:
                summarized["suggested_next_move"] = suggested[:180]
            return summarized
        if tool == "read_file":
            content = str(result.get("content", ""))
            return {
                "tool": tool,
                "path": str(result.get("path", "")),
                "content_preview": content[:1200],
            }
        if tool == "list_dir":
            items = result.get("items")
            return {
                "tool": tool,
                "path": str(result.get("path", "")),
                "items": items[:40] if isinstance(items, list) else [],
            }
        if tool == "write_file":
            return {
                "tool": tool,
                "path": str(result.get("path", "")),
                "bytes": result.get("bytes"),
            }
        if tool == "log_attempt":
            return {
                "tool": tool,
                "result": result.get("result"),
            }
        if tool in {"yield_guard", "finish"}:
            return result
        return result

    def _history_tool_results_message_content(
        self,
        results: list[dict[str, Any]],
    ) -> str:
        return "Tool results:\n" + json.dumps(
            [
                self._history_result_summary(result)
                for result in results
                if isinstance(result, dict)
            ],
            ensure_ascii=True,
            indent=2,
        )

    def _validate_action(self, action: Any) -> str | None:
        if not isinstance(action, dict):
            return "Action must be an object."
        tool = str(action.get("tool", "")).strip()
        if not tool:
            return "Action is missing tool."
        if tool not in tt.ALL_CONTROLLER_TOOLS:
            return f"Unknown tool: {tool}"
        if tool == "write_file":
            path = action.get("path")
            if not isinstance(path, str) or not path.strip():
                return "write_file requires a non-empty string path."
            content = action.get("content")
            if not isinstance(content, str) or not content.strip():
                return "write_file requires a non-empty string content field."
            return None
        if tool in {"read_file", "list_dir"}:
            path = action.get("path")
            if not isinstance(path, str) or not path.strip():
                return f"{tool} requires a non-empty string path."
            return None
        if tool == "log_attempt":
            artifact_dir = action.get("artifact_dir")
            if not isinstance(artifact_dir, str) or not artifact_dir.strip():
                return "log_attempt requires a non-empty string artifact_dir."
            return None
        if tool == "finish":
            summary = action.get("summary", "")
            if summary is not None and not isinstance(summary, str):
                return "finish summary must be a string."
            return None
        if tool == "prepare_profile":
            mode = str(action.get("mode") or "").strip()
            if mode not in {"scaffold_from_seed", "clone_local", "from_template"}:
                return "prepare_profile requires mode scaffold_from_seed, clone_local, or from_template."
            if mode == "scaffold_from_seed":
                ids = action.get("indicator_ids")
                if not isinstance(ids, list) or not ids:
                    return "prepare_profile scaffold_from_seed requires indicator_ids array."
            if mode == "clone_local":
                has_source_name = isinstance(action.get("source_candidate_name"), str) and bool(
                    str(action.get("source_candidate_name")).strip()
                )
                has_source_ref = isinstance(action.get("source_profile_ref"), str) and bool(
                    str(action.get("source_profile_ref")).strip()
                )
                if not has_source_name and not has_source_ref:
                    return "prepare_profile clone_local requires source_candidate_name or source_profile_ref."
            return None
        if tool == "mutate_profile":
            has_name = isinstance(action.get("candidate_name"), str) and bool(
                str(action.get("candidate_name")).strip()
            )
            has_ref = isinstance(action.get("profile_ref"), str) and bool(
                str(action.get("profile_ref")).strip()
            )
            if not has_name and not has_ref:
                return "mutate_profile requires candidate_name or profile_ref."
            mutations = action.get("mutations")
            if not isinstance(mutations, list) or not mutations:
                return "mutate_profile requires mutations array."
            return None
        if tool == "validate_profile":
            has_name = isinstance(action.get("candidate_name"), str) and bool(
                str(action.get("candidate_name")).strip()
            )
            has_ref = isinstance(action.get("profile_ref"), str) and bool(
                str(action.get("profile_ref")).strip()
            )
            if not has_name and not has_ref:
                return "validate_profile requires candidate_name or profile_ref."
            return None
        if tool == "register_profile":
            op = str(action.get("operation") or "create").strip().lower()
            if op == "update":
                ref = action.get("profile_ref")
                if not isinstance(ref, str) or not ref.strip():
                    return "register_profile update requires profile_ref."
            has_name = isinstance(action.get("candidate_name"), str) and bool(
                str(action.get("candidate_name")).strip()
            )
            has_ref = isinstance(action.get("profile_ref"), str) and bool(
                str(action.get("profile_ref")).strip()
            )
            if not has_name and not has_ref:
                return "register_profile requires candidate_name or profile_ref."
            return None
        if tool == "evaluate_candidate":
            inst = action.get("instruments")
            if not isinstance(inst, list) or not inst:
                return "evaluate_candidate requires instruments array."
            has_ref = isinstance(action.get("profile_ref"), str) and bool(
                str(action.get("profile_ref")).strip()
            )
            has_name = isinstance(action.get("candidate_name"), str) and bool(
                str(action.get("candidate_name")).strip()
            )
            if not has_ref and not has_name:
                return "evaluate_candidate requires profile_ref or candidate_name."
            return None
        if tool == "run_parameter_sweep":
            ref = action.get("profile_ref")
            if not isinstance(ref, str) or not ref.strip():
                return "run_parameter_sweep requires profile_ref."
            axes = action.get("axes")
            if not isinstance(axes, list) or not axes:
                return "run_parameter_sweep requires axes array."
            return None
        if tool == "inspect_artifact":
            ad = action.get("artifact_dir")
            aid = action.get("attempt_id")
            if (not isinstance(ad, str) or not ad.strip()) and (
                not isinstance(aid, str) or not aid.strip()
            ):
                return "inspect_artifact requires artifact_dir or attempt_id."
            return None
        if tool == "compare_artifacts":
            ids = action.get("attempt_ids") or action.get("artifact_dirs")
            if not isinstance(ids, list) or not ids:
                return "compare_artifacts requires attempt_ids or artifact_dirs array."
            return None
        if tool == "run_cli":
            try:
                self._normalize_cli_args(action)
            except Exception as exc:
                return str(exc)
            return None
        return None

    def _validate_actions(self, actions: Any) -> list[str]:
        if not isinstance(actions, list) or not actions:
            return ["Response must include a non-empty actions array."]
        if len(actions) > 3:
            return [f"Response must include at most 3 actions, got {len(actions)}."]
        errors: list[str] = []
        for index, action in enumerate(actions, start=1):
            error = self._validate_action(action)
            if error:
                errors.append(f"Action {index}: {error}")
        return errors

    def _validate_finish_timing(
        self,
        tool_context: ToolContext,
        actions: Any,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> list[str]:
        if not isinstance(actions, list):
            return []
        errors: list[str] = []
        for index, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                continue
            if str(action.get("tool", "")).strip() != "finish":
                continue
            summary = action.get("summary", "")
            if summary is not None and not isinstance(summary, str):
                continue
            allow, message = self._allow_finish(
                tool_context,
                step,
                step_limit,
                str(summary or ""),
                policy,
            )
            if not allow:
                errors.append(f"Action {index}: finish is not allowed now. {message}")
        return errors

    def _validate_repeated_actions(
        self,
        tool_context: ToolContext,
        actions: Any,
    ) -> list[str]:
        if not isinstance(actions, list) or not actions:
            return []
        current_summaries = [
            self._history_action_summary(action)
            for action in actions
            if isinstance(action, dict)
        ]
        if not current_summaries or len(current_summaries) != len(actions):
            return []
        recent_payloads = self._load_recent_step_payloads(tool_context, 3)
        if len(recent_payloads) < 3:
            return []

        for payload in recent_payloads:
            prior_actions = payload.get("actions")
            if not isinstance(prior_actions, list) or len(prior_actions) != len(
                actions
            ):
                return []
            prior_summaries = [
                self._history_action_summary(action)
                for action in prior_actions
                if isinstance(action, dict)
            ]
            if prior_summaries != current_summaries:
                return []
            prior_results = payload.get("results")
            if not isinstance(prior_results, list):
                return []
            if any(
                isinstance(result, dict) and result.get("error")
                for result in prior_results
            ):
                return []

        summarized = " | ".join(current_summaries)
        return [
            "Response repeats the same action plan from the last 3 steps without new evidence. "
            f"Choose a different branch or advance the workflow instead of repeating: {summarized[:400]}"
        ]

    def _validate_timeframe_mismatch_block(
        self,
        actions: Any,
    ) -> list[str]:
        if not isinstance(actions, list):
            return []
        if not self.config.research.timeframe_adjustment_repeat_block:
            return []
        status = self._get_timeframe_mismatch_status()
        if not status.get("repeat_blocked"):
            return []
        if not status.get("has_mismatch"):
            return []
        latest_requested = status.get("latest", {}).get("requested")
        if latest_requested is None:
            return []
        for action in actions:
            if not isinstance(action, dict):
                continue
            tool = str(action.get("tool", "")).strip()
            if tool == "run_cli":
                args = action.get("args")
                if isinstance(args, list):
                    args_str = " ".join(str(a).lower() for a in args)
                else:
                    args_str = str(action.get("command", "")).lower()
                if latest_requested.lower() in args_str:
                    return [
                        f"Timeframe mismatch repeat BLOCKED: the previous step requested {latest_requested} "
                        f"but the CLI auto-adjusted to {status.get('latest', {}).get('effective')}. "
                        f"Repeatedly requesting {latest_requested} with the same unchanged profile is not a valid experiment. "
                        f"Resolve the mismatch first: patch indicator timeframe(s) to match, reformulate as {status.get('latest', {}).get('effective')} test, or abandon the higher-timeframe hypothesis."
                    ]
            elif tool == "evaluate_candidate":
                pol = str(action.get("timeframe_policy") or "").strip().lower()
                if pol != "explicit":
                    continue
                tf = action.get("timeframe")
                if isinstance(tf, str) and tf.strip():
                    candidate = tf.strip().lower()
                    if latest_requested.lower() in candidate or candidate in latest_requested.lower():
                        return [
                            f"Timeframe mismatch repeat BLOCKED: the previous step requested {latest_requested} "
                            f"but the CLI auto-adjusted to {status.get('latest', {}).get('effective')}. "
                            f"Repeatedly requesting {latest_requested} with the same unchanged profile is not a valid experiment. "
                            f"Resolve the mismatch first: patch indicator timeframe(s) to match, reformulate as {status.get('latest', {}).get('effective')} test, or abandon the higher-timeframe hypothesis."
                        ]
        return []

    def _repair_invalid_response(
        self,
        tool_context: ToolContext,
        step: int,
        messages: list[ChatMessage],
        reasoning: str,
        actions: list[Any],
        errors: list[str],
    ) -> dict[str, Any] | None:
        action_summaries = []
        for action in actions:
            if isinstance(action, dict):
                action_summaries.append(self._history_action_summary(action))
            else:
                action_summaries.append(str(action))
        repair_payload = {
            "reasoning": reasoning,
            "actions": actions,
            "errors": errors,
        }
        opening_step = self._is_true_opening_step(tool_context, step)
        next_action_template = (
            None
            if opening_step
            else self._followup_next_action_template_prompt_state(tool_context)
        )
        if self._uses_local_transformers_provider() or opening_step:
            repair_messages = self._compact_repair_messages(
                {
                    "reasoning": reasoning,
                    "actions": actions,
                },
                errors=errors,
                opening_step=opening_step,
                next_action_template=next_action_template,
            )
        else:
            repair_messages = [
                *messages,
                ChatMessage(
                    role="assistant",
                    content=(
                        f"Reasoning: {reasoning or '(empty)'}\n"
                        "Planned actions:\n"
                        + "\n".join(f"- {summary}" for summary in action_summaries)
                    ),
                ),
                ChatMessage(
                    role="user",
                    content=(
                        f"{RESPONSE_REPAIR_PROMPT}\n\n"
                        "Problems:\n"
                        + "\n".join(f"- {error}" for error in errors)
                        + (
                            "\n\nnext_action_template:\n"
                            + json.dumps(next_action_template, ensure_ascii=True)
                            if isinstance(next_action_template, dict)
                            else ""
                        )
                    ),
                ),
            ]
        self._append_raw_explorer_payload(
            tool_context,
            step=step,
            phase="response_repair",
            event="repair_request",
            source="controller",
            label="response_repair",
            payload_text=json.dumps(repair_payload, ensure_ascii=True),
        )
        self._trace_runtime(
            tool_context,
            step=step,
            phase="response_repair",
            status="start",
            message="Repairing invalid controller response.",
            error_count=len(errors),
        )
        try:
            with self._provider_scope(
                tool_context=tool_context,
                step=step,
                label="response_repair",
                phase="response_repair",
                provider=self.provider,
            ):
                repaired = self.provider.complete_json(repair_messages)
            self._append_raw_explorer_payload(
                tool_context,
                step=step,
                phase="response_repair",
                event="repair_response",
                source="controller",
                label="response_repair",
                payload_json=repaired,
            )
            normalized = self._normalize_model_response(repaired)
            normalized = self._apply_runtime_interventions(
                tool_context,
                step,
                normalized,
                phase="response_repair",
            )
        except (ProviderError, RuntimeError, TypeError, ValueError) as exc:
            self._append_raw_explorer_payload(
                tool_context,
                step=step,
                phase="response_repair",
                event="repair_failed",
                source="controller",
                label="response_repair",
                payload_text="\n".join(errors),
                error=str(exc),
            )
            self._trace_runtime(
                tool_context,
                step=step,
                phase="response_repair",
                status="failed",
                message="Response repair failed.",
                error=exc,
            )
            return None
        repaired_actions = normalized.get("actions")
        repaired_errors = self._validate_actions(repaired_actions)
        pol = self._current_run_policy or RunPolicy()
        lim = self._current_step_limit or self.config.research.max_steps
        repaired_errors.extend(
            self._validate_finish_timing(
                tool_context, repaired_actions, step, lim, pol
            )
        )
        repaired_errors.extend(
            self._validate_repeated_actions(tool_context, repaired_actions)
        )
        repaired_errors.extend(
            self._validate_timeframe_mismatch_block(repaired_actions)
        )
        repaired_errors.extend(
            self._validate_branch_lifecycle_actions(
                tool_context, repaired_actions, step, lim, pol
            )
        )
        if repaired_errors:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="response_repair",
                status="rejected",
                message="Repaired response still failed validation.",
                error_count=len(repaired_errors),
            )
            return None
        self._trace_runtime(
            tool_context,
            step=step,
            phase="response_repair",
            status="ok",
            message="Response repair succeeded.",
            action_count=len(repaired_actions)
            if isinstance(repaired_actions, list)
            else None,
        )
        return normalized

    def _repair_invalid_payload_shape(
        self,
        tool_context: ToolContext,
        step: int,
        messages: list[ChatMessage],
        payload: Any,
        error: str,
    ) -> dict[str, Any] | None:
        payload_text = json.dumps(payload, ensure_ascii=False)
        opening_step = self._is_true_opening_step(tool_context, step)
        if self._uses_local_transformers_provider() or opening_step:
            repair_messages = self._compact_repair_messages(
                payload,
                errors=[],
                shape_error=error,
                opening_step=opening_step,
            )
        else:
            repair_messages = [
                *messages,
                ChatMessage(role="assistant", content=payload_text),
                ChatMessage(
                    role="user",
                    content=(
                        f"{RESPONSE_REPAIR_PROMPT}\n\n"
                        "The previous response was valid JSON but had the wrong top-level shape for the controller.\n"
                        f"Problem:\n- {error}\n\n"
                        "Use the same intent, but convert it into controller actions. "
                        "Do not return a raw scoring-profile JSON document as the top-level response."
                    ),
                ),
            ]
        self._append_raw_explorer_payload(
            tool_context,
            step=step,
            phase="payload_shape_repair",
            event="repair_request",
            source="controller",
            label="payload_shape_repair",
            payload_text=payload_text,
            error=error,
        )
        self._trace_runtime(
            tool_context,
            step=step,
            phase="payload_shape_repair",
            status="start",
            message="Repairing invalid top-level payload shape.",
            error=error,
        )
        try:
            with self._provider_scope(
                tool_context=tool_context,
                step=step,
                label="payload_shape_repair",
                phase="payload_shape_repair",
                provider=self.provider,
            ):
                repaired = self.provider.complete_json(repair_messages)
            self._append_raw_explorer_payload(
                tool_context,
                step=step,
                phase="payload_shape_repair",
                event="repair_response",
                source="controller",
                label="payload_shape_repair",
                payload_json=repaired,
            )
            normalized = self._normalize_model_response(repaired)
            normalized = self._apply_runtime_interventions(
                tool_context,
                step,
                normalized,
                phase="payload_shape_repair",
            )
        except (ProviderError, RuntimeError) as exc:
            self._append_raw_explorer_payload(
                tool_context,
                step=step,
                phase="payload_shape_repair",
                event="repair_failed",
                source="controller",
                label="payload_shape_repair",
                payload_text=payload_text,
                error=str(exc),
            )
            self._trace_runtime(
                tool_context,
                step=step,
                phase="payload_shape_repair",
                status="failed",
                message="Payload-shape repair failed.",
                error=exc,
            )
            return None
        repaired_actions = normalized.get("actions")
        repaired_errors = self._validate_actions(repaired_actions)
        pol = self._current_run_policy or RunPolicy()
        lim = self._current_step_limit or self.config.research.max_steps
        repaired_errors.extend(
            self._validate_finish_timing(
                tool_context, repaired_actions, step, lim, pol
            )
        )
        repaired_errors.extend(
            self._validate_repeated_actions(tool_context, repaired_actions)
        )
        repaired_errors.extend(
            self._validate_timeframe_mismatch_block(repaired_actions)
        )
        repaired_errors.extend(
            self._validate_branch_lifecycle_actions(
                tool_context, repaired_actions, step, lim, pol
            )
        )
        if repaired_errors:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="payload_shape_repair",
                status="rejected",
                message="Payload-shape repair still failed validation.",
                error_count=len(repaired_errors),
            )
            return None
        self._trace_runtime(
            tool_context,
            step=step,
            phase="payload_shape_repair",
            status="ok",
            message="Payload-shape repair succeeded.",
            action_count=len(repaired_actions)
            if isinstance(repaired_actions, list)
            else None,
        )
        return normalized

    def _extract_profile_ref(self, payload: dict[str, Any]) -> str | None:
        if "id" in payload and isinstance(payload["id"], str):
            return payload["id"]
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("id"), str):
            return data["id"]
        return None

    def _resolve_profile_ref_arg(self, value: str) -> str:
        if (
            value.startswith("<")
            and value.endswith(">")
            and self.last_created_profile_ref
        ):
            return self.last_created_profile_ref
        candidate = Path(value)
        if not candidate.exists() or not candidate.is_file():
            return value
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return value
        if not isinstance(payload, dict):
            return value
        resolved = self._extract_profile_ref(payload)
        return resolved or value

    def _substitute_runtime_placeholders(self, value: str) -> str:
        if not self.last_created_profile_ref:
            return value
        return value.replace("<created_profile_ref>", self.last_created_profile_ref)

    def _record_attempt_from_artifact(
        self,
        tool_context: ToolContext,
        artifact_dir: Path,
        *,
        profile_ref: str | None = None,
        note: str | None = None,
        requested_horizon_months: int | None = None,
    ) -> dict[str, Any]:
        artifact_dir = artifact_dir.resolve()
        if attempt_exists(tool_context.attempts_path, artifact_dir):
            attempts = load_attempts(tool_context.attempts_path)
            existing = next(
                attempt
                for attempt in attempts
                if str(attempt.get("artifact_dir", "")).lower()
                == str(artifact_dir).lower()
            )
            return {"status": "existing", "attempt": existing}

        compare_payload = self.cli.score_artifact(artifact_dir)
        sensitivity_snapshot_path = artifact_dir / "sensitivity-response.json"
        sensitivity_snapshot = (
            load_sensitivity_snapshot(artifact_dir)
            if sensitivity_snapshot_path.exists()
            else None
        )
        score = build_attempt_score(compare_payload, sensitivity_snapshot)
        ev = self._normalized_attempt_record_evidence(
            artifact_dir,
            sensitivity_snapshot if isinstance(sensitivity_snapshot, dict) else None,
            score,
            compare_payload if isinstance(compare_payload, dict) else None,
        )
        if requested_horizon_months is not None:
            ev["requested_horizon_months"] = requested_horizon_months
        record = make_attempt_record(
            self.config,
            tool_context.attempts_path,
            tool_context.run_id,
            artifact_dir,
            score,
            candidate_name=artifact_dir.name,
            profile_ref=profile_ref,
            profile_path=self.profile_sources.get(profile_ref) if profile_ref else None,
            sensitivity_snapshot_path=sensitivity_snapshot_path
            if sensitivity_snapshot_path.exists()
            else None,
            note=note,
            **ev,
        )
        append_attempt(tool_context.attempts_path, record)
        self._render_run_progress(tool_context)
        signal_count = None
        resolved_trades = None
        effective_window_months = None
        if isinstance(record.best_summary, dict):
            signal_count = record.best_summary.get("signal_count")
            best_cell = record.best_summary.get("best_cell")
            if isinstance(best_cell, dict):
                resolved_trades = best_cell.get("resolved_trades")
            market_window = record.best_summary.get("market_data_window")
            if isinstance(market_window, dict):
                effective_window_months = market_window.get("effective_window_months")
        auto_log_reason = None
        if record.composite_score is None:
            auto_log_reason = "quality_score was null in the evaluation artifacts"
        eff_from_ev = record.effective_window_months
        if eff_from_ev is None and effective_window_months is not None:
            eff_from_ev = effective_window_months
        derived_attempt = {
            "best_summary": record.best_summary if isinstance(record.best_summary, dict) else {},
            "effective_window_months": eff_from_ev,
        }
        resolved_trades, derived_trades_per_month, derived_positive_cell_ratio = (
            self._resolve_support_metrics(
                derived_attempt,
                resolved_trades=resolved_trades if resolved_trades is not None else record.resolved_trades,
                trades_per_month=record.trades_per_month,
                positive_ratio=record.positive_cell_ratio,
            )
        )
        return {
            "status": "logged",
            "attempt_id": record.attempt_id,
            "composite_score": record.composite_score,
            "primary_score": record.primary_score,
            "score_basis": record.score_basis,
            "metrics": record.metrics,
            "profile_ref": record.profile_ref,
            "signal_count": signal_count,
            "resolved_trades": resolved_trades
            if resolved_trades is not None
            else record.resolved_trades,
            "trades_per_month": derived_trades_per_month,
            "positive_cell_ratio": derived_positive_cell_ratio,
            "effective_window_months": eff_from_ev,
            "effective_window_source": record.effective_window_source,
            "requested_horizon_months": record.requested_horizon_months,
            "requested_timeframe": record.requested_timeframe,
            "effective_timeframe": record.effective_timeframe,
            "coverage_status": record.coverage_status,
            "validation_outcome": record.validation_outcome,
            "job_status": record.job_status,
            "reason": auto_log_reason,
            "artifact_dir": record.artifact_dir,
            "run_progress_plot": str(tool_context.progress_plot_path),
            "sensitivity_snapshot_loaded": sensitivity_snapshot is not None,
        }

    def _refresh_progress_artifacts(self, tool_context: ToolContext) -> None:
        self._render_run_progress(tool_context)

    def _maybe_auto_log_attempt(
        self,
        tool_context: ToolContext,
        args: list[str],
    ) -> dict[str, Any] | None:
        primary = str(args[0]).lower()
        if (
            primary not in {"sensitivity", "sensitivity-basket"}
            or "--output-dir" not in args
        ):
            return None
        output_index = args.index("--output-dir") + 1
        if output_index >= len(args):
            return None
        artifact_dir = Path(str(args[output_index]))
        profile_ref = None
        if "--profile-ref" in args:
            profile_index = args.index("--profile-ref") + 1
            if profile_index < len(args):
                profile_ref = str(args[profile_index])
        requested_horizon_months = self._parse_lookback_months_from_cli_args(args)
        return self._record_attempt_from_artifact(
            tool_context,
            artifact_dir,
            profile_ref=profile_ref,
            requested_horizon_months=requested_horizon_months,
        )

    def _execute_cli_invocation(
        self,
        tool_context: ToolContext,
        *,
        args: list[str],
        cwd: Path | None,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        source_action: dict[str, Any],
        result_tool: str = "run_cli",
    ) -> dict[str, Any]:
        guard_error = self._guard_cli_args(args)
        working_dir = cwd or self.config.fuzzfolio.workspace_root or Path.cwd()
        if guard_error:
            return {
                "tool": result_tool,
                "ok": False,
                "created_profile_ref": None,
                "source_profile_file": None,
                "result": {
                    "argv": [*self.cli.build_base_argv(), *args],
                    "cwd": str(working_dir.resolve()),
                    "returncode": 2,
                    "stdout": "",
                    "stderr": guard_error,
                },
                "auto_log": None,
                "warnings": [],
                "errors": [guard_error],
                "artifacts": {},
                "state_updates": {},
                "next_recommended_action": None,
                "status": "failed",
            }
        if "--profile-ref" in args:
            profile_index = args.index("--profile-ref") + 1
            if profile_index < len(args):
                args[profile_index] = self._resolve_profile_ref_arg(
                    str(args[profile_index])
                )
        result = self.cli.run(
            [str(item) for item in args],
            cwd=cwd,
            check=False,
        )

        serialized_result = json.loads(self._serialize_tool_result(result))

        profile_ref: str | None = None
        file_arg: Path | None = None
        if result.returncode == 0 and args[:2] in (
            ["profiles", "create"],
            ["profiles", "update"],
        ):
            payload = result.parsed_json if isinstance(result.parsed_json, dict) else {}
            profile_ref = self._extract_profile_ref(payload)
            if "--file" in args:
                file_index = args.index("--file") + 1
                if file_index < len(args):
                    file_arg = Path(str(args[file_index])).resolve()
            if profile_ref and file_arg:
                if args[:2] == ["profiles", "create"]:
                    self.last_created_profile_ref = profile_ref
                self.profile_sources[profile_ref] = file_arg

        auto_log = (
            self._maybe_auto_log_attempt(tool_context, args)
            if result.returncode == 0
            else None
        )
        timeframe_mismatch = self._resolve_timeframe_mismatch(
            serialized_result,
            auto_log=auto_log if isinstance(auto_log, dict) else None,
        )
        if auto_log is not None and auto_log.get("status") == "logged":
            artifact_dir_str = auto_log.get("artifact_dir", "")
            if artifact_dir_str:
                score = auto_log.get("composite_score")
                if score is not None:
                    retention_result = self._finalize_attempt_branch_state(
                        tool_context,
                        step,
                        step_limit,
                        policy,
                        auto_log=auto_log,
                        cli_args=args,
                        source_action=source_action,
                        timeframe_mismatch=timeframe_mismatch,
                    )
                    if retention_result.get("retention_failed"):
                        retention_result["auto_log"] = auto_log
                        base = {
                            "tool": result_tool,
                            "ok": result.returncode == 0,
                            "created_profile_ref": profile_ref,
                            "source_profile_file": str(file_arg) if file_arg else None,
                            "result": serialized_result,
                            "auto_log": auto_log,
                            "timeframe_mismatch": timeframe_mismatch,
                            "retention_gate": retention_result,
                            "warnings": [],
                            "errors": [],
                            "artifacts": {},
                            "state_updates": {},
                            "next_recommended_action": None,
                            "status": "failed",
                        }
                        return self._finalize_typed_cli_surface(base)

        stderr_msg = ""
        if isinstance(serialized_result, dict):
            stderr_val = serialized_result.get("stderr")
            if isinstance(stderr_val, str) and stderr_val.strip():
                stderr_msg = stderr_val.strip()
        warn_list: list[str] = []
        if isinstance(timeframe_mismatch, dict) and timeframe_mismatch:
            msg = timeframe_mismatch.get("message")
            warn_list.append(str(msg or "timeframe_mismatch"))
        err_list: list[str] = []
        if result.returncode != 0 and stderr_msg:
            err_list.append(stderr_msg[:1200])
        next_hint = None
        if args and str(args[0]).lower() in {"sensitivity", "sensitivity-basket"}:
            next_hint = "inspect_artifact" if result.returncode == 0 else None
        base = {
            "tool": result_tool,
            "ok": result.returncode == 0,
            "created_profile_ref": profile_ref,
            "source_profile_file": str(file_arg) if file_arg else None,
            "result": serialized_result,
            "auto_log": auto_log,
            "timeframe_mismatch": timeframe_mismatch,
            "warnings": warn_list,
            "errors": err_list,
            "artifacts": {},
            "state_updates": {},
            "next_recommended_action": next_hint,
            "status": "ok" if result.returncode == 0 else "failed",
        }
        return self._finalize_typed_cli_surface(base)

    def _finalize_typed_cli_surface(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload.setdefault("warnings", [])
        payload.setdefault("errors", [])
        payload.setdefault("artifacts", {})
        payload.setdefault("state_updates", {})
        if "status" not in payload:
            payload["status"] = "ok" if payload.get("ok") else "failed"
        return payload

    def _sanitize_label(self, raw: str, *, max_len: int = 72) -> str:
        text = re.sub(r"[^\w\-]+", "_", (raw or "item").strip()) or "item"
        return text[:max_len]

    def _default_profile_path_for_candidate(
        self,
        tool_context: ToolContext,
        candidate_name: str | None,
    ) -> Path:
        label = self._sanitize_label(str(candidate_name or "candidate"))
        return (tool_context.profiles_dir / f"{label}.json").resolve()

    def _resolve_local_profile_path(
        self,
        tool_context: ToolContext,
        *,
        candidate_name: Any = None,
        profile_path: Any = None,
        profile_ref: Any = None,
        require_exists: bool = True,
    ) -> Path | None:
        if isinstance(profile_path, str) and profile_path.strip():
            path = Path(
                self._substitute_runtime_placeholders(profile_path.strip())
            ).resolve()
            if not require_exists or path.exists():
                return path
        if isinstance(candidate_name, str) and candidate_name.strip():
            path = self._default_profile_path_for_candidate(
                tool_context,
                candidate_name.strip(),
            )
            if not require_exists or path.exists():
                return path
        if isinstance(profile_ref, str) and profile_ref.strip():
            mapped = self.profile_sources.get(
                self._substitute_runtime_placeholders(profile_ref.strip())
            )
            if isinstance(mapped, Path):
                path = mapped.resolve()
                if not require_exists or path.exists():
                    return path
        return None

    def _prompt_visible_action_signature(
        self,
        action: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(action, dict):
            return {}
        normalized = dict(action)
        candidate_name = normalized.get("candidate_name")
        if not isinstance(candidate_name, str) or not candidate_name.strip():
            candidate_name = _candidate_name_from_profile_path_text(
                normalized.get("profile_path")
            )
        if not isinstance(candidate_name, str) or not candidate_name.strip():
            candidate_name = _candidate_name_from_profile_path_text(
                normalized.get("destination_path")
            )
        if isinstance(candidate_name, str) and candidate_name.strip():
            normalized["candidate_name"] = self._sanitize_label(candidate_name)
        source_candidate_name = normalized.get("source_candidate_name")
        if not isinstance(source_candidate_name, str) or not source_candidate_name.strip():
            source_candidate_name = _candidate_name_from_profile_path_text(
                normalized.get("source_profile_path")
            )
        if isinstance(source_candidate_name, str) and source_candidate_name.strip():
            normalized["source_candidate_name"] = self._sanitize_label(
                source_candidate_name
            )
        destination_candidate_name = normalized.get("destination_candidate_name")
        if not isinstance(destination_candidate_name, str) or not destination_candidate_name.strip():
            destination_candidate_name = _candidate_name_from_profile_path_text(
                normalized.get("destination_path")
            )
        if (
            isinstance(destination_candidate_name, str)
            and destination_candidate_name.strip()
        ):
            normalized["destination_candidate_name"] = self._sanitize_label(
                destination_candidate_name
            )
        for field in (
            "profile_path",
            "destination_path",
            "source_profile_path",
            "metadata_out_path",
        ):
            normalized.pop(field, None)
        return normalized

    def _typed_prepare_profile(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        mode = str(action.get("mode") or "").strip()
        profiles_dir = tool_context.profiles_dir
        name = self._sanitize_label(str(action.get("candidate_name") or "candidate"))
        warnings: list[str] = []
        if mode == "from_template":
            dest_raw = action.get("destination_path")
            dest = (
                Path(str(dest_raw).strip()).resolve()
                if isinstance(dest_raw, str) and dest_raw.strip()
                else self._default_profile_path_for_candidate(tool_context, name)
            )
            tpl = tool_context.profile_template_path
            if not tpl.exists():
                return tt.normalized_tool_envelope(
                    "prepare_profile",
                    ok=False,
                    errors=[f"Template profile missing: {tpl}"],
                    profile_path=str(dest),
                    profile_name=name,
                    indicator_ids=action.get("indicator_ids"),
                    next_recommended_action="run_cli",
                )
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(tpl, dest)
            payload = _ensure_profile_name_matches_candidate(dest, name)
            candidate_summary = _candidate_summary_from_profile_payload(
                payload,
                profile_path=dest,
                draft_name=name,
            )
            return tt.normalized_tool_envelope(
                "prepare_profile",
                ok=True,
                profile_path=str(dest),
                profile_name=name,
                candidate_name=name,
                indicator_ids=action.get("indicator_ids"),
                timeframe_summary=str(action.get("timeframe") or ""),
                candidate_summary=candidate_summary,
                controller_hint="Profile scaffold is ready. Use candidate_summary directly and validate next instead of rereading the profile file.",
                warnings=warnings,
                next_recommended_action="validate_profile",
                artifacts={"profile_path": str(dest)},
            )
        if mode == "clone_local":
            src = self._resolve_local_profile_path(
                tool_context,
                candidate_name=action.get("source_candidate_name"),
                profile_path=action.get("source_profile_path"),
                profile_ref=action.get("source_profile_ref"),
                require_exists=True,
            )
            if src is None:
                return tt.normalized_tool_envelope(
                    "prepare_profile",
                    ok=False,
                    errors=[
                        "prepare_profile clone_local requires source_candidate_name or source_profile_ref."
                    ],
                    next_recommended_action=None,
                )
            dest_raw = action.get("destination_path")
            destination_candidate_name = action.get("destination_candidate_name")
            dest = (
                Path(str(dest_raw).strip()).resolve()
                if isinstance(dest_raw, str) and dest_raw.strip()
                else self._default_profile_path_for_candidate(
                    tool_context,
                    destination_candidate_name or name,
                )
            )
            args = [
                "profiles",
                "clone-local",
                "--file",
                str(src),
                "--out",
                str(dest),
                "--pretty",
            ]
            base = self._execute_cli_invocation(
                tool_context,
                args=args,
                cwd=None,
                step=step,
                step_limit=step_limit,
                policy=policy,
                source_action=action,
                result_tool="prepare_profile",
            )
            base["profile_path"] = str(dest)
            base["profile_name"] = name
            base["candidate_name"] = name
            base["indicator_ids"] = action.get("indicator_ids")
            payload = _ensure_profile_name_matches_candidate(dest, name)
            base["candidate_summary"] = _candidate_summary_from_profile_payload(
                payload,
                profile_path=dest,
                draft_name=name,
            )
            if base.get("candidate_summary"):
                base["controller_hint"] = "Local clone is ready. Validate this candidate next instead of inspecting the cloned file."
            if base.get("ok"):
                base["next_recommended_action"] = "validate_profile"
            else:
                base["next_recommended_action"] = "inspect the source draft/reference and retry"
            artifacts = dict(base.get("artifacts") or {})
            artifacts["profile_path"] = str(dest)
            base["artifacts"] = artifacts
            return self._finalize_typed_cli_surface(base)
        if mode == "scaffold_from_seed":
            ids = action.get("indicator_ids")
            if not isinstance(ids, list) or not all(
                isinstance(item, (str, int)) for item in ids
            ):
                return tt.normalized_tool_envelope(
                    "prepare_profile",
                    ok=False,
                    errors=["prepare_profile scaffold_from_seed requires indicator_ids array."],
                    next_recommended_action=None,
                )
            dest_raw = action.get("destination_path")
            dest = (
                Path(str(dest_raw).strip()).resolve()
                if isinstance(dest_raw, str) and dest_raw.strip()
                else self._default_profile_path_for_candidate(tool_context, name)
            )
            args = ["profiles", "scaffold"]
            for ind in ids:
                args.extend(["--indicator", str(ind)])
            instruments = action.get("instruments")
            if isinstance(instruments, list):
                for sym in instruments:
                    args.extend(["--instrument", str(sym)])
            args.extend(["--out", str(dest), "--pretty"])
            base = self._execute_cli_invocation(
                tool_context,
                args=args,
                cwd=None,
                step=step,
                step_limit=step_limit,
                policy=policy,
                source_action=action,
                result_tool="prepare_profile",
            )
            base["profile_path"] = str(dest)
            base["profile_name"] = name
            base["candidate_name"] = name
            base["indicator_ids"] = ids
            payload = _ensure_profile_name_matches_candidate(dest, name)
            base["candidate_summary"] = _candidate_summary_from_profile_payload(
                payload,
                profile_path=dest,
                draft_name=name,
            )
            if base.get("candidate_summary"):
                base["controller_hint"] = "Candidate scaffold is ready. Validate next; do not reread the profile file unless the tool reported an error."
            if base.get("ok"):
                base["next_recommended_action"] = "validate_profile"
            artifacts = dict(base.get("artifacts") or {})
            artifacts["profile_path"] = str(dest)
            base["artifacts"] = artifacts
            return self._finalize_typed_cli_surface(base)
        return tt.normalized_tool_envelope(
            "prepare_profile",
            ok=False,
            errors=[f"Unknown prepare_profile mode: {mode or '(empty)'}"],
            next_recommended_action=None,
        )

    def _typed_mutate_profile(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        profile_path = self._resolve_local_profile_path(
            tool_context,
            candidate_name=action.get("candidate_name"),
            profile_path=action.get("profile_path"),
            profile_ref=action.get("profile_ref"),
            require_exists=True,
        )
        if profile_path is None:
            return tt.normalized_tool_envelope(
                "mutate_profile",
                ok=False,
                errors=["mutate_profile requires candidate_name or profile_ref."],
                next_recommended_action=None,
            )
        mutations = action.get("mutations")
        if not isinstance(mutations, list) or not mutations:
            return tt.normalized_tool_envelope(
                "mutate_profile",
                ok=False,
                errors=["mutate_profile requires non-empty mutations array."],
                next_recommended_action=None,
            )
        applied: list[dict[str, Any]] = []
        args: list[str] = ["profiles", "patch", "--file", str(profile_path)]
        for mut in mutations:
            if not isinstance(mut, dict):
                continue
            path = mut.get("path")
            if not isinstance(path, str) or not path.strip():
                continue
            value = mut.get("value")
            if isinstance(value, str):
                rendered = value
            else:
                rendered = json.dumps(value, ensure_ascii=True)
            args.extend(["--set", f"{path.strip()}={rendered}"])
            applied.append({"path": path.strip(), "value": value})
        if len(args) <= 3:
            return tt.normalized_tool_envelope(
                "mutate_profile",
                ok=False,
                errors=["No valid mutations were provided."],
                next_recommended_action=None,
            )
        out_raw = action.get("destination_path")
        destination_candidate_name = action.get("destination_candidate_name")
        out_path = (
            Path(str(out_raw).strip()).resolve()
            if isinstance(out_raw, str) and out_raw.strip()
            else (
                self._default_profile_path_for_candidate(
                    tool_context,
                    destination_candidate_name,
                )
                if isinstance(destination_candidate_name, str)
                and destination_candidate_name.strip()
                else profile_path
            )
        )
        args.extend(["--out", str(out_path), "--pretty"])
        base = self._execute_cli_invocation(
            tool_context,
            args=args,
            cwd=None,
            step=step,
            step_limit=step_limit,
            policy=policy,
            source_action=action,
            result_tool="mutate_profile",
        )
        base["profile_path"] = str(out_path)
        base["candidate_name"] = str(out_path.stem)
        base["applied_mutations"] = applied
        base["mutation_summary"] = f"{len(applied)} patch operation(s)"
        payload = _ensure_profile_name_matches_candidate(out_path, str(out_path.stem))
        base["candidate_summary"] = _candidate_summary_from_profile_payload(
            payload,
            profile_path=out_path if out_path.exists() else None,
            draft_name=str(out_path.stem),
        )
        if base.get("ok"):
            base["next_recommended_action"] = "validate_profile"
        arts = dict(base.get("artifacts") or {})
        arts["profile_path"] = str(out_path)
        base["artifacts"] = arts
        return self._finalize_typed_cli_surface(base)

    def _typed_validate_profile(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        path = self._resolve_local_profile_path(
            tool_context,
            candidate_name=action.get("candidate_name"),
            profile_path=action.get("profile_path"),
            profile_ref=action.get("profile_ref"),
            require_exists=True,
        )
        if path is None:
            return tt.normalized_tool_envelope(
                "validate_profile",
                ok=False,
                errors=["validate_profile requires candidate_name or profile_ref."],
                next_recommended_action=None,
            )
        args = ["profiles", "validate", "--file", str(path), "--pretty"]
        base = self._execute_cli_invocation(
            tool_context,
            args=args,
            cwd=None,
            step=step,
            step_limit=step_limit,
            policy=policy,
            source_action=action,
            result_tool="validate_profile",
        )
        base["profile_path"] = str(path)
        base["candidate_name"] = str(path.stem)
        source_payload = _read_json_if_exists(path)
        parsed = {}
        res = base.get("result")
        if isinstance(res, dict):
            pj = res.get("parsed_json")
            if isinstance(pj, dict):
                parsed = pj
        parsed_data = parsed.get("data") if isinstance(parsed.get("data"), dict) else parsed
        normalized_profile = (
            parsed_data.get("normalized_profile")
            if isinstance(parsed_data, dict)
            and isinstance(parsed_data.get("normalized_profile"), dict)
            else None
        )
        candidate_summary = _candidate_summary_from_profile_payload(
            {"profile": normalized_profile}
            if isinstance(normalized_profile, dict)
            else source_payload,
            profile_path=path if path.exists() else None,
            draft_name=str(path.stem),
        )
        base["validation_ok"] = bool(base.get("ok"))
        base["candidate_summary"] = candidate_summary
        base["normalized_timeframe_summary"] = str(
            (candidate_summary or {}).get("timeframe_summary")
            or parsed_data.get("timeframe_summary")
            or parsed_data.get("timeframes")
            or parsed.get("timeframe_summary")
            or parsed.get("timeframes")
            or ""
        )
        base["normalized_instrument_summary"] = str(
            (candidate_summary or {}).get("instrument_summary")
            or parsed_data.get("instrument_summary")
            or parsed_data.get("instruments")
            or parsed.get("instrument_summary")
            or parsed.get("instruments")
            or ""
        )
        base["material_changes"] = _normalized_profile_material_changes(
            source_payload,
            normalized_profile,
        )
        base["ready_for_registration"] = bool(base.get("ok"))
        if base.get("ok"):
            base["controller_hint"] = (
                "Validation passed. Register next using this candidate summary; only revisit the file if you need to debug a warning."
            )
        if base.get("ok"):
            base["next_recommended_action"] = "register_profile"
        return self._finalize_typed_cli_surface(base)

    def _typed_register_profile(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        operation = str(action.get("operation") or "create").strip().lower()
        path = self._resolve_local_profile_path(
            tool_context,
            candidate_name=action.get("candidate_name"),
            profile_path=action.get("profile_path"),
            profile_ref=action.get("profile_ref") if operation == "update" else None,
            require_exists=True,
        )
        if path is None:
            return tt.normalized_tool_envelope(
                "register_profile",
                ok=False,
                errors=["register_profile requires candidate_name or profile_ref."],
                next_recommended_action=None,
            )
        out_raw = action.get("metadata_out_path")
        if isinstance(out_raw, str) and out_raw.strip():
            out_path = Path(out_raw.strip()).resolve()
        else:
            out_path = path.parent / f"{path.stem}.created.json"
        if operation == "update":
            ref = action.get("profile_ref")
            if not isinstance(ref, str) or not ref.strip():
                return tt.normalized_tool_envelope(
                    "register_profile",
                    ok=False,
                    errors=["register_profile update requires profile_ref."],
                    next_recommended_action=None,
                )
            ref = self._substitute_runtime_placeholders(ref.strip())
            args = [
                "profiles",
                "update",
                "--profile-ref",
                ref,
                "--file",
                str(path),
                "--out",
                str(out_path),
                "--pretty",
            ]
        else:
            args = [
                "profiles",
                "create",
                "--file",
                str(path),
                "--out",
                str(out_path),
                "--pretty",
            ]
        base = self._execute_cli_invocation(
            tool_context,
            args=args,
            cwd=None,
            step=step,
            step_limit=step_limit,
            policy=policy,
            source_action=action,
            result_tool="register_profile",
        )
        profile_ref = base.get("created_profile_ref")
        payload = base.get("result")
        if isinstance(payload, dict):
            pj = payload.get("parsed_json")
            if profile_ref is None and isinstance(pj, dict):
                profile_ref = self._extract_profile_ref(pj)
        base["profile_ref"] = profile_ref
        base["profile_path"] = str(path)
        base["candidate_name"] = str(path.stem)
        base["created"] = operation != "update"
        base["updated"] = operation == "update"
        base["candidate_summary"] = _candidate_summary_from_profile_payload(
            _read_json_if_exists(path),
            profile_path=path if path.exists() else None,
            draft_name=str(path.stem),
            profile_ref=str(profile_ref).strip() if isinstance(profile_ref, str) else None,
        )
        base["ready_to_evaluate"] = bool(base.get("ok") and profile_ref)
        if base.get("ok"):
            base["controller_hint"] = (
                "Registration produced a canonical profile_ref. Use that ref directly in evaluate_candidate; do not reread the profile file unless debugging."
            )
        if base.get("ok"):
            base["next_recommended_action"] = "evaluate_candidate"
        artifacts = dict(base.get("artifacts") or {})
        artifacts["metadata_out_path"] = str(out_path)
        base["artifacts"] = artifacts
        return self._finalize_typed_cli_surface(base)

    def _transactional_prepare_profile_ref_for_eval(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        """Resolve profile_ref: existing ref, run-mapped path, content-hash dedupe, or validate+register."""
        warnings: list[str] = []
        meta: dict[str, Any] = {
            "profile_content_fingerprint": None,
            "reused_profile_ref_via_hash": False,
            "reused_profile_ref_via_run_map": False,
            "skipped_validate_cached": False,
        }
        raw_ref = action.get("profile_ref")
        raw_path = action.get("profile_path")
        raw_name = action.get("candidate_name")
        if isinstance(raw_ref, str) and raw_ref.strip():
            ref = self._substitute_runtime_placeholders(raw_ref.strip())
            return {
                "ok": True,
                "ref": ref,
                "warnings": warnings,
                "errors": [],
                "meta": meta,
            }
        path = self._resolve_local_profile_path(
            tool_context,
            candidate_name=raw_name,
            profile_path=raw_path,
            profile_ref=None,
            require_exists=True,
        )
        if path is None:
            return {
                "ok": False,
                "ref": None,
                "warnings": [],
                "errors": [
                    "candidate_name or profile_ref required when the profile is not yet registered."
                ],
                "meta": meta,
            }
        mapped = self._profile_ref_for_local_file(path)
        if mapped:
            meta["reused_profile_ref_via_run_map"] = True
            warnings.append("Using profile_ref already mapped to this path in the current run.")
            return {
                "ok": True,
                "ref": mapped,
                "warnings": warnings,
                "errors": [],
                "meta": meta,
            }
        fp, ferr = pi.compute_profile_fingerprint(path)
        if not fp:
            return {
                "ok": False,
                "ref": None,
                "warnings": [],
                "errors": [ferr or "could not fingerprint profile JSON"],
                "meta": meta,
            }
        meta["profile_content_fingerprint"] = fp
        cached_ref = self._profile_fingerprint_to_ref.get(fp)
        if cached_ref:
            meta["reused_profile_ref_via_hash"] = True
            warnings.append(
                "Reused profile_ref from content-hash cache (same normalized JSON as a prior registration)."
            )
            return {
                "ok": True,
                "ref": cached_ref,
                "warnings": warnings,
                "errors": [],
                "meta": meta,
            }
        try:
            mtime = path.stat().st_mtime
        except OSError as exc:
            return {
                "ok": False,
                "ref": None,
                "warnings": [],
                "errors": [str(exc)],
                "meta": meta,
            }
        need_validate = True
        pv = self._profile_path_validate_cache.get(str(path))
        if (
            pv is not None
            and pv == (fp, mtime)
            and self._profile_fingerprint_validate_ok.get(fp)
        ):
            need_validate = False
            meta["skipped_validate_cached"] = True
        if need_validate:
            v = self._typed_validate_profile(
                tool_context,
                {"profile_path": str(path)},
                step=step,
                step_limit=step_limit,
                policy=policy,
            )
            if not v.get("ok"):
                return {
                    "ok": False,
                    "ref": None,
                    "warnings": warnings,
                    "errors": list(v.get("errors") or ["validate_profile failed"]),
                    "meta": meta,
                }
            self._profile_path_validate_cache[str(path)] = (fp, mtime)
            self._profile_fingerprint_validate_ok[fp] = True
        reg = self._typed_register_profile(
            tool_context,
            {"profile_path": str(path), "operation": "create"},
            step=step,
            step_limit=step_limit,
            policy=policy,
        )
        if not reg.get("ok"):
            return {
                "ok": False,
                "ref": None,
                "warnings": warnings,
                "errors": list(reg.get("errors") or ["register_profile failed"]),
                "meta": meta,
            }
        ref = reg.get("profile_ref") or reg.get("created_profile_ref")
        if not isinstance(ref, str) or not ref.strip():
            return {
                "ok": False,
                "ref": None,
                "warnings": warnings,
                "errors": ["register_profile did not yield profile_ref"],
                "meta": meta,
            }
        ref = ref.strip()
        self._profile_fingerprint_to_ref[fp] = ref
        return {
            "ok": True,
            "ref": ref,
            "warnings": warnings,
            "errors": [],
            "meta": meta,
        }

    def _typed_evaluate_candidate(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        instruments = action.get("instruments")
        if not isinstance(instruments, list) or not instruments:
            return tt.normalized_tool_envelope(
                "evaluate_candidate",
                ok=False,
                errors=["evaluate_candidate requires instruments array."],
                next_recommended_action=None,
            )
        prep = self._transactional_prepare_profile_ref_for_eval(
            tool_context,
            action,
            step=step,
            step_limit=step_limit,
            policy=policy,
        )
        if not prep["ok"]:
            return tt.normalized_tool_envelope(
                "evaluate_candidate",
                ok=False,
                errors=list(prep["errors"]),
                warnings=list(prep.get("warnings") or []),
                next_recommended_action="validate_profile",
            )
        ref = str(prep["ref"])
        txn_warnings = list(prep.get("warnings") or [])
        profile_txn = prep.get("meta") or {}
        label = self._sanitize_label(
            str(
                action.get("candidate_name")
                or action.get("output_name")
                or "candidate"
            )
        )
        out_dir = (
            tool_context.evals_dir / f"eval_{label}_{self._timestamp()}"
        ).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        args: list[str] = [
            "sensitivity-basket",
            "--profile-ref",
            ref,
            "--output-dir",
            str(out_dir),
        ]
        for ins in instruments:
            args.extend(["--instrument", str(ins)])
        pol = str(action.get("timeframe_policy") or "profile_default").strip().lower()
        if pol == "explicit":
            tf = action.get("timeframe")
            if isinstance(tf, str) and tf.strip():
                args.extend(["--timeframe", tf.strip()])
        rh = action.get("requested_horizon_months")
        if rh is not None:
            try:
                args.extend(["--lookback-months", str(int(rh))])
            except (TypeError, ValueError):
                pass
        args.append("--pretty")
        args = self._apply_horizon_policy_to_cli_args(
            args,
            step=step,
            step_limit=step_limit,
            policy=policy,
        )
        base = self._execute_cli_invocation(
            tool_context,
            args=args,
            cwd=None,
            step=step,
            step_limit=step_limit,
            policy=policy,
            source_action=action,
            result_tool="evaluate_candidate",
        )
        auto = base.get("auto_log")
        mode = str(action.get("evaluation_mode") or "screen").strip().lower()
        score_basis = None
        score_val = None
        attempted = False
        tpm = None
        pos_ratio = None
        trades = None
        eff_months = None
        eff_src = None
        req_tf = None
        eff_tf = None
        if isinstance(auto, dict):
            attempted = str(auto.get("status") or "") == "logged"
            score_basis = auto.get("score_basis")
            score_val = auto.get("composite_score")
            tpm = auto.get("trades_per_month")
            pos_ratio = auto.get("positive_cell_ratio")
            trades = auto.get("resolved_trades")
            eff_months = auto.get("effective_window_months")
            eff_src = auto.get("effective_window_source")
        cli_res = base.get("result")
        if isinstance(cli_res, dict) and isinstance(cli_res.get("parsed_json"), dict):
            pj = cli_res["parsed_json"]
            req_tf = pj.get("requested_timeframe")
            eff_tf = pj.get("effective_timeframe")
        base["attempt_logged"] = attempted
        base["attempt_id"] = auto.get("attempt_id") if isinstance(auto, dict) else None
        base["artifact_dir"] = str(out_dir)
        base["profile_ref"] = ref
        base["requested_timeframe"] = req_tf
        base["effective_timeframe"] = eff_tf
        base["requested_horizon_months"] = self._parse_lookback_months_from_cli_args(
            args
        )
        base["effective_window_months"] = eff_months
        base["effective_window_source"] = eff_src
        base["score"] = score_val
        base["score_basis"] = score_basis
        base["resolved_trades"] = trades
        base["trades_per_month"] = tpm
        base["positive_cell_ratio"] = pos_ratio
        base["retention_relevant_flags"] = {
            "evaluation_mode": mode,
            "timeframe_policy": pol,
        }
        base["timeframe_auto_adjusted"] = bool(base.get("timeframe_mismatch"))
        if mode == "validate":
            base["next_recommended_action"] = (
                "compare_artifacts" if base.get("ok") else "inspect_artifact"
            )
        elif base.get("ok"):
            base["next_recommended_action"] = "inspect_artifact"
        arts = dict(base.get("artifacts") or {})
        arts["artifact_dir"] = str(out_dir)
        base["artifacts"] = arts
        su = dict(base.get("state_updates") or {})
        su["evaluate_profile_transaction"] = profile_txn
        base["state_updates"] = su
        tw = list(base.get("warnings") or [])
        tw.extend(txn_warnings)
        base["warnings"] = tw
        if isinstance(auto, dict) and str(auto.get("status") or "") == "logged":
            base["requested_timeframe"] = base.get("requested_timeframe") or auto.get(
                "requested_timeframe"
            )
            base["effective_timeframe"] = base.get("effective_timeframe") or auto.get(
                "effective_timeframe"
            )
            if base.get("effective_window_months") is None:
                base["effective_window_months"] = auto.get("effective_window_months")
            if base.get("requested_horizon_months") is None:
                base["requested_horizon_months"] = auto.get("requested_horizon_months")
            fid = self._family_id_for_profile_ref(ref)
            if fid:
                br = self._family_branches.get(fid)
                if br and br.last_validation_evidence:
                    base["validation_evidence"] = br.last_validation_evidence
                    base["branch_lifecycle_after_eval"] = {
                        "family_id": fid,
                        "lifecycle_state": br.lifecycle_state,
                        "promotion_level": br.promotion_level,
                        "retention_status": br.retention_status,
                        "latest_horizon_months": br.latest_horizon_months,
                        "latest_effective_window_months": br.latest_effective_window_months,
                        "promotability_status": br.promotability_status,
                        "validation_confidence": br.validation_confidence,
                    }
        return self._finalize_typed_cli_surface(base)

    def _typed_run_parameter_sweep(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        raw_ref = action.get("profile_ref")
        if not isinstance(raw_ref, str) or not raw_ref.strip():
            return tt.normalized_tool_envelope(
                "run_parameter_sweep",
                ok=False,
                errors=["run_parameter_sweep requires profile_ref."],
                next_recommended_action=None,
            )
        ref = self._substitute_runtime_placeholders(raw_ref.strip())
        axes = action.get("axes")
        if not isinstance(axes, list) or not axes:
            return tt.normalized_tool_envelope(
                "run_parameter_sweep",
                ok=False,
                errors=["run_parameter_sweep requires axes array of sweep axis strings."],
                next_recommended_action=None,
            )
        prefix = self._sanitize_label(
            str(action.get("candidate_name_prefix") or "sweep")
        )
        out_raw = action.get("output_dir")
        if isinstance(out_raw, str) and out_raw.strip():
            out_dir = Path(out_raw.strip()).resolve()
        else:
            out_dir = (
                tool_context.evals_dir / f"sweep_{prefix}_{self._timestamp()}"
            ).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        args: list[str] = [
            "sweep",
            "run",
            "--profile-ref",
            ref,
            "--output-dir",
            str(out_dir),
        ]
        instruments = action.get("instruments")
        if isinstance(instruments, list):
            for ins in instruments:
                args.extend(["--instrument", str(ins)])
        for ax in axes:
            args.extend(["--axis", str(ax)])
        args.append("--pretty")
        base = self._execute_cli_invocation(
            tool_context,
            args=args,
            cwd=None,
            step=step,
            step_limit=step_limit,
            policy=policy,
            source_action=action,
            result_tool="run_parameter_sweep",
        )
        ranked = None
        best = None
        sweep_id = None
        completed = bool(base.get("ok"))
        cli_res = base.get("result")
        if isinstance(cli_res, dict) and isinstance(cli_res.get("parsed_json"), dict):
            pj = cli_res["parsed_json"]
            ranked = pj.get("ranked") or pj.get("results")
            best = pj.get("best")
            sweep_id = pj.get("sweep_id") or pj.get("id")
        base["sweep_id"] = sweep_id
        base["completed"] = completed
        base["ranked_results"] = ranked
        base["best_variant"] = best
        if isinstance(best, dict):
            base["best_score"] = best.get("quality_score")
        base["artifact_dir"] = str(out_dir)
        base["artifact_kind"] = "parameter_sweep"
        base["inspect_ref"] = out_dir.name
        base["quality_score_preset"] = self.config.research.quality_score_preset
        base["controller_hint"] = (
            "Inspect this sweep using artifact_dir or inspect_ref. Sweep outputs are not attempt_ids."
        )
        if base.get("ok"):
            base["next_recommended_action"] = "inspect_artifact"
        arts = dict(base.get("artifacts") or {})
        arts["artifact_dir"] = str(out_dir)
        base["artifacts"] = arts
        return self._finalize_typed_cli_surface(base)

    def _resolve_artifact_path(
        self, tool_context: ToolContext, action: dict[str, Any]
    ) -> Path | None:
        ad = action.get("artifact_dir")
        if isinstance(ad, str) and ad.strip():
            return Path(ad.strip()).resolve()
        aid = action.get("attempt_id")
        if isinstance(aid, str) and aid.strip():
            needle = aid.strip()
            for att in load_run_attempts(tool_context.run_dir):
                if str(att.get("attempt_id") or "") != needle:
                    continue
                ar = att.get("artifact_dir")
                if isinstance(ar, str) and ar.strip():
                    return Path(ar.strip()).resolve()
        return None

    def _attempt_row_for_id(
        self, tool_context: ToolContext, attempt_id: str
    ) -> dict[str, Any] | None:
        needle = attempt_id.strip()
        for att in load_run_attempts(tool_context.run_dir):
            if str(att.get("attempt_id") or "") == needle:
                return att if isinstance(att, dict) else None
        return None

    def _typed_inspect_artifact(
        self, tool_context: ToolContext, action: dict[str, Any]
    ) -> dict[str, Any]:
        aid_raw = action.get("attempt_id")
        aid = aid_raw.strip() if isinstance(aid_raw, str) else None
        ledger_dir: str | None = None
        attempt_row: dict[str, Any] | None = None
        if aid:
            attempt_row = self._attempt_row_for_id(tool_context, aid)
            if attempt_row and isinstance(attempt_row.get("artifact_dir"), str):
                ledger_dir = str(attempt_row["artifact_dir"]).strip() or None
        path = self._resolve_artifact_path(tool_context, action)
        if path is None or not path.exists():
            return tt.normalized_tool_envelope(
                "inspect_artifact",
                ok=False,
                errors=["inspect_artifact needs artifact_dir or a known attempt_id."],
                next_recommended_action="evaluate_candidate",
            )
        explicit_ad = action.get("artifact_dir")
        if (
            isinstance(explicit_ad, str)
            and explicit_ad.strip()
            and ledger_dir
            and path.resolve() != Path(ledger_dir).resolve()
        ):
            return tt.normalized_tool_envelope(
                "inspect_artifact",
                ok=False,
                errors=[
                    "artifact_dir does not match the path recorded for this attempt_id in the ledger."
                ],
                next_recommended_action=None,
                artifact_resolution={
                    "resolution": ar.RESOLUTION_ATTEMPT_MISMATCH,
                    "artifact_dir": str(path),
                    "attempt_id": aid,
                    "ledger_artifact_dir": ledger_dir,
                },
            )
        view = str(action.get("view") or "summary").strip().lower()
        snapshot = load_sensitivity_snapshot(path)
        files = sorted(str(p) for p in path.iterdir()) if path.is_dir() else []
        snap_keys: list[str] = []
        if isinstance(snapshot, dict):
            snap_keys = [str(k) for k in list(snapshot.keys())[:40]]
        scorer = self.cli.score_artifact if view == "summary" else None
        resolution = ar.artifact_resolution_status(
            path,
            expected_attempt_id=aid,
            ledger_artifact_dir=ledger_dir,
            score_artifact=scorer,
        )
        payload: dict[str, Any] = {
            "tool": "inspect_artifact",
            "ok": True,
            "status": "ok",
            "artifact_dir": str(path),
            "view": view,
            "warnings": [],
            "errors": [],
            "artifacts": {"artifact_dir": str(path)},
            "state_updates": {},
            "next_recommended_action": "compare_artifacts",
            "files": files[:80],
            "sensitivity_snapshot_keys": snap_keys,
            "artifact_resolution": resolution,
            "attempt_ledger_hint": (
                {
                    "attempt_id": attempt_row.get("attempt_id"),
                    "composite_score": attempt_row.get("composite_score"),
                    "requested_horizon_months": attempt_row.get(
                        "requested_horizon_months"
                    ),
                    "effective_window_months": attempt_row.get(
                        "effective_window_months"
                    ),
                    "requested_timeframe": attempt_row.get("requested_timeframe"),
                    "effective_timeframe": attempt_row.get("effective_timeframe"),
                    "validation_outcome": attempt_row.get("validation_outcome"),
                    "coverage_status": attempt_row.get("coverage_status"),
                    "job_status": attempt_row.get("job_status"),
                }
                if isinstance(attempt_row, dict)
                else None
            ),
        }
        if view == "files":
            return payload
        if view == "curve_meta":
            detail = path / "best-cell-path-detail.json"
            if detail.exists():
                try:
                    payload["curve_meta"] = json.loads(
                        detail.read_text(encoding="utf-8")
                    )
                except (OSError, json.JSONDecodeError):
                    payload["curve_meta"] = None
            else:
                payload["curve_meta"] = None
            return payload
        if view == "request_meta":
            resp = path / "sensitivity-response.json"
            if resp.exists():
                try:
                    payload["request_meta"] = json.loads(
                        resp.read_text(encoding="utf-8")
                    )
                except (OSError, json.JSONDecodeError):
                    payload["request_meta"] = None
            return payload
        sweep_results_path = path / "sweep-results.json"
        if view == "summary" and sweep_results_path.exists():
            sweep_payload = _read_json_if_exists(sweep_results_path)
            sweep_summary = _summarize_sweep_results_payload(
                sweep_payload,
                artifact_dir=path,
            )
            if sweep_summary is not None:
                payload["artifact_kind"] = "parameter_sweep"
                payload["sweep_summary"] = sweep_summary
                payload["artifact_resolution"] = {
                    "artifact_dir": str(path),
                    "resolution": "parameter_sweep_results",
                    "reason": "sweep-results.json present",
                    "has_sweep_results": True,
                }
                payload["next_recommended_action"] = "compare_artifacts"
                payload["controller_hint"] = (
                    "Sweep summary already identifies the top permutation. Compare or evaluate from this summary before opening raw sweep JSON."
                )
                return payload
        try:
            compare = self.cli.score_artifact(path)
        except (CliError, OSError, RuntimeError, TypeError, ValueError) as exc:
            payload["ok"] = False
            payload["status"] = "failed"
            payload["errors"] = [str(exc)[:800]]
            payload["next_recommended_action"] = None
            return payload
        payload["artifact_kind"] = "sensitivity_eval"
        payload["compare_summary"] = compare
        best = compare.get("best")
        mw_hint = None
        if isinstance(best, dict):
            mdw = best.get("market_data_window")
            if isinstance(mdw, dict):
                mw_hint = mdw.get("effective_window_months")
        payload["effective_window_months_hint"] = mw_hint
        payload["controller_hint"] = (
            "Use compare_summary and attempt_ledger_hint before reading raw artifact files."
        )
        return payload

    def _typed_compare_artifacts(
        self, tool_context: ToolContext, action: dict[str, Any]
    ) -> dict[str, Any]:
        entries = action.get("attempt_ids") or action.get("artifact_dirs")
        if not isinstance(entries, list) or not entries:
            return tt.normalized_tool_envelope(
                "compare_artifacts",
                ok=False,
                errors=["compare_artifacts requires attempt_ids or artifact_dirs array."],
                next_recommended_action=None,
            )
        resolved: list[tuple[str, Path]] = []
        for item in entries:
            if not isinstance(item, str) or not item.strip():
                continue
            token = item.strip()
            p = Path(token)
            if p.exists() and p.is_dir():
                resolved.append((token, p.resolve()))
                continue
            r = self._resolve_artifact_path(
                tool_context, {"attempt_id": token, "artifact_dir": ""}
            )
            if r and r.exists():
                resolved.append((token, r))
        if not resolved:
            return tt.normalized_tool_envelope(
                "compare_artifacts",
                ok=False,
                errors=["Could not resolve any artifact directories from inputs."],
                next_recommended_action=None,
            )
        scored: list[dict[str, Any]] = []
        errors: list[str] = []
        for label, p in resolved:
            try:
                payload = self.cli.score_artifact(p)
                score = None
                best = payload.get("best")
                if isinstance(best, dict):
                    score = best.get("quality_score")
                scored.append(
                    {
                        "label": label,
                        "artifact_dir": str(p),
                        "quality_score": score,
                        "best": best,
                    }
                )
            except (CliError, OSError, RuntimeError, TypeError, ValueError) as exc:
                errors.append(f"{label}: {exc}")
        if not scored:
            return tt.normalized_tool_envelope(
                "compare_artifacts",
                ok=False,
                errors=errors or ["compare failed"],
                next_recommended_action=None,
            )
        lower_better = bool(self.config.research.plot_lower_is_better)

        def _rank_key(row: dict[str, Any]) -> float:
            v = row.get("quality_score")
            try:
                f = float(v)
            except (TypeError, ValueError):
                return float("inf") if lower_better else float("-inf")
            return f

        ranked = sorted(
            scored,
            key=_rank_key,
            reverse=not lower_better,
        )
        leader = ranked[0]
        trailer = ranked[-1] if len(ranked) > 1 else None
        deltas: list[str] = []
        if trailer and leader.get("quality_score") is not None:
            try:
                deltas.append(
                    f"score_delta={float(leader['quality_score']) - float(trailer.get('quality_score')):.4f}"
                )
            except (TypeError, ValueError):
                pass
        retention_note = "Compare effective_window_months and trades_per_month in each best cell before trusting longer horizons."
        return tt.normalized_tool_envelope(
            "compare_artifacts",
            ok=True,
            ranked_comparison=ranked,
            dominant_deltas=deltas,
            retention_notes=[retention_note],
            suggested_next_move="evaluate_candidate on leader unless retention already satisfied",
            errors=errors,
            artifacts={"compared": [r["artifact_dir"] for r in ranked]},
            next_recommended_action="evaluate_candidate",
        )

    def _execute_action(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
        *,
        step: int,
        step_limit: int,
        policy: RunPolicy,
    ) -> dict[str, Any]:
        tool = action.get("tool")
        self._bump_tool_usage(str(tool or ""))
        if tool == "run_cli":
            args = [
                self._substitute_runtime_placeholders(str(item))
                for item in self._normalize_cli_args(action)
            ]
            args = self._apply_horizon_policy_to_cli_args(
                args,
                step=step,
                step_limit=step_limit,
                policy=policy,
            )
            return self._execute_cli_invocation(
                tool_context,
                args=args,
                cwd=Path(action["cwd"]) if action.get("cwd") else None,
                step=step,
                step_limit=step_limit,
                policy=policy,
                source_action=action,
                result_tool="run_cli",
            )

        if tool == "prepare_profile":
            return self._typed_prepare_profile(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "mutate_profile":
            return self._typed_mutate_profile(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "validate_profile":
            return self._typed_validate_profile(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "register_profile":
            return self._typed_register_profile(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "evaluate_candidate":
            return self._typed_evaluate_candidate(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "run_parameter_sweep":
            return self._typed_run_parameter_sweep(
                tool_context, action, step=step, step_limit=step_limit, policy=policy
            )
        if tool == "inspect_artifact":
            return self._typed_inspect_artifact(tool_context, action)
        if tool == "compare_artifacts":
            return self._typed_compare_artifacts(tool_context, action)

        if tool == "write_file":
            path = Path(str(action.get("path", ""))).resolve()
            content = action.get("content")
            if not isinstance(content, str):
                raise ValueError("write_file requires string content.")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            return {
                "tool": "write_file",
                "path": str(path),
                "bytes": len(content.encode("utf-8")),
            }

        if tool == "read_file":
            path = Path(str(action.get("path", ""))).resolve()
            if not path.exists():
                raise FileNotFoundError(
                    f"read_file failed: path does not exist: {path}"
                )
            if path.is_dir():
                raise IsADirectoryError(
                    f"read_file failed: path is a directory, not a file: {path}. Use list_dir instead."
                )
            max_chars = int(action.get("max_chars", 6000))
            content = path.read_text(encoding="utf-8")
            return {
                "tool": "read_file",
                "path": str(path),
                "content": content[:max_chars],
            }

        if tool == "list_dir":
            path = Path(str(action.get("path", ""))).resolve()
            if not path.exists():
                raise FileNotFoundError(f"list_dir failed: path does not exist: {path}")
            if path.is_file():
                raise NotADirectoryError(
                    f"list_dir failed: path is a file, not a directory: {path}. Use read_file instead."
                )
            recursive = bool(action.get("recursive", False))
            if recursive:
                items = [str(item) for item in sorted(path.rglob("*"))[:300]]
            else:
                items = [str(item) for item in sorted(path.iterdir())[:300]]
            return {"tool": "list_dir", "path": str(path), "items": items}

        if tool == "log_attempt":
            artifact_dir = Path(str(action.get("artifact_dir", ""))).resolve()
            profile_ref = action.get("profile_ref")
            note = action.get("note")
            log_result = self._record_attempt_from_artifact(
                tool_context, artifact_dir, profile_ref=profile_ref, note=note
            )
            if (
                isinstance(log_result, dict)
                and log_result.get("status") == "logged"
                and log_result.get("composite_score") is not None
            ):
                if not log_result.get("profile_ref") and isinstance(profile_ref, str):
                    log_result = {**log_result, "profile_ref": profile_ref.strip()}
                self._finalize_attempt_branch_state(
                    tool_context,
                    step,
                    step_limit,
                    policy,
                    auto_log=log_result,
                    cli_args=None,
                    source_action=action,
                    timeframe_mismatch=None,
                )
            return {"tool": "log_attempt", "result": log_result}

        if tool == "finish":
            return {"tool": "finish", "summary": action.get("summary", "")}

        raise ValueError(f"Unknown tool: {tool}")

    def _append_step_log(
        self, tool_context: ToolContext, payload: dict[str, Any]
    ) -> None:
        path = self._step_log_path(tool_context)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def _runtime_state_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "runtime-state.json"

    def _runtime_trace_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "runtime-trace.jsonl"

    def _raw_explorer_payloads_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "raw-explorer-payloads.jsonl"

    def _llm_request_snapshots_dir(self, tool_context: ToolContext) -> Path | None:
        if not self._llm_request_snapshots_enabled:
            return None
        return tool_context.run_dir / "llm-request-snapshots"

    def _json_safe_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, BaseException):
            return f"{type(value).__name__}: {value}"
        if isinstance(value, dict):
            sanitized: dict[str, Any] = {}
            for key, item in value.items():
                sanitized[str(key)] = self._json_safe_value(item)
            return sanitized
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe_value(item) for item in value]
        try:
            json.dumps(value, ensure_ascii=True)
            return value
        except (TypeError, ValueError):
            return str(value)

    def _trace_runtime(
        self,
        tool_context: ToolContext,
        *,
        step: int | None,
        phase: str,
        status: str,
        message: str,
        level: str = "info",
        **fields: Any,
    ) -> None:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": tool_context.run_id,
            "step": step,
            "phase": phase,
            "status": status,
            "message": message,
        }
        for key, value in fields.items():
            if value is not None:
                payload[key] = self._json_safe_value(value)
        trace_path = self._runtime_trace_path(tool_context)
        with trace_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        state_path = self._runtime_state_path(tool_context)
        merged: dict[str, Any] = {"last_trace": payload}
        if state_path.exists():
            try:
                cur = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(cur, dict):
                    if isinstance(cur.get("controller"), dict):
                        merged["controller"] = cur["controller"]
                    if isinstance(cur.get("controller_updated_at"), str):
                        merged["controller_updated_at"] = cur["controller_updated_at"]
                    if isinstance(cur.get("manager"), dict):
                        merged["manager"] = cur["manager"]
            except (OSError, json.JSONDecodeError):
                pass
        state_path.write_text(
            json.dumps(merged, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
        parts = [
            "run_trace",
            f"run_id={tool_context.run_id}",
            f"phase={phase}",
            f"status={status}",
        ]
        if step is not None:
            parts.append(f"step={step}")
        parts.append(f"message={message}")
        for key, value in fields.items():
            if value is None:
                continue
            text = str(value).replace("\n", " ").strip()
            if not text:
                continue
            if len(text) > 220:
                text = text[:217] + "..."
            parts.append(f"{key}={text}")
        line = " ".join(parts)
        if not _should_emit_runtime_trace_line(
            status=status, level=str(fields.get("level") or "")
        ):
            return
        print(line, file=sys.stderr, flush=True)

    def _append_raw_explorer_payload(
        self,
        tool_context: ToolContext,
        *,
        step: int,
        phase: str,
        event: str,
        source: str,
        label: str,
        payload_text: str | None = None,
        payload_json: Any = None,
        **fields: Any,
    ) -> None:
        record: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": tool_context.run_id,
            "step": step,
            "phase": phase,
            "event": event,
            "source": source,
            "label": label,
        }
        if payload_text is not None:
            record["payload_text"] = payload_text
            record["payload_text_chars"] = len(payload_text)
        if payload_json is not None:
            record["payload_json"] = self._json_safe_value(payload_json)
        for key, value in fields.items():
            if value is not None:
                record[key] = self._json_safe_value(value)
        path = self._raw_explorer_payloads_path(tool_context)
        try:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        except OSError:
            return

    def _provider_scope(
        self,
        *,
        tool_context: ToolContext,
        step: int,
        label: str,
        phase: str,
        provider: Any,
    ):
        provider_config = getattr(provider, "config", None)
        request_snapshot_dir = self._llm_request_snapshots_dir(tool_context)
        return provider_trace_scope(
            label=label,
            run_id=tool_context.run_id,
            step=step,
            phase=phase,
            provider_type=getattr(provider_config, "provider_type", None),
            model=getattr(provider_config, "model", None),
            capture_path=str(self._raw_explorer_payloads_path(tool_context)),
            request_snapshot_dir=(
                str(request_snapshot_dir) if request_snapshot_dir is not None else None
            ),
        )

    def _load_recent_step_payloads(
        self, tool_context: ToolContext, limit: int
    ) -> list[dict[str, Any]]:
        path = self._step_log_path(tool_context)
        if not path.exists() or limit <= 0:
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        payloads: list[dict[str, Any]] = []
        for line in lines[-limit:]:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                payloads.append(item)
        return payloads

    def _recent_step_window_text(
        self,
        tool_context: ToolContext,
        current_step_payload: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> str:
        effective_limit = max(1, limit or self.config.research.recent_step_window_steps)
        payloads = self._load_recent_step_payloads(tool_context, effective_limit)
        if current_step_payload is not None:
            payloads.append(current_step_payload)
        if not payloads:
            return "No recent step history is available."
        lines: list[str] = []
        for payload in payloads[-effective_limit:]:
            step = payload.get("step")
            reasoning = _short = " ".join(str(payload.get("reasoning", "")).split())
            if len(_short) > 180:
                _short = _short[:177] + "..."
            lines.append(f"Step {step}: {_short or 'n/a'}")
            actions = payload.get("actions")
            if isinstance(actions, list):
                for action in actions[:3]:
                    if isinstance(action, dict):
                        lines.append(
                            f"  action: {self._history_action_summary(action)}"
                        )
            results = payload.get("results")
            if isinstance(results, list):
                for result in results[:4]:
                    if isinstance(result, dict):
                        summary = self._history_result_summary(result)
                        lines.append(
                            f"  result: {json.dumps(summary, ensure_ascii=True)[:240]}"
                        )
        return "\n".join(lines)

    def _attempt_trade_count(self, attempt: dict[str, Any]) -> int | None:
        best_summary = attempt.get("best_summary")
        if not isinstance(best_summary, dict):
            return None
        best_cell = best_summary.get("best_cell")
        if isinstance(best_cell, dict):
            try:
                value = int(best_cell.get("resolved_trades"))
            except (TypeError, ValueError):
                value = None
            if value is not None and value >= 0:
                return value
        path_metrics = best_summary.get("best_cell_path_metrics")
        if isinstance(path_metrics, dict):
            try:
                value = int(path_metrics.get("trade_count"))
            except (TypeError, ValueError):
                value = None
            if value is not None and value >= 0:
                return value
        return None

    def _attempt_trades_per_month(self, attempt: dict[str, Any]) -> float | None:
        best_summary = attempt.get("best_summary")
        if isinstance(best_summary, dict):
            quality_score_payload = best_summary.get("quality_score_payload")
            if isinstance(quality_score_payload, dict):
                inputs = quality_score_payload.get("inputs")
                if isinstance(inputs, dict):
                    try:
                        value = float(inputs.get("trades_per_month"))
                    except (TypeError, ValueError):
                        value = None
                    if value is not None and value >= 0:
                        return value
                    try:
                        trade_count = float(inputs.get("resolved_trades"))
                        months = float(inputs.get("effective_window_months"))
                    except (TypeError, ValueError):
                        trade_count = None
                        months = None
                    if trade_count is not None and months is not None and months > 0:
                        return trade_count / months
        return None

    def _attempt_max_drawdown_r(self, attempt: dict[str, Any]) -> float | None:
        best_summary = attempt.get("best_summary")
        if not isinstance(best_summary, dict):
            return None
        path_metrics = best_summary.get("best_cell_path_metrics")
        if not isinstance(path_metrics, dict):
            return None
        try:
            value = float(path_metrics.get("max_drawdown_r"))
        except (TypeError, ValueError):
            return None
        return value

    def _attempt_positive_cell_ratio(self, attempt: dict[str, Any]) -> float | None:
        best_summary = attempt.get("best_summary")
        if not isinstance(best_summary, dict):
            return None
        matrix_summary = best_summary.get("matrix_summary")
        if not isinstance(matrix_summary, dict):
            return None
        try:
            value = float(matrix_summary.get("positive_cell_ratio"))
        except (TypeError, ValueError):
            return None
        return value

    def _recent_scored_attempts_text(
        self, tool_context: ToolContext, limit: int
    ) -> str:
        attempts = [
            attempt
            for attempt in self._run_attempts(tool_context.run_id)
            if attempt.get("composite_score") is not None
        ]
        if not attempts:
            return "No scored attempts yet."
        lines: list[str] = []
        for attempt in attempts[-max(1, limit) :]:
            trade_count = self._attempt_trade_count(attempt)
            trades_per_month = self._attempt_trades_per_month(attempt)
            positive_cell_ratio = self._attempt_positive_cell_ratio(attempt)
            parts = [
                f"seq={attempt.get('sequence')}",
                f"candidate={attempt.get('candidate_name')}",
                f"score={self._format_score(attempt.get('composite_score'))}",
                f"basis={attempt.get('score_basis', 'n/a')}",
            ]
            if trade_count is not None:
                parts.append(f"trades={trade_count}")
            if trades_per_month is not None:
                parts.append(f"trades_per_month={self._format_score(trades_per_month)}")
            if positive_cell_ratio is not None:
                parts.append(
                    f"positive_cell_ratio={self._format_score(positive_cell_ratio)}"
                )
            lines.append("- " + " ".join(parts))
        return "\n".join(lines)

    def _execution_issue_lines(
        self,
        tool_context: ToolContext,
        current_step_payload: dict[str, Any] | None,
        limit: int,
    ) -> list[str]:
        payloads = self._load_recent_step_payloads(tool_context, max(1, limit))
        if current_step_payload is not None:
            payloads.append(current_step_payload)
        issues: list[str] = []
        for payload in payloads[-max(1, limit) :]:
            step = payload.get("step")
            results = payload.get("results")
            if not isinstance(results, list):
                continue
            for result in results:
                if not isinstance(result, dict):
                    continue
                tool = str(result.get("tool", "unknown"))
                if result.get("error"):
                    issues.append(
                        f"- step={step} {tool}: {str(result.get('error'))[:220]}"
                    )
                elif tool in {"response_guard", "step_guard", "yield_guard"}:
                    message = str(
                        result.get("message") or result.get("error") or ""
                    ).strip()
                    if message:
                        issues.append(f"- step={step} {tool}: {message[:220]}")
        deduped: list[str] = []
        seen: set[str] = set()
        for issue in issues:
            if issue in seen:
                continue
            seen.add(issue)
            deduped.append(issue)
        return deduped[:6]

    def _synthesized_run_diagnosis(
        self,
        tool_context: ToolContext,
        current_step_payload: dict[str, Any] | None,
    ) -> str:
        attempts = self._run_attempts(tool_context.run_id)
        scored = [
            attempt
            for attempt in attempts
            if attempt.get("composite_score") is not None
        ]
        unscored = [
            attempt for attempt in attempts if attempt.get("composite_score") is None
        ]
        leader = self._best_attempt(attempts)
        lines: list[str] = []
        lines.append(
            f"- total_attempts={len(attempts)} scored={len(scored)} unscored={len(unscored)}"
        )
        if leader is not None:
            leader_trade_rate = self._attempt_trades_per_month(leader)
            leader_drawdown = self._attempt_max_drawdown_r(leader)
            leader_parts = [
                f"current_leader_seq={leader.get('sequence')}",
                f"score={self._format_score(leader.get('composite_score'))}",
                f"candidate={leader.get('candidate_name')}",
            ]
            if leader_trade_rate is not None:
                leader_parts.append(
                    f"trades_per_month={self._format_score(leader_trade_rate)}"
                )
            if leader_drawdown is not None:
                leader_parts.append(
                    f"max_drawdown_r={self._format_score(leader_drawdown)}"
                )
            lines.append("- " + " ".join(leader_parts))

        if len(scored) >= 2 and leader is not None:
            high_trade_scored = [
                attempt
                for attempt in scored
                if attempt is not leader
                and self._attempt_trades_per_month(attempt) is not None
            ]
            if high_trade_scored:
                highest_trade = max(
                    high_trade_scored,
                    key=lambda attempt: float(
                        self._attempt_trades_per_month(attempt) or 0.0
                    ),
                )
                highest_trade_rate = self._attempt_trades_per_month(highest_trade)
                leader_trade_rate = self._attempt_trades_per_month(leader)
                if (
                    highest_trade_rate is not None
                    and leader_trade_rate is not None
                    and highest_trade_rate
                    > max(leader_trade_rate * 2.0, leader_trade_rate + 20.0)
                    and self._score_better(
                        float(leader.get("composite_score")),
                        float(highest_trade.get("composite_score")),
                    )
                ):
                    lines.append(
                        "- high-trade branches have not beaten the current selective leader; "
                        f"highest recent trade-rate loser was {highest_trade.get('candidate_name')} at "
                        f"{self._format_score(highest_trade_rate)} trades/month with score "
                        f"{self._format_score(highest_trade.get('composite_score'))}"
                    )

        recent_tail = attempts[-6:]
        recent_unscored = sum(
            1 for attempt in recent_tail if attempt.get("composite_score") is None
        )
        if recent_unscored:
            lines.append(f"- recent_unscored_in_last_6={recent_unscored}")

        issues = self._execution_issue_lines(tool_context, current_step_payload, 4)
        if issues:
            lines.append(
                "- recent execution issues are present; prefer recovery over fresh broadening"
            )

        return "\n".join(lines)

    def _checkpoint_messages(
        self, history_messages: list[ChatMessage]
    ) -> list[ChatMessage]:
        serialized_history = [
            {"role": message.role, "content": message.content}
            for message in history_messages
        ]
        return [
            ChatMessage(role="system", content=COMPACTION_PROMPT),
            ChatMessage(
                role="user",
                content=(
                    "Summarize this controller history for the next continuation turn.\n\n"
                    + json.dumps(serialized_history, ensure_ascii=True)
                ),
            ),
        ]

    def _compact_message_history(
        self,
        messages: list[ChatMessage],
        tool_context: ToolContext,
        policy: RunPolicy,
        step: int,
        step_limit: int,
        *,
        compact_trigger_tokens: int,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[ChatMessage]:
        history_messages = messages[2:]
        if not history_messages:
            return messages
        approx_prompt_tokens_before = self._approx_message_tokens(messages)
        self._trace_runtime(
            tool_context,
            step=step,
            phase="compaction",
            status="start",
            message="Compacting message history.",
            history_messages=len(history_messages),
            approx_prompt_tokens_before=approx_prompt_tokens_before,
            compact_trigger_tokens=compact_trigger_tokens,
        )
        try:
            with self._provider_scope(
                tool_context=tool_context,
                step=step,
                label="compaction",
                phase="compaction",
                provider=self.provider,
            ):
                payload = self.provider.complete_json(
                    self._checkpoint_messages(history_messages)
                )
        except ProviderError as exc:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="compaction",
                status="failed",
                message="Compaction request failed; keeping full message history.",
                error=exc,
                level="warning",
            )
            return messages
        summary = payload.get("checkpoint_summary")
        if not isinstance(summary, str) or not summary.strip():
            self._trace_runtime(
                tool_context,
                step=step,
                phase="compaction",
                status="empty",
                message="Compaction returned no summary; keeping full message history.",
                level="warning",
            )
            return messages

        checkpoint_text = f"{SUMMARY_PREFIX}\n{summary.strip()}"
        self._checkpoint_path(tool_context).write_text(
            checkpoint_text, encoding="utf-8"
        )

        keep = max(0, self.config.research.compact_keep_recent_messages)
        recent_tail = history_messages[-keep:] if keep else []
        compacted_messages = [
            ChatMessage(
                role="system",
                content=self._system_protocol_text(
                    policy,
                    tool_context=tool_context,
                    step=step,
                ),
            ),
            ChatMessage(
                role="user",
                content=self._run_state_prompt(
                    tool_context, policy, step=step, step_limit=step_limit
                ),
            ),
            *recent_tail,
        ]
        approx_prompt_tokens_after = self._approx_message_tokens(compacted_messages)
        self._trace_runtime(
            tool_context,
            step=step,
            phase="compaction",
            status="ok",
            message="Compaction succeeded.",
            approx_prompt_tokens_before=approx_prompt_tokens_before,
            approx_prompt_tokens_after=approx_prompt_tokens_after,
            compact_trigger_tokens=compact_trigger_tokens,
            history_messages=len(history_messages),
            compacted_message_count=len(compacted_messages),
        )
        if progress_callback:
            progress_callback(
                {
                    "event": "context_compaction",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "step": step,
                    "approx_tokens_before": approx_prompt_tokens_before,
                    "approx_tokens_after": approx_prompt_tokens_after,
                    "compact_trigger_tokens": compact_trigger_tokens,
                }
            )
        return compacted_messages

    def _maybe_compact_messages(
        self,
        messages: list[ChatMessage],
        tool_context: ToolContext,
        policy: RunPolicy,
        step: int,
        step_limit: int,
        *,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[ChatMessage]:
        trigger = self.config.compact_trigger_tokens_for(
            self.config.llm.explorer_profile
        )
        if trigger <= 0:
            return messages
        if self._approx_message_tokens(messages) < trigger:
            return messages
        return self._compact_message_history(
            messages,
            tool_context,
            policy,
            step,
            step_limit,
            compact_trigger_tokens=trigger,
            progress_callback=progress_callback,
        )

    def _allow_finish(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        summary: str,
        policy: RunPolicy,
    ) -> tuple[bool, str]:
        if not policy.allow_finish:
            return (
                False,
                "Finish is disabled in supervised mode. Keep working until the controller stops the session.",
            )
        if not summary.strip():
            return (
                False,
                "Do not use finish as a continue marker. Finish is terminal and requires a non-empty summary.",
            )
        attempts = self._run_attempts(tool_context.run_id)
        min_attempts_before_finish = min(
            self.config.research.finish_min_attempts, step_limit
        )
        phase_info = self._run_phase_info(step, step_limit, policy)
        score_target = self._score_target_snapshot(tool_context)
        if phase_info["name"] != "wrap_up":
            wrap_up_start = phase_info.get("wrap_up_start")
            wrap_up_text = (
                f"Wrap-up begins at step {wrap_up_start}."
                if wrap_up_start
                else "Stay in exploration mode."
            )
            return (
                False,
                (
                    f"You are still in {phase_info['name']} phase. {phase_info['summary']} "
                    f"{wrap_up_text} {score_target['summary']}"
                ),
            )
        if len(attempts) >= min_attempts_before_finish:
            if (
                self.config.research.wrap_up_requires_validated_leader
                and not self._branch_overlay.validated_leader_family_id
            ):
                return (
                    False,
                    (
                        "Finish withheld: a validated leader (long-horizon retention passed) is required before stop. "
                        f"Continue with structural contrast or longer-horizon validation. {score_target['summary']}"
                    ),
                )
            return True, ""
        if step >= step_limit:
            return True, ""
        return (
            False,
            (
                "Do not finish yet. Wrap-up is open, but this run still needs more evidence before stopping. "
                f"Keep working until you have logged at least {min_attempts_before_finish} evaluated candidates "
                f"or hit the step limit. {score_target['summary']}"
            ),
        )

    def run(
        self,
        max_steps: int | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        policy: RunPolicy | None = None,
    ) -> dict[str, Any]:
        policy = policy or RunPolicy()
        self.profile_sources = {}
        self.last_created_profile_ref = None
        self.finish_denials = 0
        self._reset_run_state()
        self.cli.ensure_login()
        tool_context = self.create_run_context()
        self._refresh_progress_artifacts(tool_context)
        self._trace_runtime(
            tool_context,
            step=0,
            phase="run",
            status="started",
            message="Research run started.",
            explorer_profile=self.config.llm.explorer_profile,
        )
        effective_step_limit = max_steps or self.config.research.max_steps
        if progress_callback:
            initial_phase = self._run_phase_info(1, effective_step_limit, policy)
            initial_horizon = self._horizon_policy_snapshot(
                1, effective_step_limit, policy
            )
            initial_target = self._score_target_snapshot(tool_context)
            progress_callback(
                {
                    "event": "run_started",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "attempts_path": str(tool_context.attempts_path),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "max_steps": effective_step_limit,
                    "mode": policy.mode_name,
                    "phase": initial_phase["name"],
                    "horizon_target": initial_horizon["summary"],
                    "score_target": initial_target["summary"],
                }
            )
        if not self._within_operating_window(policy):
            result = {
                "status": "window_closed",
                "run_id": tool_context.run_id,
                "run_dir": str(tool_context.run_dir),
                "attempts_path": str(tool_context.attempts_path),
                "run_progress_plot": str(tool_context.progress_plot_path),
            }
            if progress_callback:
                progress_callback({"event": "window_closed", "result": result})
            return result
        messages: list[ChatMessage] = [
            ChatMessage(
                role="system",
                content=self._system_protocol_text(
                    policy,
                    tool_context=tool_context,
                    step=1,
                ),
            ),
            ChatMessage(
                role="user",
                content=self._run_state_prompt(
                    tool_context, policy, step=1, step_limit=effective_step_limit
                ),
            ),
        ]

        step_limit = effective_step_limit
        self._current_step_limit = step_limit
        self._current_run_policy = policy
        for step in range(1, step_limit + 1):
            self.last_created_profile_ref = None
            self._current_controller_step = step
            self._current_step_limit = step_limit
            self._current_run_policy = policy
            self._pending_manager_events = []
            self._branch_step_maintenance(tool_context, step, step_limit, policy)
            self._sync_branch_budget_mode(step, step_limit, policy)
            phase_now = self._run_phase_info(step, step_limit, policy)
            if (
                str(phase_now.get("name")) == "wrap_up"
                and self.config.manager.enabled
                and self.manager_providers
            ):
                self._manager_invoke_for_hook(
                    ManagerHookEvent.before_wrap_up_decision,
                    tool_context,
                    step,
                    step_limit,
                    policy,
                    extra_issues=["wrap_up_phase"],
                )
                self._sync_branch_budget_mode(step, step_limit, policy)
                self._persist_branch_runtime_state(tool_context, step)
            self._trace_runtime(
                tool_context,
                step=step,
                phase="step",
                status="start",
                message="Starting controller step.",
            )
            messages[1] = ChatMessage(
                role="user",
                content=self._run_state_prompt(
                    tool_context, policy, step=step, step_limit=step_limit
                ),
            )
            if step > 1 and not self._within_operating_window(policy):
                result = {
                    "status": "window_closed",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "attempts_path": str(tool_context.attempts_path),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                }
                if progress_callback:
                    progress_callback({"event": "window_closed", "result": result})
                return result
            messages = self._maybe_compact_messages(
                messages,
                tool_context,
                policy,
                step,
                step_limit,
                progress_callback=progress_callback,
            )
            try:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="explorer_provider",
                    status="waiting",
                    message="Waiting for explorer provider response.",
                    message_count=len(messages),
                )
                with self._provider_scope(
                    tool_context=tool_context,
                    step=step,
                    label="explorer",
                    phase="explorer_provider",
                    provider=self.provider,
                ):
                    raw_response = self.provider.complete_json(messages)
                self._append_raw_explorer_payload(
                    tool_context,
                    step=step,
                    phase="explorer_provider",
                    event="controller_received_payload",
                    source="controller",
                    label="explorer",
                    payload_json=raw_response,
                    message_count=len(messages),
                )
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="explorer_provider",
                    status="ok",
                    message="Explorer provider response received.",
                )
                try:
                    self._trace_runtime(
                        tool_context,
                        step=step,
                        phase="explorer_normalize",
                        status="start",
                        message="Normalizing explorer payload.",
                    )
                    response = self._normalize_model_response(raw_response)
                    response = self._apply_runtime_interventions(
                        tool_context,
                        step,
                        response,
                        phase="explorer_normalize",
                    )
                    self._append_raw_explorer_payload(
                        tool_context,
                        step=step,
                        phase="explorer_normalize",
                        event="controller_normalized_payload",
                        source="controller",
                        label="explorer",
                        payload_json=response,
                    )
                    self._trace_runtime(
                        tool_context,
                        step=step,
                        phase="explorer_normalize",
                        status="ok",
                        message="Explorer payload normalized.",
                    )
                except RuntimeError as exc:
                    repaired_shape = self._repair_invalid_payload_shape(
                        tool_context,
                        step,
                        messages,
                        raw_response,
                        str(exc),
                    )
                    if repaired_shape is None:
                        self._trace_runtime(
                            tool_context,
                            step=step,
                            phase="explorer_normalize",
                            status="failed",
                            message="Explorer payload normalization failed and shape repair did not recover it.",
                            error=exc,
                            level="error",
                        )
                        raise
                    response = repaired_shape
            except (ProviderError, CliError) as exc:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="explorer_provider",
                    status="failed",
                    message="Explorer provider call failed.",
                    error=exc,
                    level="error",
                )
                raise RuntimeError(str(exc)) from exc

            actions = response.get("actions")
            reasoning = str(response.get("reasoning", "")).strip()
            validation_errors = self._validate_actions(actions)
            validation_errors.extend(
                self._validate_finish_timing(
                    tool_context,
                    actions,
                    step,
                    step_limit,
                    policy,
                )
            )
            validation_errors.extend(
                self._validate_repeated_actions(
                    tool_context,
                    actions,
                )
            )
            validation_errors.extend(self._validate_timeframe_mismatch_block(actions))
            validation_errors.extend(
                self._validate_branch_lifecycle_actions(
                    tool_context,
                    actions,
                    step,
                    step_limit,
                    policy,
                )
            )
            if validation_errors:
                repaired = self._repair_invalid_response(
                    tool_context,
                    step,
                    messages,
                    reasoning,
                    actions if isinstance(actions, list) else [],
                    validation_errors,
                )
                if repaired is not None:
                    response = repaired
                    actions = response.get("actions")
                    reasoning = str(response.get("reasoning", "")).strip()
                    validation_errors = self._validate_actions(actions)
                    validation_errors.extend(
                        self._validate_finish_timing(
                            tool_context,
                            actions,
                            step,
                            step_limit,
                            policy,
                        )
                    )
                    validation_errors.extend(
                        self._validate_repeated_actions(
                            tool_context,
                            actions,
                        )
                    )
                    validation_errors.extend(
                        self._validate_timeframe_mismatch_block(actions)
                    )
                    validation_errors.extend(
                        self._validate_branch_lifecycle_actions(
                            tool_context,
                            actions,
                            step,
                            step_limit,
                            policy,
                        )
                    )
            if validation_errors:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="response_guard",
                    status="blocked",
                    message="Controller rejected model response after validation.",
                    error_count=len(validation_errors),
                    level="warning",
                )
                self._append_raw_explorer_payload(
                    tool_context,
                    step=step,
                    phase="response_guard",
                    event="response_guard_rejected",
                    source="controller",
                    label="explorer",
                    payload_json=response,
                    validation_errors=validation_errors,
                )
                horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
                step_payload = {
                    "step": step,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "phase": self._run_phase_info(step, step_limit, policy)["name"],
                    "horizon_target": horizon_policy["summary"],
                    "score_target": self._score_target_snapshot(tool_context)[
                        "summary"
                    ],
                    "reasoning": reasoning,
                    "actions": actions if isinstance(actions, list) else [],
                    "manager_events": [],
                    "results": [
                        {
                            "tool": "response_guard",
                            "ok": False,
                            "error": " ; ".join(validation_errors),
                        }
                    ],
                }
                step_payload["manager_events"].extend(self._flush_pending_manager_events())
                self._append_step_log(tool_context, step_payload)
                if progress_callback:
                    progress_callback(
                        {
                            "event": "step_completed",
                            "run_id": tool_context.run_id,
                            "run_dir": str(tool_context.run_dir),
                            "step_payload": step_payload,
                        }
                    )
                messages.append(
                    ChatMessage(
                        role="assistant",
                        content=f"Reasoning: {reasoning}",
                    )
                )
                messages.append(
                    ChatMessage(
                        role="user",
                        content=self._history_tool_results_message_content(
                            [step_payload["results"][0]]
                        ),
                    )
                )
                continue

            horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
            step_payload: dict[str, Any] = {
                "step": step,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "phase": self._run_phase_info(step, step_limit, policy)["name"],
                "horizon_target": horizon_policy["summary"],
                "score_target": self._score_target_snapshot(tool_context)["summary"],
                "reasoning": reasoning,
                "actions": actions,
                "manager_events": [],
                "results": [],
            }

            finished = False
            finish_summary = ""
            self._trace_runtime(
                tool_context,
                step=step,
                phase="action_execution",
                status="start",
                message="Executing planned actions.",
                action_count=len(actions) if isinstance(actions, list) else None,
            )
            for action in actions:
                action_summary = (
                    self._history_action_summary(action)
                    if isinstance(action, dict)
                    else str(action)
                )
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="action_execution",
                    status="action_start",
                    message="Starting action.",
                    action=action_summary,
                )
                try:
                    result = self._execute_action(
                        tool_context,
                        action,
                        step=step,
                        step_limit=step_limit,
                        policy=policy,
                    )
                except Exception as exc:
                    result = {
                        "tool": str(action.get("tool", "unknown")),
                        "ok": False,
                        "error": str(exc),
                    }
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="action_execution",
                    status="action_done"
                    if not result.get("error")
                    else "action_failed",
                    message="Action completed."
                    if not result.get("error")
                    else "Action failed.",
                    action=action_summary,
                    tool=result.get("tool"),
                    ok=result.get("ok"),
                    error=result.get("error"),
                    level="warning" if result.get("error") else "info",
                )
                step_payload["results"].append(result)
                hard_failure = bool(result.get("error"))
                res_tool = str(result.get("tool") or "")
                if res_tool in tt.CLI_OK_TOOLS and not bool(result.get("ok", True)):
                    hard_failure = True
                if hard_failure:
                    self._trace_runtime(
                        tool_context,
                        step=step,
                        phase="step_guard",
                        status="blocked",
                        message="Stopped executing remaining actions after first failed action.",
                        action=action_summary,
                        level="warning",
                    )
                    step_payload["results"].append(
                        {
                            "tool": "step_guard",
                            "message": "Stopped executing remaining actions after the first failed action in this step.",
                        }
                    )
                    break
                if result.get("tool") == "finish":
                    proposed_summary = str(result.get("summary", ""))
                    allow, message = self._allow_finish(
                        tool_context, step, step_limit, proposed_summary, policy
                    )
                    if allow:
                        self._trace_runtime(
                            tool_context,
                            step=step,
                            phase="finish",
                            status="accepted",
                            message="Finish accepted for run.",
                        )
                        finished = True
                        finish_summary = proposed_summary
                    else:
                        self._trace_runtime(
                            tool_context,
                            step=step,
                            phase="finish",
                            status="denied",
                            message="Finish denied.",
                            level="warning",
                        )
                        self.finish_denials += 1
                        guard_payload: dict[str, Any] = {
                            "tool": "yield_guard",
                            "message": message,
                            "finish_denials": self.finish_denials,
                            "phase": step_payload.get("phase"),
                            "horizon_target": step_payload.get("horizon_target"),
                            "score_target": step_payload.get("score_target"),
                        }
                        step_payload["results"].append(guard_payload)
                    break

            step_payload["manager_events"].extend(self._flush_pending_manager_events())
            self._append_step_log(tool_context, step_payload)
            self._persist_branch_runtime_state(tool_context, step)
            self._trace_runtime(
                tool_context,
                step=step,
                phase="step",
                status="completed",
                message="Controller step completed.",
                result_count=len(step_payload["results"]),
            )
            if progress_callback:
                progress_callback(
                    {
                        "event": "step_completed",
                        "run_id": tool_context.run_id,
                        "run_dir": str(tool_context.run_dir),
                        "step_payload": step_payload,
                    }
                )
            action_summaries = [
                self._history_action_summary(action)
                for action in actions
                if isinstance(action, dict)
            ]
            assistant_summary_lines = [f"Reasoning: {reasoning}"]
            if action_summaries:
                assistant_summary_lines.append("Planned actions:")
                assistant_summary_lines.extend(f"- {item}" for item in action_summaries)
            messages.append(
                ChatMessage(
                    role="assistant",
                    content="\n".join(assistant_summary_lines),
                )
            )
            messages.append(
                ChatMessage(
                    role="user",
                    content=self._history_tool_results_message_content(
                        [
                            result
                            for result in step_payload["results"]
                            if isinstance(result, dict)
                        ]
                    ),
                )
            )

            if finished:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="run",
                    status="finished",
                    message="Research run finished normally.",
                )
                return {
                    "status": "finished",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "attempts_path": str(tool_context.attempts_path),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "summary": finish_summary,
                }

        self._trace_runtime(
            tool_context,
            step=step_limit,
            phase="run",
            status="step_limit_reached",
            message="Research run hit the step limit.",
            level="warning",
        )
        self._persist_branch_runtime_state(tool_context, step_limit)
        return {
            "status": "step_limit_reached",
            "run_id": tool_context.run_id,
            "run_dir": str(tool_context.run_dir),
            "attempts_path": str(tool_context.attempts_path),
            "run_progress_plot": str(tool_context.progress_plot_path),
        }
