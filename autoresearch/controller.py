from __future__ import annotations

import json
import sys
import re
import shlex
from difflib import get_close_matches
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from . import branch_lifecycle as bl
from .config import AppConfig
from .fuzzfolio import CliError, CommandResult, FuzzfolioCli
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
from .scoring import build_attempt_score, load_sensitivity_snapshot


SYSTEM_PROTOCOL = """You are operating an autonomous Fuzzfolio research loop.

Return JSON only in this exact top-level shape:
{
  "reasoning": "one short paragraph",
  "actions": [
    {
      "tool": "run_cli" | "write_file" | "read_file" | "list_dir" | "log_attempt" | "finish",
      "... tool specific fields ..."
    }
  ]
}

Rules:
- Use absolute Windows paths.
- Prefer fuzzfolio-agent-cli for workflow actions.
- Keep actions bounded. Use at most 3 actions per response.
- Every evaluated candidate should end up in the attempts ledger. You may rely on automatic logging after sensitivity runs, or call log_attempt explicitly.
- Do not emit Markdown. Return raw JSON only.
- Do not return a raw scoring-profile document as the top-level response. If you want to create or edit a profile, do it through actions such as `write_file`, `profiles scaffold`, or `profiles patch`.
- The controller already handled auth bootstrap and created the run seed file before this conversation started. Do not spend steps repeating auth or seed unless a prior tool result shows a failure that requires recovery.
- Auth is already verified at run start. Do not call `auth whoami` unless you are recovering from an auth-related tool failure.
- Use only real CLI commands and subcommands. Do not invent near-miss names.
- Existing saved profiles from outside this run are off-limits as candidate seeds. Do not call profiles list/get/export to mine old profiles unless the user explicitly asks.
- Start from the current run's seed hand and write fresh portable profile JSON files under the current run's profiles directory.
- Prefer `profiles scaffold` to generate a valid starter profile from seeded indicator ids instead of hand-writing the whole schema from scratch.
- Prefer `profiles clone-local` to normalize/copy an existing local profile into a fresh run-owned portable document before branching.
- Prefer `profiles patch` for bounded edits to local profile files instead of rewriting whole JSON documents when only a few fields need to change.
- Prefer `profiles validate --file <ABS_FILE>` as a cheap preflight after materially editing a profile file.
- Use sweeps as a normal research tool, not a rare last resort. Prefer `sweep run` for the common scaffold+submit+wait workflow. Use `sweep scaffold`, `sweep patch`, `sweep validate` only when you need to inspect or edit the definition between steps.
- Only update profile refs that were created during this run.
- In profile JSON, `indicator.meta.id` must be an exact id from the sticky indicator catalog. Seed phrases and concept labels are not valid ids.
- After `profiles create`, use the returned profile id for later `--profile-ref` calls. A local `*.created.json` file is not itself a profile ref.
- After `profiles create`, the tool result will surface `created_profile_ref` directly. Use that exact value for the next evaluation step.
- Runtime placeholders like `<created_profile_ref>` may appear in tool arguments. Reuse them exactly when provided; the controller will substitute the real value.
- After `sensitivity-basket`, the expected artifact files are `sensitivity-response.json`, `deep-replay-job.json`, and sometimes `best-cell-path-detail.json`. Do not look for `summary.json`.
- `sensitivity` and `sensitivity-basket` now expose `requested_timeframe` and `effective_timeframe` in JSON output when you inspect stdout or saved responses.
- Saved analysis artifacts may also expose `effective_window_start`, `effective_window_end`, `effective_window_days`, and `effective_window_months`. Use those to judge whether a requested horizon was actually satisfied.
- Think in weeks, months, and years of evidence, not in raw bars.
- The controller owns default horizon policy and may inject phase-appropriate `--lookback-months` into sensitivity runs when you omit it.
- The controller also owns the active quality-score preset and injects it into deep-replay-backed evaluations and scaffolded sweeps. Do not try to vary or omit it yourself.
- Do not use `--bar-limit` as a research lever unless the user explicitly asks. Treat bar counts as implementation detail, not strategy.
- `__BASKET__` may appear inside saved analysis summaries as an aggregate label. It is not a valid CLI instrument argument. Use exact catalog symbols from the catalog.
- In early phase, diversify across multiple distinct instruments or small instrument groups before narrowing hard onto one pair unless the evidence is already unusually strong.
- Basket pruning is allowed when per-instrument evidence shows a specific symbol is a clear empirical drag on an otherwise promising basket. Do not assume basket expansion is justified from per-instrument results alone.
- `finish` is terminal for the whole run. Never use it to mean "continue" or "step complete".
- Only call `finish` when you intend to stop the run now and can provide a concise non-empty final summary.
- This is an iterative research session, not a one-shot evaluation. Keep exploring unless you have reached the step limit or the controller explicitly allows finish.
- A strong result should usually trigger a contrasting follow-up candidate, not immediate finish.
- Even after the minimum exploration threshold is satisfied, prefer using most of the remaining step budget if there are still obvious contrasting branches to test.
- For run_cli, prefer this shape:
  { "tool": "run_cli", "args": ["auth", "whoami", "--pretty"] }
- A legacy string command may also work, but args arrays are preferred.
- For write_file, always include both:
  { "tool": "write_file", "path": "C:\\abs\\file.json", "content": "{...full file text...}" }
- Never emit write_file without a full non-empty string `content` field.
- If a file body is too large to fit comfortably, emit fewer actions in that step. Do not omit `content`.
- Do not call `profiles create` or `profiles update` for a profile JSON path unless that file already exists on disk or you wrote it earlier in the same step.
- If `profiles create` fails, recover by fixing the profile JSON first. Do not continue to `sensitivity-basket` in the same step.

Retention and pacing rules (controller-enforced):
- The controller tracks each indicator family separately using instance IDs. Indicators sharing the same `meta.instanceId` values are the same family.
- After a family earns a strong score (quality_score >= 55) and the controller has spent several same-family exploit steps on it, the controller will require a longer-horizon validation before allowing more same-family tweaks.
- If a longer-horizon eval degrades materially vs the baseline strong score (delta <= -12 or ratio < 0.82), the controller will block further same-family exploit and require a structural contrast: a different indicator family, instrument cluster, timeframe architecture, or directional regime.
- After a family passes a longer-horizon retention check (delta >= -6 and score still strong), local tuning on that family is unlocked again.
- Indicators with few resolved trades (< 30) or low trades-per-month (< 2) are treated as sparse/selective and face stricter retention requirements.
- Same-family exploit actions include: notificationThreshold tweaks, lookbackBars tweaks, range-width tweaks, weight tweaks, and adjacent sweeps on the same core family.

Timeframe mismatch rules (controller-enforced):
- If a CLI output shows "Auto-adjusted timeframe from X to Y", that does NOT count as a valid higher-timeframe experiment. The run actually ran at Y, not X.
- The controller tracks these mismatches. If you repeatedly request the same higher timeframe with an unchanged profile that was already auto-adjusted, the controller will block that action.
- To properly test a higher timeframe: patch the indicator timeframe(s) in the profile to match your intended timeframe first, or reformulate the experiment as the effective lower timeframe and acknowledge that in your reasoning.

Behavior digest fields (available in run state prompt after each eval):
- edge_shape: persistent = strong across all horizons; episodic = strong sometimes; one_burst = early spike only; late_breakdown = degrades at longer horizons; flat_weak = weak everywhere
- support_shape: well_supported = many trades across many cells; selective_but_credible = fewer trades but credible; sparse_risky = too few signals to be sure; too_sparse = essentially uninterpretable
- drawdown_shape: smooth = consistent; clustered = blows up in specific regimes; late_blowup = holds early but fails later; high_chop = noisy across all horizons
- retention_risk: low = likely holds at longer horizons; moderate = uncertain; high = likely degrades when extended
- failure_mode_hint: recent_only = short-horizon artifact; trend_regime_dependent = works in trends not ranges; range_regime_dependent = the opposite; weak_support = too few signals to trust
- next_move_hint: validate_longer = run a longer-horizon check; contrast_family = try a different indicator family; prune_family = abandon this family; test_same_logic_new_instrument = same idea different market; local_tune_allowed = nearby tweaks still worthwhile; stop_threshold_tuning = plateau reached, stop tweaking thresholds

Use the behavior digest to guide your next branch decision, not just the scalar score.
"""

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
- You are running in supervised mode. The supervisor, not you, decides when the session stops.
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

SUPERVISOR_PROMPT = """You are the supervisor for an autonomous Fuzzfolio research run.

Your job is to redirect the explorer away from low-value wandering when it tries to stop early or gets stuck.
Be sharp, adventurous, concrete, and Socratic. Push for better branch quality, not just more steps.

Return JSON only in this exact shape:
{
  "message": "2-4 sentences of direct coaching",
  "questions": ["short question 1", "short question 2"],
  "next_moves": ["concrete move 1", "concrete move 2", "concrete move 3"]
}

Rules:
- Keep it compact.
- Work only within the current run, its seed hand, and run-owned artifacts.
- Do not suggest invalid CLI syntax or invalid instruments like __BASKET__.
- Prefer hypothesis pivots, contrast branches, and meaningful parameter or timeframe shifts over repetitive retries.
- Treat sweeps as first-class. If the explorer is doing repeated manual branch edits without any sweep support, push it toward a bounded sweep around the current promising family.
- Horizon policy belongs to you and the controller, not the explorer. Push the run to think in months and years, not bars.
- Quality-score preset choice also belongs to the controller. Assume evaluations are using the current preset consistently and do not ask the explorer to vary it.
- If an analysis window came back truncated, focus on the missing effective months/days, not the raw bar machinery.
- Early phase should screen cheaply, mid phase should deepen evidence, and late phase should pressure-test survivors over longer horizons.
- If the explorer is drifting, say so plainly.
- If the controller provides a score target, use it as a believable next stretch goal instead of vague encouragement.
- The controller's score target refers to `quality_score`, the aggregate source-of-truth metric. Do not describe it as PSR. You may mention PSR, DSR, drawdown, robustness, or other inputs separately as reasons why quality_score moved.
- During exploration phase, do not encourage finish or summary-writing.
"""

ADVISOR_PROMPT = """You are an expert strategy advisor for an autonomous Fuzzfolio research run.

You are not executing tools. You are giving short, high-signal guidance to the explorer model that is actively operating the loop.

Return JSON only in this exact shape:
{
  "message": "2-4 sentences of direct guidance",
  "next_moves": ["concrete move 1", "concrete move 2", "concrete move 3"],
  "risks": ["risk 1", "risk 2"]
}

Rules:
- Keep it compact and specific.
- Do not suggest finish or wrap-up unless the packet says wrap-up is active.
- Do not suggest invalid CLI syntax or invalid instruments.
- Prefer guidance that changes branch quality, not just branch count.
- Treat sweeps as first-class.
- Think in months and years of evidence, not bars.
- Use quality_score as the primary target metric, while using PSR, DSR, drawdown, robustness, trade rate, and coverage as reasons.
- Do not recommend broad indicator-family swaps unless the packet explicitly allows structural pivots.
- It is valid to recommend pruning a specific instrument from a basket when per-instrument evidence shows it is a clear drag. Do not recommend adding instruments based on per-instrument results alone.
- Assume the explorer can choose whether to follow your guidance; optimize for clarity, not control.
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
- Every write_file action must include a full non-empty string `content` field.
- If you cannot fit all planned work, reduce the number of actions.
- Do not omit required fields.
- Return raw JSON only.
"""


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
    def __init__(self, app_config: AppConfig):
        self.config = app_config
        self.provider = create_provider(app_config.provider)
        self.supervisor_provider = create_provider(app_config.supervisor_provider)
        self.advisor_providers = [
            (
                f"advisor{index}",
                profile_name,
                create_provider(app_config.providers[profile_name]),
            )
            for index, profile_name in enumerate(app_config.advisor.profiles, start=1)
            if profile_name in app_config.providers
        ]
        self.cli = FuzzfolioCli(app_config.fuzzfolio)
        self.profile_sources: dict[str, Path] = {}
        self.last_created_profile_ref: str | None = None
        self.finish_denials = 0
        self.profile_template_path = (
            self.config.repo_root / "portable_profile_template.json"
        )
        self._cli_help_catalog_cache: dict[str, Any] | None = None
        self._family_mutation_counts: dict[str, int] = {}
        self._family_last_score: dict[str, float] = {}
        self._family_baseline_score: dict[str, float] = {}
        self._family_last_horizon_months: dict[str, int] = {}
        self._family_retention_state: dict[str, dict[str, Any]] = {}
        self._consecutive_same_family_exploit: int = 0
        self._last_family_id: str | None = None
        self._timeframe_mismatches: list[dict[str, Any]] = []
        self._same_family_exploit_history: list[str] = []
        self._family_branches: dict[str, bl.FamilyBranchState] = {}
        self._branch_overlay = bl.BranchRunOverlay()
        self._current_controller_step: int = 0
        self._current_step_limit: int = 0
        self._current_run_policy: RunPolicy | None = None

    def _reset_run_state(self) -> None:
        self._family_mutation_counts = {}
        self._family_last_score = {}
        self._family_baseline_score = {}
        self._family_last_horizon_months = {}
        self._family_retention_state = {}
        self._consecutive_same_family_exploit = 0
        self._last_family_id = None
        self._timeframe_mismatches = []
        self._same_family_exploit_history = []
        self._family_branches = {}
        self._branch_overlay = bl.BranchRunOverlay()
        self._current_controller_step = 0
        self._current_step_limit = 0
        self._current_run_policy = None

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

    def _cli_action_hits_family_exploit_surface(
        self, args: list[str], family_id: str | None
    ) -> bool:
        if not family_id:
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

    def _mark_family_collapsed(
        self,
        tool_context: ToolContext,
        family_id: str,
        reason: str,
        step: int,
        step_limit: int,
    ) -> None:
        cfg = self.config.research
        branch = bl.ensure_family_branch(self._family_branches, family_id)
        if branch.exploit_dead:
            return
        branch.lifecycle_state = bl.LIFECYCLE_COLLAPSED
        branch.retention_status = bl.RETENTION_FAILED
        branch.bankrupt = True
        branch.exploit_dead = True
        branch.collapse_reason = reason
        branch.structural_contrast_required = True
        branch.cooldown_until_step = step + int(cfg.bankruptcy_cooldown_steps)
        self._branch_overlay.recent_retention_failures.append(step)
        keep = max(20, cfg.reseed_max_recent_failures_window * 4)
        self._branch_overlay.recent_retention_failures = self._branch_overlay.recent_retention_failures[
            -keep:
        ]
        self._maybe_activate_reseed(tool_context, step, step_limit)
        self._trace_runtime(
            tool_context,
            step=step,
            phase="branch_lifecycle",
            status="collapsed",
            message=f"Family {family_id[:20]}... collapsed: {reason}",
            family_id_prefix=family_id[:16],
        )

    def _maybe_activate_reseed(
        self, tool_context: ToolContext, step: int, step_limit: int
    ) -> None:
        cfg = self.config.research
        remaining = step_limit - step
        if remaining < cfg.reseed_min_remaining_steps:
            return
        if self._branch_overlay.validated_leader_family_id:
            return
        window = max(1, cfg.reseed_max_recent_failures_window)
        recent_fails = [s for s in self._branch_overlay.recent_retention_failures if step - s <= window]
        if not recent_fails:
            return
        dead = sum(1 for b in self._family_branches.values() if b.exploit_dead)
        if dead < 1 and not any(
            b.structural_contrast_required for b in self._family_branches.values()
        ):
            return
        if not self._branch_overlay.reseed_active:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="branch_lifecycle",
                status="reseed",
                message="Reseed / collapse-recovery window activated.",
            )
        self._branch_overlay.reseed_active = True
        if self._branch_overlay.reseed_started_step is None:
            self._branch_overlay.reseed_started_step = step
        self._branch_overlay.collapse_recovery_remaining = max(
            self._branch_overlay.collapse_recovery_remaining,
            cfg.collapse_recovery_max_steps,
        )

    def _branch_step_maintenance(
        self, step: int, step_limit: int, policy: RunPolicy
    ) -> None:
        for branch in self._family_branches.values():
            if branch.bankrupt and branch.cooldown_until_step <= step:
                branch.bankrupt = False
                if branch.exploit_dead:
                    branch.lifecycle_state = bl.LIFECYCLE_RESEED_ELIGIBLE
                else:
                    branch.lifecycle_state = bl.LIFECYCLE_SCOUT
        if (
            self._branch_overlay.reseed_active
            and self._branch_overlay.collapse_recovery_remaining > 0
        ):
            self._branch_overlay.collapse_recovery_remaining -= 1
            if self._branch_overlay.collapse_recovery_remaining <= 0:
                self._branch_overlay.reseed_active = False
        self._sync_branch_budget_mode(step, step_limit, policy)

    def _sync_branch_budget_mode(
        self, step: int, step_limit: int, policy: RunPolicy
    ) -> None:
        phase_info = self._run_phase_info(step, step_limit, policy)
        phase_name = str(phase_info.get("name") or "")
        self._recompute_branch_leaders()
        self._branch_overlay.explored_family_count = len(self._family_branches)
        dead_cnt = sum(1 for b in self._family_branches.values() if b.exploit_dead)
        if dead_cnt >= self.config.research.max_bankrupt_families_before_force_breadth:
            self._branch_overlay.budget_mode = bl.BUDGET_SCOUTING
            return
        if phase_name == "wrap_up":
            self._branch_overlay.budget_mode = bl.BUDGET_WRAP_UP
            return
        if (
            self._branch_overlay.reseed_active
            and self._branch_overlay.collapse_recovery_remaining > 0
        ):
            self._branch_overlay.budget_mode = bl.BUDGET_COLLAPSE_RECOVERY
            return
        if self._branch_overlay.validated_leader_family_id:
            self._branch_overlay.budget_mode = (
                bl.BUDGET_VALIDATION
                if phase_name in {"mid", "late"}
                else bl.BUDGET_EXPLOIT
            )
            return
        if phase_name == "early" or not self._branch_overlay.provisional_leader_family_id:
            self._branch_overlay.budget_mode = bl.BUDGET_SCOUTING
            return
        self._branch_overlay.budget_mode = (
            bl.BUDGET_VALIDATION if phase_name in {"mid", "late"} else bl.BUDGET_EXPLOIT
        )

    def _recompute_branch_leaders(self) -> None:
        threshold = self.config.research.retention_strong_candidate_threshold
        best_prov: tuple[float, str | None] = (float("-inf"), None)
        best_val: tuple[float, str | None] = (float("-inf"), None)
        for fid, st in self._family_branches.items():
            if st.exploit_dead or st.bankrupt or bl.cooldown_active(st, self._current_controller_step):
                continue
            sc = st.best_score
            if sc is None or sc < threshold:
                continue
            if (
                st.promotion_level == bl.PROMOTION_VALIDATED
                and st.retention_status == bl.RETENTION_PASSED
            ):
                if sc > best_val[0]:
                    best_val = (sc, fid)
            else:
                if sc > best_prov[0]:
                    best_prov = (sc, fid)
        self._branch_overlay.validated_leader_family_id = best_val[1]
        self._branch_overlay.provisional_leader_family_id = best_prov[1]

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
) -> None:
        if not family_id:
            return
        cfg = self.config.research
        branch = bl.ensure_family_branch(self._family_branches, family_id)
        digest = behavior_digest or {}
        support_shape = str(digest.get("support_shape") or "")
        retention_risk = str(digest.get("retention_risk") or "low")
        next_move_hint = str(digest.get("next_move_hint") or "")

        if branch.first_seen_attempt_id is None and attempt_id:
            branch.first_seen_attempt_id = attempt_id
        branch.latest_attempt_id = attempt_id
        branch.latest_score = score
        branch.latest_horizon_months = requested_horizon_months
        branch.latest_effective_window_months = effective_window_months
        if profile_ref:
            branch.last_profile_ref = str(profile_ref).strip()

        if branch.best_score is None or score > branch.best_score:
            branch.best_score = score
            branch.best_attempt_id = attempt_id
            branch.best_horizon_months = requested_horizon_months
            branch.best_effective_window_months = effective_window_months

        if support_shape == "too_sparse" and branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED:
            branch.lifecycle_state = bl.LIFECYCLE_RETENTION_WARNING

        if retention_risk == "high" and next_move_hint in {
            "contrast_family",
            "prune_family",
        }:
            branch.structural_contrast_required = True
        if next_move_hint == "stop_threshold_tuning":
            branch.structural_contrast_required = True

        coverage_ok = True
        if requested_horizon_months is not None and effective_window_months is not None:
            try:
                req = float(requested_horizon_months)
                eff = float(effective_window_months)
                floor = req * float(cfg.effective_coverage_min_ratio)
                coverage_ok = eff + 1e-6 >= floor
            except (TypeError, ValueError):
                coverage_ok = False
        elif (
            cfg.horizon_failure_counts_as_retention_fail
            and requested_horizon_months is not None
            and effective_window_months is None
        ):
            coverage_ok = False

        if not coverage_ok and cfg.horizon_failure_counts_as_retention_fail:
            w = (
                cfg.retention_digest_high_risk_fail_weight
                if retention_risk == "high"
                else 1.0
            )
            branch.retention_fail_count += max(1, int(round(w)))
            branch.coverage_inadequate_count += 1

        if had_timeframe_mismatch:
            branch.timeframe_mismatch_hits += 1
            if branch.timeframe_mismatch_hits >= 3:
                self._mark_family_collapsed(
                    tool_context,
                    family_id,
                    "repeated_timeframe_intent_mismatch",
                    step,
                    step_limit,
                )

        min_horizon = cfg.validated_leader_min_horizon_months
        if (
            requested_horizon_months is not None
            and requested_horizon_months < min_horizon
        ):
            peak = branch.provisional_peak_score
            if peak is None or score > peak:
                branch.provisional_peak_score = score
                branch.provisional_peak_horizon_months = requested_horizon_months

        if (
            score is not None
            and requested_horizon_months is not None
            and requested_horizon_months >= min_horizon
            and branch.provisional_peak_score is not None
            and score
            < branch.provisional_peak_score * float(cfg.provisional_leader_decay_ratio)
        ):
            branch.long_rung_low_score_streak += 1
        else:
            branch.long_rung_low_score_streak = 0

        if branch.long_rung_low_score_streak >= 2:
            self._mark_family_collapsed(
                tool_context,
                family_id,
                "repeated_long_horizon_scores_far_below_provisional_peak",
                step,
                step_limit,
            )

        rr = retention_result or {}
        if rr.get("retention_failed"):
            self._mark_family_collapsed(
                tool_context,
                family_id,
                "retention_threshold_failed",
                step,
                step_limit,
            )

        rs = self._family_retention_state.get(family_id, {})
        if not branch.exploit_dead and rs.get("retention_check_passed"):
            branch.retention_status = bl.RETENTION_PASSED
            if (
                requested_horizon_months is not None
                and requested_horizon_months >= min_horizon
                and coverage_ok
            ):
                branch.promotion_level = bl.PROMOTION_VALIDATED
                branch.lifecycle_state = bl.LIFECYCLE_VALIDATED_LEADER
            else:
                branch.promotion_level = bl.PROMOTION_PROVISIONAL
                if branch.lifecycle_state not in {
                    bl.LIFECYCLE_COLLAPSED,
                    bl.LIFECYCLE_VALIDATED_LEADER,
                }:
                    branch.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_LEADER

        if rr.get("needs_retention_check") and not branch.exploit_dead:
            branch.retention_status = bl.RETENTION_PENDING

        if (
            not branch.exploit_dead
            and branch.retention_fail_count >= cfg.bankruptcy_fail_count
        ):
            self._mark_family_collapsed(
                tool_context,
                family_id,
                "retention_fail_count_budget_exceeded",
                step,
                step_limit,
            )

        if (
            not branch.exploit_dead
            and score >= cfg.retention_strong_candidate_threshold
            and branch.promotion_level == bl.PROMOTION_SCOUT
            and branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED
        ):
            branch.promotion_level = bl.PROMOTION_PROVISIONAL
            branch.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_CONTENDER

        self._sync_branch_budget_mode(step, step_limit, policy)
        self._persist_branch_runtime_state(tool_context, step)

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
        path.write_text(json.dumps(prior, ensure_ascii=True, indent=2), encoding="utf-8")

    def _build_branch_runtime_snapshot(
        self, tool_context: ToolContext, step: int
    ) -> dict[str, Any]:
        overlay = self._branch_overlay
        collapsed = [
            fid
            for fid, st in self._family_branches.items()
            if st.lifecycle_state == bl.LIFECYCLE_COLLAPSED or st.exploit_dead
        ]
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
            "budget_mode": overlay.budget_mode,
            "reseed_active": overlay.reseed_active,
            "reseed_started_step": overlay.reseed_started_step,
            "collapse_recovery_remaining": overlay.collapse_recovery_remaining,
            "explored_family_count": overlay.explored_family_count,
            "collapsed_families_count": len(collapsed),
            "collapsed_family_prefixes": [c[:16] + "..." for c in collapsed[:12]],
            "families_on_cooldown": cooldown,
            "families": {
                k[:24] + ("..." if len(k) > 24 else ""): v.to_dict()
                for k, v in list(self._family_branches.items())[:40]
            },
        }

    def _branch_lifecycle_run_packet_text(
        self, tool_context: ToolContext, step: int, step_limit: int
    ) -> str:
        ov = self._branch_overlay
        lines = [
            "Branch lifecycle (controller-owned):",
            f"- budget_mode: {ov.budget_mode}",
            f"- reseed_active: {ov.reseed_active} (collapse_recovery_steps_left={ov.collapse_recovery_remaining})",
            f"- provisional_leader_family: {(ov.provisional_leader_family_id or 'none')[:28]}{'...' if ov.provisional_leader_family_id and len(ov.provisional_leader_family_id) > 28 else ''}",
            f"- validated_leader_family: {(ov.validated_leader_family_id or 'none')[:28]}{'...' if ov.validated_leader_family_id and len(ov.validated_leader_family_id) > 28 else ''}",
            f"- explored_distinct_families: {ov.explored_family_count}",
        ]
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
            if str(action.get("tool", "")).strip() != "run_cli":
                continue
            try:
                args = [str(item) for item in self._normalize_cli_args(action)]
            except Exception:
                continue
            family_id = self._resolve_cli_family_id(args)
            if not family_id:
                continue
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
        return errors

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
        trade_count = self._attempt_trade_count(attempt)
        trades_per_month = self._attempt_trades_per_month(attempt)
        positive_ratio = self._attempt_positive_cell_ratio(attempt)
        if trade_count is None:
            return "sparse"
        if trade_count < 30:
            return "sparse"
        if trades_per_month is not None and trades_per_month < 2:
            return "selective"
        if positive_ratio is not None and positive_ratio < 0.3:
            return "selective"
        return "broad"

    def _is_same_family_exploit_action(self, action: dict[str, Any]) -> bool:
        tool = str(action.get("tool", "")).strip()
        if tool != "run_cli":
            return False
        args = action.get("args")
        if not isinstance(args, list):
            command = str(action.get("command", "")).lower()
            args = command.split()
        args_lower = [str(a).lower() for a in args]
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
        threshold = cfg.retention_strong_candidate_threshold
        max_mutations = cfg.retention_max_same_family_mutations_before_check
        current_mutations = self._family_mutation_counts.get(candidate_family_id, 0)
        last_score = self._family_last_score.get(candidate_family_id)
        baseline_score = self._family_baseline_score.get(candidate_family_id)
        last_horizon = self._family_last_horizon_months.get(candidate_family_id)
        retention_state = self._family_retention_state.get(candidate_family_id, {})
        is_strong = current_score >= threshold
        support_quality = retention_state.get("support_quality", "normal")
        needs_retention_check = False
        gated_message = None
        horizon_increased = (
            horizon_months is not None
            and last_horizon is not None
            and horizon_months > last_horizon
        )
        if is_strong and current_mutations >= max_mutations:
            if not retention_state.get("retention_check_passed"):
                retention_done = retention_state.get("retention_check_done")
                if not retention_done:
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
                self._family_retention_state[candidate_family_id] = {
                    "retention_check_done": True,
                    "retention_check_passed": False,
                    "last_delta": delta,
                    "last_ratio": ratio,
                    "support_quality": support_quality,
                    "baseline_score": baseline_score,
                    "retention_horizon": horizon_months,
                }
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
            if delta >= cfg.retention_pass_delta and current_score >= threshold:
                self._family_retention_state[candidate_family_id] = {
                    "retention_check_done": True,
                    "retention_check_passed": True,
                    "last_delta": delta,
                    "support_quality": support_quality,
                }
        self._family_last_score[candidate_family_id] = current_score
        if horizon_months is not None:
            self._family_last_horizon_months[candidate_family_id] = horizon_months
        if is_strong and baseline_score is None:
            self._family_baseline_score[candidate_family_id] = current_score
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
            support = "normal"
            if self._family_retention_state.get(family_id):
                support = self._family_retention_state[family_id].get(
                    "support_quality", "normal"
                )
            elif support_quality:
                support = support_quality
            self._family_retention_state[family_id] = {"support_quality": support}
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
        if "Auto-adjusted timeframe from" not in combined_output:
            return None
        match = re.search(
            r"Auto-adjusted timeframe from\s+(\S+)\s+to\s+(\S+)", combined_output
        )
        if not match:
            return None
        requested_timeframe = match.group(1)
        effective_timeframe = match.group(2)
        entry = {
            "requested": requested_timeframe,
            "effective": effective_timeframe,
            "mismatch": requested_timeframe != effective_timeframe,
        }
        self._timeframe_mismatches.append(entry)
        return entry

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
            "message": (
                f"Timeframe auto-adjustment detected: requested {latest.get('requested')} "
                f"but CLI ran {latest.get('effective')}. "
                f"This does NOT count as a valid higher-timeframe experiment. "
                f"Next action must resolve the mismatch: patch active indicator timeframe(s) "
                f"to the intended timeframe, reformulate as {latest.get('effective')} test, "
                f"or abandon that timeframe hypothesis. "
                f"Do NOT request the same higher-timeframe eval with the same unchanged profile."
            ),
        }

    def _generate_behavior_digest(self, attempt: dict[str, Any]) -> dict[str, Any]:
        score = attempt.get("composite_score")
        trade_count = self._attempt_trade_count(attempt)
        trades_per_month = self._attempt_trades_per_month(attempt)
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
        elif trade_count is not None and trade_count < 20:
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

    def _system_protocol_text(self, policy: RunPolicy) -> str:
        if policy.allow_finish:
            return SYSTEM_PROTOCOL
        return SYSTEM_PROTOCOL + "\n" + SUPERVISED_EXTRA_RULES

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
            "supervisor_profile": self.config.llm.supervisor_profile,
            "supervisor_provider_type": self.config.supervisor_provider.provider_type,
            "supervisor_model": self.config.supervisor_provider.model,
            "quality_score_preset": self.config.research.quality_score_preset,
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
        run_best = self._best_attempt(self._run_attempts(tool_context.run_id))

        current_score = (
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
                "push past the current run leader with one believable improvement"
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
                f"Next target: get quality_score <= {self._format_score(target_score)}. "
                f"Current run best={self._format_score(current_score)}."
            )
        else:
            summary = (
                f"Next target: get quality_score >= {self._format_score(target_score)}. "
                f"Current run best={self._format_score(current_score)}."
            )

        return {
            "target_score": target_score,
            "current_run_best_score": current_score,
            "current_run_best_candidate": run_best.get("candidate_name")
            if isinstance(run_best, dict)
            else None,
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
            lines.append(
                f"- seq={attempt.get('sequence')} score={attempt.get('composite_score')} "
                f"basis={attempt.get('score_basis', 'n/a')} dsr={metrics.get('dsr', 'n/a')} "
                f"psr={metrics.get('psr', 'n/a')} candidate={attempt.get('candidate_name')} "
                f"trades={cell.get('resolved_trades', 'n/a')} "
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
            "Sensitivity artifact layout:\n"
            "- sensitivity-response.json\n"
            "- deep-replay-job.json\n"
            "- best-cell-path-detail.json (when available)\n"
            "Use compare-sensitivity for compact scoring. Do not expect summary.json."
        )

    def _seed_to_catalog_hints_text(self, seed_indicator_ids: list[str]) -> str:
        if not seed_indicator_ids:
            return (
                "Seed indicator guidance:\n"
                "- No seeded indicator ids were available for this run.\n"
                "- If the seed hand lacks explicit ids, inspect the seed file first before drafting profiles."
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
        soft_wrap_note = self._soft_wrap_note(policy)
        cli_guide = (
            "Important CLI command shapes:\n"
            "- profiles clone-local --file <ABS_FILE> --out <ABS_FILE>\n"
            '- profiles patch --file <ABS_FILE> --set profile.name="..." --set profile.indicators[0].config.timeframe="H1" --out <ABS_FILE>\n'
            "- profiles scaffold --indicator <ID> --indicator <ID> --instrument <SYMBOL> --out <ABS_FILE>\n"
            "- profiles scaffold --indicator <ID> --indicator <ID> --instrument <SYMBOL> --instrument <SYMBOL> --out <ABS_FILE>\n"
            "- profiles validate --file <ABS_FILE> --pretty\n"
            "- profiles create --file <ABS_FILE> --out <ABS_FILE>\n"
            "- profiles update --profile-ref <REF> --file <ABS_FILE> --out <ABS_FILE>\n"
            "- sweep scaffold --profile-ref <REF> --instrument <SYMBOL> --axis profile.notificationThreshold=70,75,80 --axis indicator[0].config.lookbackBars=1,2,3 --out <ABS_FILE>\n"
            '- sweep patch --definition <ABS_FILE> --set fitness_metric="quality_score" --out <ABS_FILE>\n'
            "- sweep validate --definition <ABS_FILE> --pretty\n"
            "- sweep submit --definition <ABS_FILE_OR_INLINE_JSON> --out <ABS_FILE> --pretty\n"
            "- sweep run --profile-ref <REF> --instrument <SYMBOL> --axis profile.notificationThreshold=70,75,80 --axis indicator[0].config.lookbackBars=1,2,3\n"
            "- sensitivity-basket --profile-ref <REF> --timeframe <TF> --instrument <INSTRUMENT> --lookback-months <MONTHS> --output-dir <ABS_DIR>\n"
            "- sensitivity-basket --profile-ref <REF> --timeframe <TF> --instrument <INSTRUMENT> --instrument <INSTRUMENT> --lookback-months <MONTHS> --output-dir <ABS_DIR>\n"
            "- compare-sensitivity --input <ABS_DIR> --pretty\n"
            "Notes:\n"
            "- profiles scaffold generates a valid portable profile from live indicator templates and is preferred for fresh candidate bootstrapping.\n"
            "- profiles clone-local normalizes/copies an existing local profile into a fresh portable document for safe local branching.\n"
            "- profiles patch applies deterministic path=value edits to a local profile file and is preferred for small branch mutations.\n"
            "- profiles validate performs a local schema/instrument preflight and is preferred before create when you materially edited a profile.\n"
            "- profiles create/update require --file. They do not accept branch/indicator/timeframe flags.\n"
            "- Create fresh run-owned profile JSON from scaffold output, clone-local output, or the portable template, then call profiles create.\n"
            "- sweep scaffold builds a valid sweep definition around an existing saved profile using simple axis expressions. Prefer it over hand-writing sweep JSON.\n"
            "- sweep patch applies deterministic edits to a local sweep definition file.\n"
            "- sweep validate performs a local structural preflight before sweep submit.\n"
            "- sweep run combines scaffold+submit+wait into a single action: use it as the default when you want to run a sweep and get results in one step. Only use scaffold/validate/submit separately when you need to inspect or edit the definition between steps.\n"
            "- IMPORTANT: sweep run does NOT take --timeframe or --out. It uses the timeframe embedded in the profile. It does accept optional --output-dir to write results to a file. Do NOT mix sensitivity-basket flags into a sweep run command.\n"
            "- Only exact indicator ids from the sticky indicator catalog are valid in indicator.meta.id.\n"
            "- The seed prompt is backed by the live indicator catalog, but seed concepts are still ideas, not ids.\n"
            "- Use the seed-to-valid-id hints when the seed uses semantic phrases instead of exact ids.\n"
            "- After profiles create, use the returned data.id as the profile ref for later commands.\n"
            "- The controller also returns created_profile_ref explicitly in the tool result. Prefer that field.\n"
            "- sensitivity and sensitivity-basket accept --pretty when printing JSON to stdout.\n"
            "- sensitivity-basket writes a directory when using --output-dir.\n"
            "- If you omit --lookback-months on sensitivity commands, the controller will inject the phase-appropriate horizon automatically.\n"
            "- Do not use --bar-limit as a research lever. The controller strips it unless the user explicitly asks.\n"
            "- sensitivity-basket may auto-adjust the timeframe down to the profile's lowest active indicator timeframe.\n"
            "- Saved sensitivity responses now include requested_timeframe and effective_timeframe fields.\n"
            "- Raw bar-count mechanics are implementation detail. Prefer effective_window_days and effective_window_months when judging whether a requested horizon was really satisfied.\n"
            '- If command syntax drifts, use run_cli ["help"] or run_cli ["help", "profiles"] instead of guessing.\n'
            "- Multi-instrument commands repeat --instrument once per symbol. Never comma-join symbols into a single token.\n"
            "- `__BASKET__` may appear in saved summaries as an aggregate label. Never pass it as --instrument.\n"
            "- Invalid instrument aliases now fail fast with close-match suggestions.\n"
            "- In early phase, do not spend the whole run anchored to one pair. Explore across multiple distinct instruments or small instrument groups before narrowing hard.\n"
            "- If basket analysis shows one instrument is a clear empirical drag, pruning that weak link is a valid follow-up branch.\n"
            "- Do not widen a basket just because extra instruments look acceptable. Correlation-aware expansion is out of scope for now.\n"
            "- A normal managed run should explore multiple candidates. Do not stop after the first strong score; branch and test at least a few follow-up ideas.\n"
            "- Do not finish the run as soon as the minimum threshold is reached if there is still room in the step budget for a couple more meaningful contrasts.\n"
            "- If a sensitivity run already auto-logged the attempt, avoid redundant log_attempt unless you are recovering from a missing ledger entry.\n"
            "- Use compare-sensitivity when comparing artifact directories or inspecting score details, not as a mandatory step after every successful sensitivity run.\n"
            "- Sweeps are a required part of healthy search, especially in early and mid phases. Do not spend the whole run on manual one-off edits only.\n"
            "- Post-eval files are sensitivity-response.json, deep-replay-job.json, and best-cell-path-detail.json when available.\n"
            "- Do not try to read summary.json after sensitivity-basket.\n"
            "- Do not use old saved profiles as candidate seeds for this run.\n"
            "- If you need a new profile, write the profile JSON first, then create it, then evaluate it.\n"
            "- If a profile create fails, do not evaluate that profile ref in the same step.\n"
            "- Reuse successful profile JSON patterns and valid TA-Lib parameter names from prior successful create results when branching.\n"
            "- MA_CROSSOVER uses fastperiod, slowperiod, and optional matype. It does not use signalperiod.\n"
            "- Horizon strategy is phase-driven: early = cheap screening, mid = deeper confirmation, late/wrap-up = long-horizon pressure test.\n"
        )
        return (
            f"Repo root: {self.config.repo_root}\n"
            f"Mode: {policy.mode_name}\n"
            f"Run id: {tool_context.run_id}\n"
            f"Run dir: {tool_context.run_dir}\n"
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
            f"Operating window: {policy.window_start or 'none'} -> {policy.window_end or 'none'} ({policy.timezone_name})\n"
            f"{soft_wrap_note + chr(10) if soft_wrap_note else ''}"
            f"Profiles dir: {tool_context.profiles_dir}\n"
            f"Evals dir: {tool_context.evals_dir}\n"
            f"Notes dir: {tool_context.notes_dir}\n"
            f"Run attempts ledger: {tool_context.attempts_path}\n"
            f"Run progress plot: {tool_context.progress_plot_path}\n"
            f"CLI help catalog path: {tool_context.cli_help_catalog_path}\n"
            f"Program:\n{self._program_text()}\n\n"
            f"Current seed hand:\n{self._seed_text(tool_context)}\n\n"
            f"Portable profile template path: {tool_context.profile_template_path}\n"
            f"Portable profile template:\n{self._profile_template_text(tool_context)}\n\n"
            f"Sticky indicator context:\n{tool_context.indicator_catalog_summary or 'Unavailable'}\n\n"
            f"Seeded indicator parameter hints:\n{tool_context.seed_indicator_parameter_hints or 'Unavailable'}\n\n"
            f"Sticky instrument context:\n{tool_context.instrument_catalog_summary or 'Unavailable'}\n\n"
            f"{self._seed_to_catalog_hints_text(self._seed_indicator_ids(tool_context.seed_prompt_path))}\n\n"
            f"{self._artifact_layout_text()}\n\n"
            f"Profile JSON files currently on disk:\n{self._profile_files_summary(tool_context)}\n\n"
            f"Run-owned profiles so far:\n{self._run_owned_profiles_summary(tool_context)}\n\n"
            f"Sticky frontier snapshot:\n{self._frontier_snapshot_text(tool_context)}\n\n"
            f"Checkpoint summary:\n{checkpoint}\n\n"
            f"Recent attempts:\n{self._recent_attempts_summary(tool_context)}\n\n"
            f"{self._retention_and_exploit_status_text(tool_context)}\n\n"
            f"{self._branch_lifecycle_run_packet_text(tool_context, effective_step, effective_step_limit)}\n\n"
            f"{self._timeframe_mismatch_status_text()}\n\n"
            f"{self._recent_behavior_digest_text(tool_context)}\n"
            f"\nCLI guide:\n{cli_guide}\n"
        )

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
        for family_id, state in self._family_retention_state.items():
            passed = state.get("retention_check_passed")
            done = state.get("retention_check_done")
            support = state.get("support_quality", "unknown")
            mutations = self._family_mutation_counts.get(family_id, 0)
            short_family = family_id[:16] + "..." if len(family_id) > 16 else family_id
            if done:
                if passed:
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
        if tool == "run_cli":
            payload = (
                result.get("result") if isinstance(result.get("result"), dict) else {}
            )
            summarized: dict[str, Any] = {
                "tool": tool,
                "ok": bool(result.get("ok")),
            }
            if result.get("created_profile_ref"):
                summarized["created_profile_ref"] = result.get("created_profile_ref")
            if result.get("auto_log") is not None:
                summarized["auto_log"] = result.get("auto_log")
            if isinstance(payload, dict):
                argv = payload.get("argv")
                cli_args = argv if isinstance(argv, list) else []
                command_head = [str(item) for item in cli_args[-3:]]
                returncode = payload.get("returncode")
                if returncode is not None:
                    summarized["returncode"] = returncode
                stdout = payload.get("stdout")
                stderr = payload.get("stderr")
                parsed = payload.get("parsed_json")
                if isinstance(parsed, dict):
                    preview_keys = [
                        "data",
                        "id",
                        "requested_timeframe",
                        "effective_timeframe",
                        "status",
                    ]
                    summarized["parsed_json_keys"] = [
                        key for key in preview_keys if key in parsed
                    ][:8]
                    if len(cli_args) >= 1:
                        if "compare-sensitivity" in cli_args:
                            best = parsed.get("best")
                            if isinstance(best, dict):
                                best_cell = best.get("best_cell")
                                best_path = best.get("best_cell_path_metrics")
                                market_window = best.get("market_data_window")
                                matrix_summary = best.get("matrix_summary")
                                compare_summary: dict[str, Any] = {
                                    "quality_score": best.get("quality_score"),
                                    "signal_count": best.get("signal_count"),
                                    "timeframe": best.get("timeframe"),
                                }
                                if isinstance(best_cell, dict):
                                    compare_summary["resolved_trades"] = best_cell.get(
                                        "resolved_trades"
                                    )
                                    compare_summary["avg_net_r_per_closed_trade"] = (
                                        best_cell.get("avg_net_r_per_closed_trade")
                                    )
                                if isinstance(best_path, dict):
                                    compare_summary["psr"] = best_path.get("psr")
                                    compare_summary["dsr"] = best.get("dsr")
                                    compare_summary["k_ratio"] = best_path.get(
                                        "k_ratio"
                                    )
                                    compare_summary["sharpe_r"] = best_path.get(
                                        "sharpe_r"
                                    )
                                    compare_summary["max_drawdown_r"] = best_path.get(
                                        "max_drawdown_r"
                                    )
                                if isinstance(market_window, dict):
                                    compare_summary["effective_window_months"] = (
                                        market_window.get("effective_window_months")
                                    )
                                    compare_summary["window_truncated"] = (
                                        market_window.get("window_truncated")
                                    )
                                if isinstance(matrix_summary, dict):
                                    compare_summary["positive_cell_ratio"] = (
                                        matrix_summary.get("positive_cell_ratio")
                                    )
                                summarized["compare_summary"] = compare_summary
                if isinstance(stdout, str) and "Auto-adjusted timeframe from" in stdout:
                    summarized["timeframe_auto_adjusted"] = True
                if (
                    isinstance(stderr, str)
                    and stderr.strip()
                    and not bool(result.get("ok"))
                ):
                    summarized["stderr"] = stderr[:500]
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
        if tool == "advisor_guidance":
            advisors = result.get("advisors")
            labels = []
            if isinstance(advisors, list):
                for item in advisors[:4]:
                    if isinstance(item, dict):
                        label = str(item.get("label") or "").strip()
                        if label:
                            labels.append(label)
            return {
                "tool": tool,
                "message": result.get("message"),
                "advisors": labels,
            }
        if tool in {"yield_guard", "finish"}:
            return result
        return result

    def _validate_action(self, action: Any) -> str | None:
        if not isinstance(action, dict):
            return "Action must be an object."
        tool = str(action.get("tool", "")).strip()
        if not tool:
            return "Action is missing tool."
        if tool not in {
            "run_cli",
            "write_file",
            "read_file",
            "list_dir",
            "log_attempt",
            "finish",
        }:
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
        try:
            self._normalize_cli_args(action)
        except Exception as exc:
            return str(exc)
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
            if tool != "run_cli":
                continue
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
                    "Problems:\n" + "\n".join(f"- {error}" for error in errors)
                ),
            ),
        ]
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
            normalized = self._normalize_model_response(repaired)
        except (ProviderError, RuntimeError, TypeError, ValueError) as exc:
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
            normalized = self._normalize_model_response(repaired)
        except (ProviderError, RuntimeError) as exc:
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
        if (
            record.composite_score is not None
            and self._current_run_policy is not None
            and self._current_step_limit > 0
        ):
            profile_path = (
                Path(record.profile_path)
                if record.profile_path
                else None
            )
            family_id = self._derive_family_id_from_profile(profile_path)
            if family_id:
                attempt_dict = asdict(record)
                digest = self._generate_behavior_digest(attempt_dict)
                eff_float: float | None = None
                if effective_window_months is not None:
                    try:
                        eff_float = float(effective_window_months)
                    except (TypeError, ValueError):
                        eff_float = None
                horizon_int = (
                    int(eff_float) if eff_float is not None else None
                )
                retention_snapshot = self._check_retention_gating(
                    tool_context,
                    family_id,
                    float(record.composite_score),
                    horizon_int,
                )
                self._refresh_branch_lifecycle_after_eval(
                    tool_context,
                    self._current_controller_step,
                    self._current_step_limit,
                    self._current_run_policy,
                    family_id=family_id,
                    profile_ref=record.profile_ref,
                    attempt_id=record.attempt_id,
                    score=float(record.composite_score),
                    requested_horizon_months=horizon_int,
                    effective_window_months=eff_float,
                    retention_result=retention_snapshot,
                    behavior_digest=digest,
                    had_timeframe_mismatch=False,
                )
        return {
            "status": "logged",
            "attempt_id": record.attempt_id,
            "composite_score": record.composite_score,
            "primary_score": record.primary_score,
            "score_basis": record.score_basis,
            "metrics": record.metrics,
            "signal_count": signal_count,
            "resolved_trades": resolved_trades,
            "effective_window_months": effective_window_months,
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
        return self._record_attempt_from_artifact(
            tool_context, artifact_dir, profile_ref=profile_ref
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
            guard_error = self._guard_cli_args(args)
            if guard_error:
                return {
                    "tool": "run_cli",
                    "ok": False,
                    "created_profile_ref": None,
                    "source_profile_file": None,
                    "result": {
                        "argv": [*self.cli.build_base_argv(), *args],
                        "cwd": str(Path(action["cwd"]).resolve())
                        if action.get("cwd")
                        else str(
                            (
                                self.config.fuzzfolio.workspace_root or Path.cwd()
                            ).resolve()
                        ),
                        "returncode": 2,
                        "stdout": "",
                        "stderr": guard_error,
                    },
                    "auto_log": None,
                }
            if "--profile-ref" in args:
                profile_index = args.index("--profile-ref") + 1
                if profile_index < len(args):
                    args[profile_index] = self._resolve_profile_ref_arg(
                        str(args[profile_index])
                    )
            result = self.cli.run(
                [str(item) for item in args],
                cwd=Path(action["cwd"]) if action.get("cwd") else None,
                check=False,
            )

            serialized_result = json.loads(self._serialize_tool_result(result))
            timeframe_mismatch = self._detect_timeframe_mismatch(serialized_result)

            profile_ref: str | None = None
            file_arg: Path | None = None
            if result.returncode == 0 and args[:2] in (
                ["profiles", "create"],
                ["profiles", "update"],
            ):
                payload = (
                    result.parsed_json if isinstance(result.parsed_json, dict) else {}
                )
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
            if auto_log is not None and auto_log.get("status") == "logged":
                artifact_dir_str = auto_log.get("artifact_dir", "")
                if artifact_dir_str:
                    artifact_path = Path(artifact_dir_str)
                    score = auto_log.get("composite_score")
                    profile_ref_for_family = auto_log.get("profile_ref")
                    profile_path_for_family = (
                        self.profile_sources.get(profile_ref_for_family)
                        if profile_ref_for_family
                        else None
                    )
                    family_id = self._derive_family_id_from_profile(
                        profile_path_for_family
                    )
                    is_exploit = self._is_same_family_exploit_action(action)
                    resolved_trades = auto_log.get("resolved_trades")
                    trades_per_month = auto_log.get("trades_per_month")
                    positive_ratio = auto_log.get("positive_cell_ratio")
                    support_quality = "broad"
                    trade_count_val = (
                        resolved_trades if isinstance(resolved_trades, int) else None
                    )
                    tpm_val = (
                        trades_per_month
                        if isinstance(trades_per_month, (int, float))
                        else None
                    )
                    pos_val = (
                        positive_ratio
                        if isinstance(positive_ratio, (int, float))
                        else None
                    )
                    if trade_count_val is not None and trade_count_val < 30:
                        support_quality = "sparse"
                    elif tpm_val is not None and tpm_val < 2:
                        support_quality = "selective"
                    elif pos_val is not None and pos_val < 0.3:
                        support_quality = "selective"
                    if family_id:
                        self._update_family_exploit_state(
                            family_id, is_exploit, support_quality
                        )
                        if score is not None:
                            eff_raw = auto_log.get("effective_window_months")
                            horizon_months = (
                                int(eff_raw)
                                if eff_raw is not None
                                and str(eff_raw).strip() not in {"", "none"}
                                else None
                            )
                            requested_horizon_months = self._parse_lookback_months_from_cli_args(
                                args
                            )
                            retention_result = self._check_retention_gating(
                                tool_context, family_id, float(score), horizon_months
                            )
                            attempts_list = self._run_attempts(tool_context.run_id)
                            attempt_dict = (
                                attempts_list[-1] if attempts_list else None
                            )
                            digest = (
                                self._generate_behavior_digest(attempt_dict)
                                if isinstance(attempt_dict, dict)
                                else {}
                            )
                            had_mismatch = timeframe_mismatch is not None
                            eff_float: float | None = None
                            if isinstance(eff_raw, (int, float)):
                                eff_float = float(eff_raw)
                            elif eff_raw is not None:
                                try:
                                    eff_float = float(str(eff_raw).strip())
                                except (TypeError, ValueError):
                                    eff_float = None
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
                                requested_horizon_months=(
                                    requested_horizon_months
                                    if requested_horizon_months is not None
                                    else horizon_months
                                ),
                                effective_window_months=eff_float,
                                retention_result=retention_result,
                                behavior_digest=digest,
                                had_timeframe_mismatch=had_mismatch,
                            )
                            if retention_result.get("retention_failed"):
                                retention_result["auto_log"] = auto_log
                                return {
                                    "tool": "run_cli",
                                    "ok": result.returncode == 0,
                                    "created_profile_ref": profile_ref,
                                    "source_profile_file": str(file_arg)
                                    if file_arg
                                    else None,
                                    "result": serialized_result,
                                    "auto_log": auto_log,
                                    "timeframe_mismatch": timeframe_mismatch,
                                    "retention_gate": retention_result,
                                }
            return {
                "tool": "run_cli",
                "ok": result.returncode == 0,
                "created_profile_ref": profile_ref,
                "source_profile_file": str(file_arg) if file_arg else None,
                "result": serialized_result,
                "auto_log": auto_log,
                "timeframe_mismatch": timeframe_mismatch,
            }

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
            return {
                "tool": "log_attempt",
                "result": self._record_attempt_from_artifact(
                    tool_context, artifact_dir, profile_ref=profile_ref, note=note
                ),
            }

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
                payload[key] = value
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
        return provider_trace_scope(
            label=label,
            run_id=tool_context.run_id,
            step=step,
            phase=phase,
            provider_type=getattr(provider_config, "provider_type", None),
            model=getattr(provider_config, "model", None),
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
        effective_limit = max(1, limit or self.config.research.supervisor_recent_steps)
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

    def _advisor_packet_text(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        current_step_payload: dict[str, Any] | None,
    ) -> str:
        phase_info = self._run_phase_info(step, step_limit, policy)
        horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
        score_target = self._score_target_snapshot(tool_context)
        run_metadata = load_run_metadata(tool_context.run_dir)
        leader = self._best_attempt(self._run_attempts(tool_context.run_id))
        leader_lines = "No scored leader yet."
        if leader is not None:
            leader_lines = "\n".join(
                [
                    f"- seq={leader.get('sequence')}",
                    f"- candidate={leader.get('candidate_name')}",
                    f"- quality_score={self._format_score(leader.get('composite_score'))}",
                    f"- score_basis={leader.get('score_basis', 'n/a')}",
                    f"- psr={self._format_score((leader.get('metrics') or {}).get('psr')) if isinstance(leader.get('metrics'), dict) else 'n/a'}",
                    f"- trades_per_month={self._format_score(self._attempt_trades_per_month(leader))}",
                    f"- resolved_trades={self._attempt_trade_count(leader) if self._attempt_trade_count(leader) is not None else 'n/a'}",
                    f"- max_drawdown_r={self._format_score(self._attempt_max_drawdown_r(leader))}",
                    f"- positive_cell_ratio={self._format_score(self._attempt_positive_cell_ratio(leader))}",
                    f"- artifact={leader.get('artifact_dir')}",
                ]
            )
        issues = self._execution_issue_lines(
            tool_context,
            current_step_payload,
            self.config.advisor.max_recent_steps,
        )
        issue_text = "\n".join(issues) if issues else "No recent execution issues."
        checkpoint_path = self._checkpoint_path(tool_context)
        checkpoint_summary = (
            checkpoint_path.read_text(encoding="utf-8")
            if checkpoint_path.exists()
            else "No checkpoint summary exists yet."
        )
        return (
            "Run advisory packet\n\n"
            f"Run id: {tool_context.run_id}\n"
            f"Mode: {policy.mode_name}\n"
            f"Step: {step}/{step_limit}\n"
            f"Phase: {phase_info['name']}\n"
            f"Explorer profile: {run_metadata.get('explorer_profile') or self.config.llm.explorer_profile}\n"
            f"Explorer model: {run_metadata.get('explorer_model') or self.config.provider.model}\n"
            f"Supervisor profile: {run_metadata.get('supervisor_profile') or self.config.llm.supervisor_profile}\n"
            f"Supervisor model: {run_metadata.get('supervisor_model') or self.config.supervisor_provider.model}\n"
            f"Quality-score preset: {run_metadata.get('quality_score_preset') or self.config.research.quality_score_preset}\n\n"
            f"Controller horizon target:\n{horizon_policy['summary']}\n\n"
            f"Controller score target:\n{score_target['summary']}\n\n"
            "Advisory goal:\n"
            "Give the explorer short, concrete guidance that improves the next few steps. "
            "Do not try to end the run. Help it avoid drift, stale paths, and low-value retries.\n\n"
            f"Current best run-local leader:\n{leader_lines}\n\n"
            f"Recent scored attempts:\n{self._recent_scored_attempts_text(tool_context, self.config.advisor.max_recent_attempts)}\n\n"
            f"Frontier snapshot:\n{self._frontier_snapshot_text(tool_context)}\n\n"
            f"Recent execution issues:\n{issue_text}\n\n"
            f"Synthesized run diagnosis:\n{self._synthesized_run_diagnosis(tool_context, current_step_payload)}\n\n"
            f"Checkpoint summary:\n{checkpoint_summary[:2000]}\n\n"
            f"Recent step window:\n{self._recent_step_window_text(tool_context, current_step_payload, limit=self.config.advisor.max_recent_steps)}\n\n"
            "Decision request:\n"
            "Help the explorer choose the highest-value next branch over the next 2-3 steps.\n"
        )

    def _advisor_feedback_message(self, advisor_result: dict[str, Any]) -> str:
        advisors = advisor_result.get("advisors")
        if not isinstance(advisors, list) or not advisors:
            return ""
        lines = [
            "External advisor guidance. Treat this as advisory input, not a command. "
            "Use it if it improves the next bounded actions."
        ]
        for advisor in advisors:
            if not isinstance(advisor, dict):
                continue
            profile = str(advisor.get("label") or "advisor").strip()
            message = str(advisor.get("message") or "").strip()
            next_moves = advisor.get("next_moves")
            risks = advisor.get("risks")
            lines.append(f"{profile}: {message}")
            if isinstance(next_moves, list):
                for move in next_moves[:3]:
                    lines.append(f"- next: {str(move).strip()}")
            if isinstance(risks, list):
                for risk in risks[:2]:
                    lines.append(f"- risk: {str(risk).strip()}")
        return "\n".join(lines)

    def _periodic_advisor_guidance(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        current_step_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self.config.advisor.enabled:
            return None
        if step >= step_limit:
            return None
        cadence = max(1, int(self.config.advisor.every_n_steps))
        if step % cadence != 0:
            return None
        if not self.advisor_providers:
            return None

        packet = self._advisor_packet_text(
            tool_context,
            step,
            step_limit,
            policy,
            current_step_payload,
        )
        advisors: list[dict[str, Any]] = []
        for advisor_label, profile_name, provider in self.advisor_providers:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="advisor",
                status="start",
                message="Requesting periodic advisor guidance.",
                advisor=advisor_label,
                profile=profile_name,
            )
            try:
                with self._provider_scope(
                    tool_context=tool_context,
                    step=step,
                    label=f"advisor:{advisor_label}",
                    phase="advisor",
                    provider=provider,
                ):
                    payload = provider.complete_json(
                        [
                            ChatMessage(role="system", content=ADVISOR_PROMPT),
                            ChatMessage(role="user", content=packet),
                        ]
                    )
            except ProviderError as exc:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="advisor",
                    status="failed",
                    message="Advisor request failed.",
                    advisor=advisor_label,
                    error=exc,
                    level="warning",
                )
                continue
            message = str(payload.get("message") or "").strip()
            next_moves = payload.get("next_moves")
            risks = payload.get("risks")
            if not message:
                self._trace_runtime(
                    tool_context,
                    step=step,
                    phase="advisor",
                    status="empty",
                    message="Advisor returned no usable message.",
                    advisor=advisor_label,
                    level="warning",
                )
                continue
            self._trace_runtime(
                tool_context,
                step=step,
                phase="advisor",
                status="ok",
                message="Advisor guidance received.",
                advisor=advisor_label,
            )
            advisors.append(
                {
                    "label": advisor_label,
                    "message": message,
                    "next_moves": (
                        [str(item).strip() for item in next_moves[:3]]
                        if isinstance(next_moves, list)
                        else []
                    ),
                    "risks": (
                        [str(item).strip() for item in risks[:2]]
                        if isinstance(risks, list)
                        else []
                    ),
                }
            )
        if not advisors:
            return None
        return {
            "tool": "advisor_guidance",
            "message": f"Injected {len(advisors)} advisor note(s).",
            "advisors": advisors,
        }

    def _supervisor_guidance(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        policy: RunPolicy,
        finish_summary: str,
        denial_message: str,
        current_step_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        attempts = self._run_attempts(tool_context.run_id)
        attempt_lines: list[str] = []
        for attempt in attempts[-6:]:
            attempt_lines.append(
                f"- seq={attempt.get('sequence')} candidate={attempt.get('candidate_name')} "
                f"score={attempt.get('composite_score')} basis={attempt.get('score_basis')}"
            )
        phase_info = self._run_phase_info(step, step_limit, policy)
        horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
        score_target = self._score_target_snapshot(tool_context)
        prompt = (
            f"Step: {step}/{step_limit}\n"
            f"Run phase: {phase_info['name']}\n"
            f"Phase guidance: {phase_info['summary']}\n"
            f"Horizon target: {horizon_policy['summary']}\n"
            f"Horizon guidance: {horizon_policy['guidance']}\n"
            f"Horizon rationale: {horizon_policy['rationale']}\n"
            f"Finish denial count: {self.finish_denials + 1}\n"
            f"Denied finish summary: {finish_summary or 'n/a'}\n"
            f"Controller denial: {denial_message}\n\n"
            f"Score target:\n{score_target['summary']}\n"
            f"Target rationale: {score_target['rationale']}\n\n"
            f"Frontier snapshot:\n{self._frontier_snapshot_text(tool_context)}\n\n"
            f"Recent run attempts:\n{chr(10).join(attempt_lines) if attempt_lines else 'No attempts yet.'}\n\n"
            f"Recent step window:\n{self._recent_step_window_text(tool_context, current_step_payload)}\n"
        )
        self._trace_runtime(
            tool_context,
            step=step,
            phase="supervisor",
            status="start",
            message="Requesting supervisor guidance after finish denial.",
        )
        try:
            with self._provider_scope(
                tool_context=tool_context,
                step=step,
                label="supervisor",
                phase="supervisor",
                provider=self.supervisor_provider,
            ):
                payload = self.supervisor_provider.complete_json(
                    [
                        ChatMessage(role="system", content=SUPERVISOR_PROMPT),
                        ChatMessage(role="user", content=prompt),
                    ]
                )
        except ProviderError as exc:
            self._trace_runtime(
                tool_context,
                step=step,
                phase="supervisor",
                status="failed",
                message="Supervisor request failed.",
                error=exc,
                level="warning",
            )
            return None
        message = payload.get("message")
        questions = payload.get("questions")
        next_moves = payload.get("next_moves")
        if not isinstance(message, str) or not message.strip():
            self._trace_runtime(
                tool_context,
                step=step,
                phase="supervisor",
                status="empty",
                message="Supervisor returned no usable message.",
                level="warning",
            )
            return None
        self._trace_runtime(
            tool_context,
            step=step,
            phase="supervisor",
            status="ok",
            message="Supervisor guidance received.",
        )
        return {
            "message": message.strip(),
            "questions": [str(item).strip() for item in questions[:3]]
            if isinstance(questions, list)
            else [],
            "next_moves": [str(item).strip() for item in next_moves[:3]]
            if isinstance(next_moves, list)
            else [],
        }

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
    ) -> list[ChatMessage]:
        history_messages = messages[2:]
        if not history_messages:
            return messages
        self._trace_runtime(
            tool_context,
            step=step,
            phase="compaction",
            status="start",
            message="Compacting message history.",
            history_messages=len(history_messages),
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
        self._trace_runtime(
            tool_context,
            step=step,
            phase="compaction",
            status="ok",
            message="Compaction succeeded.",
        )

        keep = max(0, self.config.research.compact_keep_recent_messages)
        recent_tail = history_messages[-keep:] if keep else []
        return [
            ChatMessage(role="system", content=self._system_protocol_text(policy)),
            ChatMessage(
                role="user",
                content=self._run_state_prompt(
                    tool_context, policy, step=step, step_limit=step_limit
                ),
            ),
            *recent_tail,
        ]

    def _maybe_compact_messages(
        self,
        messages: list[ChatMessage],
        tool_context: ToolContext,
        policy: RunPolicy,
        step: int,
        step_limit: int,
    ) -> list[ChatMessage]:
        trigger = self.config.compact_trigger_tokens_for(
            self.config.llm.explorer_profile
        )
        if trigger <= 0:
            return messages
        if self._approx_message_tokens(messages) < trigger:
            return messages
        return self._compact_message_history(
            messages, tool_context, policy, step, step_limit
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
                "Finish is disabled in supervised mode. Keep working until the supervisor stops prompting you.",
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
            supervisor_profile=self.config.llm.supervisor_profile,
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
            ChatMessage(role="system", content=self._system_protocol_text(policy)),
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
            self._branch_step_maintenance(step, step_limit, policy)
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
                messages, tool_context, policy, step, step_limit
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
                    "results": [
                        {
                            "tool": "response_guard",
                            "ok": False,
                            "error": " ; ".join(validation_errors),
                        }
                    ],
                }
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
                        content="Tool results:\n"
                        + json.dumps(
                            [self._history_result_summary(step_payload["results"][0])],
                            ensure_ascii=True,
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
                "results": [],
            }

            finished = False
            finish_summary = ""
            advisor_result: dict[str, Any] | None = None
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
                if result.get("tool") == "run_cli" and not bool(result.get("ok", True)):
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
                            message="Finish denied; requesting supervisor guidance.",
                            level="warning",
                        )
                        self.finish_denials += 1
                        supervisor = self._supervisor_guidance(
                            tool_context,
                            step,
                            step_limit,
                            policy,
                            proposed_summary,
                            message,
                            step_payload,
                        )
                        guard_payload: dict[str, Any] = {
                            "tool": "yield_guard",
                            "message": message,
                            "finish_denials": self.finish_denials,
                            "phase": step_payload.get("phase"),
                            "horizon_target": step_payload.get("horizon_target"),
                            "score_target": step_payload.get("score_target"),
                        }
                        if supervisor:
                            guard_payload["supervisor_message"] = supervisor.get(
                                "message"
                            )
                            guard_payload["questions"] = supervisor.get("questions", [])
                            guard_payload["next_moves"] = supervisor.get(
                                "next_moves", []
                            )
                        step_payload["results"].append(guard_payload)
                    break

            if not finished:
                advisor_result = self._periodic_advisor_guidance(
                    tool_context,
                    step,
                    step_limit,
                    policy,
                    step_payload,
                )
                if advisor_result is not None:
                    self._trace_runtime(
                        tool_context,
                        step=step,
                        phase="advisor",
                        status="injected",
                        message="Advisor guidance injected into conversation.",
                        advisor_count=len(advisor_result.get("advisors", []))
                        if isinstance(advisor_result, dict)
                        else None,
                    )
                    step_payload["results"].append(advisor_result)

            self._append_step_log(tool_context, step_payload)
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
                    content=(
                        "Tool results:\n"
                        + json.dumps(
                            [
                                self._history_result_summary(result)
                                for result in step_payload["results"]
                                if isinstance(result, dict)
                                and str(result.get("tool", "")) != "advisor_guidance"
                            ],
                            ensure_ascii=True,
                        )
                    ),
                )
            )
            if advisor_result is not None:
                advisor_message = self._advisor_feedback_message(advisor_result)
                if advisor_message.strip():
                    messages.append(
                        ChatMessage(
                            role="user",
                            content=advisor_message,
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
        return {
            "status": "step_limit_reached",
            "run_id": tool_context.run_id,
            "run_dir": str(tool_context.run_dir),
            "attempts_path": str(tool_context.attempts_path),
            "run_progress_plot": str(tool_context.progress_plot_path),
        }
