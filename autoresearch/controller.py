from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from .config import AppConfig
from .fuzzfolio import CliError, CommandResult, FuzzfolioCli
from .ledger import append_attempt, attempt_exists, load_attempts, make_attempt_record
from .plotting import compute_frontier, render_progress_artifacts
from .provider import ChatMessage, ProviderError, create_provider
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
- The controller already handled auth bootstrap and created the run seed file before this conversation started. Do not spend steps repeating auth or seed unless a prior tool result shows a failure that requires recovery.
- Auth is already verified at run start. Do not call `auth whoami` unless you are recovering from an auth-related tool failure.
- Use only real CLI commands and subcommands. Do not invent near-miss names.
- Existing saved profiles from outside this run are off-limits as candidate seeds. Do not call profiles list/get/export to mine old profiles unless the user explicitly asks.
- Start from the current run's seed hand and write fresh portable profile JSON files under the current run's profiles directory.
- Prefer `profiles scaffold` to generate a valid starter profile from seeded indicator ids instead of hand-writing the whole schema from scratch.
- Prefer `profiles clone-local` to normalize/copy an existing local profile into a fresh run-owned portable document before branching.
- Prefer `profiles patch` for bounded edits to local profile files instead of rewriting whole JSON documents when only a few fields need to change.
- Prefer `profiles validate --file <ABS_FILE>` as a cheap preflight after materially editing a profile file.
- Only update profile refs that were created during this run.
- In profile JSON, `indicator.meta.id` must be an exact id from the sticky indicator catalog. Seed phrases and concept labels are not valid ids.
- After `profiles create`, use the returned profile id for later `--profile-ref` calls. A local `*.created.json` file is not itself a profile ref.
- After `profiles create`, the tool result will surface `created_profile_ref` directly. Use that exact value for the next evaluation step.
- Runtime placeholders like `<created_profile_ref>` may appear in tool arguments. Reuse them exactly when provided; the controller will substitute the real value.
- After `sensitivity-basket`, the expected artifact files are `sensitivity-response.json`, `deep-replay-job.json`, and sometimes `best-cell-path-detail.json`. Do not look for `summary.json`.
- `sensitivity` and `sensitivity-basket` now expose `requested_timeframe` and `effective_timeframe` in JSON output when you inspect stdout or saved responses.
- Think in weeks, months, and years of evidence, not in raw bars.
- The controller owns default horizon policy and may inject phase-appropriate `--lookback-months` into sensitivity runs when you omit it.
- Do not use `--bar-limit` as a research lever unless the user explicitly asks. Treat bar counts as implementation detail, not strategy.
- `__BASKET__` may appear inside saved analysis summaries as an aggregate label. It is not a valid CLI instrument argument. Use exact catalog symbols like EURUSD.
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
"""

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
- Horizon policy belongs to you and the controller, not the explorer. Push the run to think in months and years, not bars.
- Early phase should screen cheaply, mid phase should deepen evidence, and late phase should pressure-test survivors over longer horizons.
- If the explorer is drifting, say so plainly.
- If the controller provides a score target, use it as a believable next stretch goal instead of vague encouragement.
- During exploration phase, do not encourage finish or summary-writing.
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
    profiles_dir: Path
    evals_dir: Path
    notes_dir: Path
    progress_plot_path: Path
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


class ResearchController:
    def __init__(self, app_config: AppConfig):
        self.config = app_config
        self.provider = create_provider(app_config.provider)
        self.supervisor_provider = create_provider(app_config.supervisor_provider)
        self.cli = FuzzfolioCli(app_config.fuzzfolio)
        self.profile_sources: dict[str, Path] = {}
        self.last_created_profile_ref: str | None = None
        self.finish_denials = 0
        self.profile_template_path = self.config.repo_root / "portable_profile_template.json"

    def _system_protocol_text(self, policy: RunPolicy) -> str:
        if policy.allow_finish:
            return SYSTEM_PROTOCOL
        return SYSTEM_PROTOCOL + "\n" + SUPERVISED_EXTRA_RULES

    def _normalize_model_response(self, payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
        if isinstance(payload, dict):
            if isinstance(payload.get("actions"), list):
                reasoning = payload.get("reasoning")
                return {
                    "reasoning": str(reasoning).strip() if isinstance(reasoning, str) else "",
                    "actions": payload.get("actions"),
                }
            if payload.get("tool"):
                reasoning = payload.get("reasoning")
                action = dict(payload)
                action.pop("reasoning", None)
                return {
                    "reasoning": str(reasoning).strip() if isinstance(reasoning, str) else "",
                    "actions": [action],
                }
        if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
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
                raise ValueError("run_cli args list only contained the CLI executable name.")
            return normalized
        if isinstance(args, str) and args.strip():
            command_text = args.strip()
        else:
            command = action.get("command")
            if not isinstance(command, str) or not command.strip():
                raise ValueError("run_cli requires a non-empty args list or command string.")
            command_text = command.strip()
        parts = shlex.split(command_text, posix=False)
        if not parts:
            raise ValueError("run_cli command string did not contain any tokens.")
        first = Path(parts[0]).name.lower()
        if first in executable_names:
            parts = parts[1:]
        if not parts:
            raise ValueError("run_cli command string only contained the CLI executable name.")
        return parts

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
        if args[0] in {"sensitivity", "sensitivity-basket"}:
            effective = self._strip_cli_flag(list(args), "--bar-limit")
            if "--lookback-months" not in effective:
                effective.extend(["--lookback-months", lookback_months])
            return effective
        if command_head == ["deep-replay", "cell-detail"]:
            return self._strip_cli_flag(list(args), "--bar-limit")
        return args

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    def create_run_context(self) -> ToolContext:
        run_id = f"{self._timestamp()}-{self.config.research.label_prefix}-{uuid4().hex[:6]}"
        run_dir = self.config.runs_root / run_id
        profiles_dir = run_dir / "profiles"
        evals_dir = run_dir / "evals"
        notes_dir = run_dir / "notes"
        progress_plot_path = run_dir / "progress.png"
        for path in [profiles_dir, evals_dir, notes_dir]:
            path.mkdir(parents=True, exist_ok=True)
        self.config.latest_run_link.parent.mkdir(parents=True, exist_ok=True)
        self.config.latest_run_link.write_text(str(run_dir.resolve()), encoding="utf-8")
        seed_prompt_path = run_dir / "seed-prompt.json"
        if self.config.research.auto_seed_prompt:
            self.cli.seed_prompt(seed_prompt_path)
        seed_indicator_ids = self._seed_indicator_ids(seed_prompt_path if seed_prompt_path.exists() else None)
        indicator_catalog_summary = self._indicator_catalog_summary(seed_indicator_ids)
        seed_indicator_parameter_hints = self._seed_indicator_parameter_hints(seed_indicator_ids)
        instrument_catalog_summary = self._instrument_catalog_summary()
        return ToolContext(
            run_id=run_id,
            run_dir=run_dir,
            profiles_dir=profiles_dir,
            evals_dir=evals_dir,
            notes_dir=notes_dir,
            progress_plot_path=progress_plot_path,
            seed_prompt_path=seed_prompt_path if seed_prompt_path.exists() else None,
            profile_template_path=self.profile_template_path,
            indicator_catalog_summary=indicator_catalog_summary,
            seed_indicator_parameter_hints=seed_indicator_parameter_hints,
            instrument_catalog_summary=instrument_catalog_summary,
        )

    def _program_text(self) -> str:
        return self.config.program_path.read_text(encoding="utf-8")

    def _seed_text(self, tool_context: ToolContext) -> str:
        if not tool_context.seed_prompt_path or not tool_context.seed_prompt_path.exists():
            return "No seed prompt file exists for this run."
        return tool_context.seed_prompt_path.read_text(encoding="utf-8")

    def _recent_attempts_summary(self) -> str:
        attempts = load_attempts(self.config.attempts_path)
        if not attempts:
            return "No attempts have been logged yet."
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
        return [
            attempt
            for attempt in load_attempts(self.config.attempts_path)
            if str(attempt.get("run_id", "")) == run_id
        ]

    def _render_run_and_global_progress(self, tool_context: ToolContext) -> None:
        all_attempts = load_attempts(self.config.attempts_path)
        run_attempts = [
            attempt
            for attempt in all_attempts
            if str(attempt.get("run_id", "")) == tool_context.run_id
        ]
        render_progress_artifacts(
            run_attempts,
            tool_context.progress_plot_path,
            lower_is_better=self.config.research.plot_lower_is_better,
            mirror_output_path=self.config.progress_plot_path,
            mirror_attempts=all_attempts,
        )

    def _scored_attempts(self, attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [attempt for attempt in attempts if attempt.get("composite_score") is not None]

    def _score_better(self, left: float, right: float) -> bool:
        if self.config.research.plot_lower_is_better:
            return left < right
        return left > right

    def _best_attempt(self, attempts: list[dict[str, Any]]) -> dict[str, Any] | None:
        scored = self._scored_attempts(attempts)
        if not scored:
            return None
        return min(
            scored,
            key=lambda attempt: float(attempt.get("composite_score")),
        ) if self.config.research.plot_lower_is_better else max(
            scored,
            key=lambda attempt: float(attempt.get("composite_score")),
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

    def _run_phase_info(self, step: int, step_limit: int, policy: RunPolicy) -> dict[str, Any]:
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
            late_cutoff = min(max(self.config.research.phase_late_ratio, early_cutoff + 0.05), 0.98)
            if progress < early_cutoff:
                phase_name = "early"
            elif progress < late_cutoff:
                phase_name = "mid"
            else:
                phase_name = "late"
        summaries = {
            "early": (
                f"Early phase: branch broadly, reject weak ideas cheaply, and prioritize fresh contrasts until step {wrap_up_start}."
            ),
            "mid": (
                f"Mid phase: narrow onto the strongest families, deepen evidence, and prefer systematic follow-up over random wandering before wrap-up at step {wrap_up_start}."
            ),
            "late": (
                f"Late phase: stop spraying branches, focus on one or two survivors, and pressure-test them before wrap-up begins at step {wrap_up_start}."
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
        months = int(phase_months.get(phase_name, self.config.research.horizon_mid_months))
        if phase_name == "early":
            rationale = "cheap early screening: test broad branches over a shorter horizon before spending more compute"
            guidance = (
                f"Target about {months} months of evidence. Favor cheap branch-heavy screening and reject weak ideas quickly."
            )
        elif phase_name == "mid":
            rationale = "deepen evidence on the strongest branches before full pressure testing"
            guidance = (
                f"Target about {months} months of evidence. Narrow onto top branches and start validating that the edge persists."
            )
        elif phase_name == "late":
            rationale = "pressure-test one or two survivors over longer history before wrap-up"
            guidance = (
                f"Target about {months} months of evidence. Prefer robustness, portability, and structured follow-up over novelty."
            )
        else:
            rationale = "final validation should use the longest horizon in the session"
            guidance = (
                f"Target about {months} months of evidence. Use the last steps to validate the likely winner over the longest believable horizon."
            )
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
        global_best = self._best_attempt(load_attempts(self.config.attempts_path))

        current_score = (
            float(run_best.get("composite_score"))
            if isinstance(run_best, dict) and run_best.get("composite_score") is not None
            else None
        )
        global_score = (
            float(global_best.get("composite_score"))
            if isinstance(global_best, dict) and global_best.get("composite_score") is not None
            else None
        )

        target_score: float | None = None
        rationale: str
        if current_score is not None and global_score is not None:
            if self._score_better(current_score, global_score):
                delta = max(3.0, abs(current_score) * 0.05)
                target_score = current_score - delta if self.config.research.plot_lower_is_better else current_score + delta
                rationale = "current run already leads the frontier, so the next target is a modest new best"
            else:
                gap = (current_score - global_score) if self.config.research.plot_lower_is_better else (global_score - current_score)
                move = max(3.0, gap * 0.6)
                if self.config.research.plot_lower_is_better:
                    target_score = max(global_score, current_score - move)
                else:
                    target_score = min(global_score, current_score + move)
                rationale = "bridge most of the gap toward the current frontier leader before wrap-up"
        elif current_score is not None:
            delta = max(3.0, abs(current_score) * 0.05)
            target_score = current_score - delta if self.config.research.plot_lower_is_better else current_score + delta
            rationale = "push past the current run leader with one believable improvement"
        elif global_score is not None:
            if self.config.research.plot_lower_is_better:
                target_score = global_score + max(3.0, abs(global_score) * 0.2)
            else:
                target_score = global_score * 0.85 if global_score > 0 else global_score + 3.0
            rationale = "land a first credible point within reach of the current global frontier"
        else:
            rationale = "log the first credible scored candidate before chasing higher targets"

        if target_score is None:
            summary = "Next target: log the first credible scored candidate for this run."
        elif self.config.research.plot_lower_is_better:
            summary = (
                f"Next target: get composite_score <= {self._format_score(target_score)}. "
                f"Current run best={self._format_score(current_score)}; global frontier best={self._format_score(global_score)}."
            )
        else:
            summary = (
                f"Next target: get composite_score >= {self._format_score(target_score)}. "
                f"Current run best={self._format_score(current_score)}; global frontier best={self._format_score(global_score)}."
            )

        return {
            "target_score": target_score,
            "current_run_best_score": current_score,
            "current_run_best_candidate": run_best.get("candidate_name") if isinstance(run_best, dict) else None,
            "global_best_score": global_score,
            "global_best_candidate": global_best.get("candidate_name") if isinstance(global_best, dict) else None,
            "summary": summary,
            "rationale": rationale,
        }

    def _frontier_snapshot_text(self) -> str:
        attempts = load_attempts(self.config.attempts_path)
        valid = [attempt for attempt in attempts if attempt.get("composite_score") is not None]
        if not valid:
            return "No frontier points exist yet."

        frontier, _ = compute_frontier(
            valid,
            lower_is_better=self.config.research.plot_lower_is_better,
        )
        if not frontier:
            return "No frontier points exist yet."

        lines: list[str] = []
        current_best = frontier[-1]
        best_summary = current_best.get("best_summary") if isinstance(current_best.get("best_summary"), dict) else {}
        current_metrics = current_best.get("metrics") if isinstance(current_best.get("metrics"), dict) else {}
        best_cell = best_summary.get("best_cell") if isinstance(best_summary.get("best_cell"), dict) else {}
        positive_ratio = None
        matrix_summary = best_summary.get("matrix_summary") if isinstance(best_summary.get("matrix_summary"), dict) else {}
        if matrix_summary:
            positive_ratio = matrix_summary.get("positive_cell_ratio")

        lines.append("Current best frontier point:")
        lines.append(
            f"- seq={current_best.get('sequence')} score={current_best.get('composite_score')} "
            f"candidate={current_best.get('candidate_name')} profile_ref={current_best.get('profile_ref') or 'n/a'} "
            f"basis={current_best.get('score_basis', 'n/a')} dsr={current_metrics.get('dsr', 'n/a')} "
            f"psr={current_metrics.get('psr', 'n/a')} resolved_trades={best_cell.get('resolved_trades', 'n/a')} "
            f"positive_cell_ratio={positive_ratio if positive_ratio is not None else 'n/a'}"
        )

        lines.append("Recent frontier points:")
        for attempt in frontier[-10:]:
            summary = attempt.get("best_summary") if isinstance(attempt.get("best_summary"), dict) else {}
            metrics = attempt.get("metrics") if isinstance(attempt.get("metrics"), dict) else {}
            cell = summary.get("best_cell") if isinstance(summary.get("best_cell"), dict) else {}
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
                key=lambda attempt: float(attempt.get("composite_score", float("-inf"))),
                reverse=not self.config.research.plot_lower_is_better,
            )
            lines.append("Top scored attempts fallback:")
            for attempt in scored[:5]:
                metrics = attempt.get("metrics") if isinstance(attempt.get("metrics"), dict) else {}
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
        timeframes = data.get("timeframes") if isinstance(data.get("timeframes"), list) else []
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
            indicator_id = str(item.get("id") or item.get("meta", {}).get("id") or "").strip()
            if not indicator_id:
                continue
            meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
            defaults = item.get("configDefaults") if isinstance(item.get("configDefaults"), dict) else {}
            talib_meta = meta.get("talibMeta") if isinstance(meta.get("talibMeta"), list) else []
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
            ranges = defaults.get("ranges") if isinstance(defaults.get("ranges"), dict) else {}
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
        asset_classes = data.get("asset_classes") if isinstance(data.get("asset_classes"), list) else []
        fx_jpy = [str(symbol) for symbol in symbols if isinstance(symbol, str) and symbol.endswith("JPY")]
        return (
            f"Asset classes: {', '.join(str(item) for item in asset_classes)}\n"
            f"JPY-related exact symbols: {', '.join(fx_jpy[:8]) if fx_jpy else 'none'}\n"
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
        for created_file in sorted(tool_context.profiles_dir.glob("*.created.json"))[:24]:
            try:
                payload = json.loads(created_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            profile_ref = str(data.get("id", "")).strip()
            profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
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
        horizon_policy = self._horizon_policy_snapshot(effective_step, effective_step_limit, policy)
        score_target = self._score_target_snapshot(tool_context)
        cli_guide = (
            "Important CLI command shapes:\n"
            '- profiles clone-local --file <ABS_FILE> --out <ABS_FILE>\n'
            '- profiles patch --file <ABS_FILE> --set profile.name="..." --set profile.indicators[0].config.timeframe="H1" --out <ABS_FILE>\n'
            '- profiles scaffold --indicator <ID> --indicator <ID> --instrument <SYMBOL> --out <ABS_FILE>\n'
            '- profiles validate --file <ABS_FILE> --pretty\n'
            '- profiles create --file <ABS_FILE> --out <ABS_FILE>\n'
            '- profiles update --profile-ref <REF> --file <ABS_FILE> --out <ABS_FILE>\n'
            '- sweep submit --definition <ABS_FILE_OR_INLINE_JSON> --out <ABS_FILE> --pretty\n'
            '- sensitivity-basket --profile-ref <REF> --timeframe <TF> --instrument <INSTRUMENT> --lookback-months <MONTHS> --output-dir <ABS_DIR>\n'
            '- compare-sensitivity --input <ABS_DIR> --pretty\n'
            "Notes:\n"
            "- profiles scaffold generates a valid portable profile from live indicator templates and is preferred for fresh candidate bootstrapping.\n"
            "- profiles clone-local normalizes/copies an existing local profile into a fresh portable document for safe local branching.\n"
            "- profiles patch applies deterministic path=value edits to a local profile file and is preferred for small branch mutations.\n"
            "- profiles validate performs a local schema/instrument preflight and is preferred before create when you materially edited a profile.\n"
            "- profiles create/update require --file. They do not accept branch/indicator/timeframe flags.\n"
            "- Create fresh run-owned profile JSON from scaffold output, clone-local output, or the portable template, then call profiles create.\n"
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
            "- Internal artifact fields like bar_limit, effective_bar_limit, and window_truncated are implementation detail. Do not reason about them as strategy.\n"
            "- `__BASKET__` may appear in saved summaries as an aggregate label. Never pass it as --instrument.\n"
            "- Invalid instrument aliases now fail fast with close-match suggestions.\n"
            "- A normal managed run should explore multiple candidates. Do not stop after the first strong score; branch and test at least a few follow-up ideas.\n"
            "- Do not finish the run as soon as the minimum threshold is reached if there is still room in the step budget for a couple more meaningful contrasts.\n"
            "- If a sensitivity run already auto-logged the attempt, avoid redundant log_attempt unless you are recovering from a missing ledger entry.\n"
            "- Use compare-sensitivity when comparing artifact directories or inspecting score details, not as a mandatory step after every successful sensitivity run.\n"
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
            f"Profiles dir: {tool_context.profiles_dir}\n"
            f"Evals dir: {tool_context.evals_dir}\n"
            f"Notes dir: {tool_context.notes_dir}\n"
            f"Attempts ledger: {self.config.attempts_path}\n"
            f"Run progress plot: {tool_context.progress_plot_path}\n"
            f"Global progress plot: {self.config.progress_plot_path}\n"
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
            f"Sticky frontier snapshot:\n{self._frontier_snapshot_text()}\n\n"
            f"Checkpoint summary:\n{checkpoint}\n\n"
            f"Recent attempts:\n{self._recent_attempts_summary()}\n"
            f"\nCLI guide:\n{cli_guide}\n"
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
            return json.dumps({key: value for key, value in action.items() if key != "content"}, ensure_ascii=True)
        return json.dumps({key: value for key, value in action.items() if key != "content"}, ensure_ascii=True)

    def _history_result_summary(self, result: dict[str, Any]) -> dict[str, Any]:
        tool = str(result.get("tool", "unknown"))
        if result.get("error") and tool not in {"yield_guard", "finish"}:
            return {
                "tool": tool,
                "error": str(result.get("error"))[:500],
            }
        if tool == "run_cli":
            payload = result.get("result") if isinstance(result.get("result"), dict) else {}
            summarized: dict[str, Any] = {
                "tool": tool,
                "ok": bool(result.get("ok")),
            }
            if result.get("created_profile_ref"):
                summarized["created_profile_ref"] = result.get("created_profile_ref")
            if result.get("auto_log") is not None:
                summarized["auto_log"] = result.get("auto_log")
            if isinstance(payload, dict):
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
                    summarized["parsed_json_keys"] = [key for key in preview_keys if key in parsed][:8]
                if isinstance(stdout, str) and "Auto-adjusted timeframe from" in stdout:
                    summarized["timeframe_auto_adjusted"] = True
                if isinstance(stderr, str) and stderr.strip() and not bool(result.get("ok")):
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
        if tool in {"yield_guard", "finish"}:
            return result
        return result

    def _validate_action(self, action: Any) -> str | None:
        if not isinstance(action, dict):
            return "Action must be an object."
        tool = str(action.get("tool", "")).strip()
        if not tool:
            return "Action is missing tool."
        if tool not in {"run_cli", "write_file", "read_file", "list_dir", "log_attempt", "finish"}:
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

    def _repair_invalid_response(
        self,
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
                    "Problems:\n"
                    + "\n".join(f"- {error}" for error in errors)
                ),
            ),
        ]
        try:
            repaired = self.provider.complete_json(repair_messages)
            normalized = self._normalize_model_response(repaired)
        except ProviderError:
            return None
        repaired_actions = normalized.get("actions")
        repaired_errors = self._validate_actions(repaired_actions)
        if repaired_errors:
            return None
        return normalized

    def _extract_profile_ref(self, payload: dict[str, Any]) -> str | None:
        if "id" in payload and isinstance(payload["id"], str):
            return payload["id"]
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("id"), str):
            return data["id"]
        return None

    def _resolve_profile_ref_arg(self, value: str) -> str:
        if value.startswith("<") and value.endswith(">") and self.last_created_profile_ref:
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
        if attempt_exists(self.config.attempts_path, artifact_dir):
            attempts = load_attempts(self.config.attempts_path)
            existing = next(
                attempt for attempt in attempts if str(attempt.get("artifact_dir", "")).lower() == str(artifact_dir).lower()
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
            tool_context.run_id,
            artifact_dir,
            score,
            candidate_name=artifact_dir.name,
            profile_ref=profile_ref,
            profile_path=self.profile_sources.get(profile_ref) if profile_ref else None,
            sensitivity_snapshot_path=sensitivity_snapshot_path if sensitivity_snapshot_path.exists() else None,
            note=note,
        )
        append_attempt(self.config.attempts_path, record)
        self._render_run_and_global_progress(tool_context)
        return {
            "status": "logged",
            "attempt_id": record.attempt_id,
            "composite_score": record.composite_score,
            "primary_score": record.primary_score,
            "score_basis": record.score_basis,
            "metrics": record.metrics,
            "artifact_dir": record.artifact_dir,
            "run_progress_plot": str(tool_context.progress_plot_path),
            "progress_plot": str(self.config.progress_plot_path),
            "sensitivity_snapshot_loaded": sensitivity_snapshot is not None,
        }

    def _refresh_progress_artifacts(self, tool_context: ToolContext) -> None:
        self._render_run_and_global_progress(tool_context)

    def _maybe_auto_log_attempt(
        self,
        tool_context: ToolContext,
        args: list[str],
    ) -> dict[str, Any] | None:
        primary = str(args[0]).lower()
        if primary not in {"sensitivity", "sensitivity-basket"} or "--output-dir" not in args:
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
        return self._record_attempt_from_artifact(tool_context, artifact_dir, profile_ref=profile_ref)

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
            if "--profile-ref" in args:
                profile_index = args.index("--profile-ref") + 1
                if profile_index < len(args):
                    args[profile_index] = self._resolve_profile_ref_arg(str(args[profile_index]))
            result = self.cli.run(
                [str(item) for item in args],
                cwd=Path(action["cwd"]) if action.get("cwd") else None,
                check=False,
            )

            profile_ref: str | None = None
            file_arg: Path | None = None
            if result.returncode == 0 and args[:2] in (["profiles", "create"], ["profiles", "update"]):
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

            auto_log = self._maybe_auto_log_attempt(tool_context, args) if result.returncode == 0 else None
            return {
                "tool": "run_cli",
                "ok": result.returncode == 0,
                "created_profile_ref": profile_ref,
                "source_profile_file": str(file_arg) if file_arg else None,
                "result": json.loads(self._serialize_tool_result(result)),
                "auto_log": auto_log,
            }

        if tool == "write_file":
            path = Path(str(action.get("path", ""))).resolve()
            content = action.get("content")
            if not isinstance(content, str):
                raise ValueError("write_file requires string content.")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            return {"tool": "write_file", "path": str(path), "bytes": len(content.encode("utf-8"))}

        if tool == "read_file":
            path = Path(str(action.get("path", ""))).resolve()
            max_chars = int(action.get("max_chars", 6000))
            content = path.read_text(encoding="utf-8")
            return {"tool": "read_file", "path": str(path), "content": content[:max_chars]}

        if tool == "list_dir":
            path = Path(str(action.get("path", ""))).resolve()
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

    def _append_step_log(self, tool_context: ToolContext, payload: dict[str, Any]) -> None:
        path = self._step_log_path(tool_context)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def _load_recent_step_payloads(self, tool_context: ToolContext, limit: int) -> list[dict[str, Any]]:
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
    ) -> str:
        limit = max(1, self.config.research.supervisor_recent_steps)
        payloads = self._load_recent_step_payloads(tool_context, limit)
        if current_step_payload is not None:
            payloads.append(current_step_payload)
        if not payloads:
            return "No recent step history is available."
        lines: list[str] = []
        for payload in payloads[-limit:]:
            step = payload.get("step")
            reasoning = _short = " ".join(str(payload.get("reasoning", "")).split())
            if len(_short) > 180:
                _short = _short[:177] + "..."
            lines.append(f"Step {step}: { _short or 'n/a' }")
            actions = payload.get("actions")
            if isinstance(actions, list):
                for action in actions[:3]:
                    if isinstance(action, dict):
                        lines.append(f"  action: {self._history_action_summary(action)}")
            results = payload.get("results")
            if isinstance(results, list):
                for result in results[:4]:
                    if isinstance(result, dict):
                        summary = self._history_result_summary(result)
                        lines.append(f"  result: {json.dumps(summary, ensure_ascii=True)[:240]}")
        return "\n".join(lines)

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
            f"Frontier snapshot:\n{self._frontier_snapshot_text()}\n\n"
            f"Recent run attempts:\n{chr(10).join(attempt_lines) if attempt_lines else 'No attempts yet.'}\n\n"
            f"Recent step window:\n{self._recent_step_window_text(tool_context, current_step_payload)}\n"
        )
        try:
            payload = self.supervisor_provider.complete_json(
                [
                    ChatMessage(role="system", content=SUPERVISOR_PROMPT),
                    ChatMessage(role="user", content=prompt),
                ]
            )
        except ProviderError:
            return None
        message = payload.get("message")
        questions = payload.get("questions")
        next_moves = payload.get("next_moves")
        if not isinstance(message, str) or not message.strip():
            return None
        return {
            "message": message.strip(),
            "questions": [str(item).strip() for item in questions[:3]] if isinstance(questions, list) else [],
            "next_moves": [str(item).strip() for item in next_moves[:3]] if isinstance(next_moves, list) else [],
        }

    def _checkpoint_messages(self, history_messages: list[ChatMessage]) -> list[ChatMessage]:
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
        try:
            payload = self.provider.complete_json(self._checkpoint_messages(history_messages))
        except ProviderError:
            return messages
        summary = payload.get("checkpoint_summary")
        if not isinstance(summary, str) or not summary.strip():
            return messages

        checkpoint_text = f"{SUMMARY_PREFIX}\n{summary.strip()}"
        self._checkpoint_path(tool_context).write_text(checkpoint_text, encoding="utf-8")

        keep = max(0, self.config.research.compact_keep_recent_messages)
        recent_tail = history_messages[-keep:] if keep else []
        return [
            ChatMessage(role="system", content=self._system_protocol_text(policy)),
            ChatMessage(role="user", content=self._run_state_prompt(tool_context, policy, step=step, step_limit=step_limit)),
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
        trigger = self.config.research.compact_trigger_tokens
        if trigger <= 0:
            return messages
        if self._approx_message_tokens(messages) < trigger:
            return messages
        return self._compact_message_history(messages, tool_context, policy, step, step_limit)

    def _allow_finish(
        self,
        tool_context: ToolContext,
        step: int,
        step_limit: int,
        summary: str,
        policy: RunPolicy,
    ) -> tuple[bool, str]:
        if not policy.allow_finish:
            return False, "Finish is disabled in supervised mode. Keep working until the supervisor stops prompting you."
        if not summary.strip():
            return False, "Do not use finish as a continue marker. Finish is terminal and requires a non-empty summary."
        attempts = self._run_attempts(tool_context.run_id)
        min_attempts_before_finish = min(self.config.research.finish_min_attempts, step_limit)
        phase_info = self._run_phase_info(step, step_limit, policy)
        score_target = self._score_target_snapshot(tool_context)
        if phase_info["name"] != "wrap_up":
            wrap_up_start = phase_info.get("wrap_up_start")
            wrap_up_text = f"Wrap-up begins at step {wrap_up_start}." if wrap_up_start else "Stay in exploration mode."
            return (
                False,
                (
                    f"You are still in {phase_info['name']} phase. {phase_info['summary']} "
                    f"{wrap_up_text} {score_target['summary']}"
                ),
            )
        if len(attempts) >= min_attempts_before_finish:
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
        self.cli.ensure_login()
        tool_context = self.create_run_context()
        self._refresh_progress_artifacts(tool_context)
        effective_step_limit = max_steps or self.config.research.max_steps
        if progress_callback:
            initial_phase = self._run_phase_info(1, effective_step_limit, policy)
            initial_horizon = self._horizon_policy_snapshot(1, effective_step_limit, policy)
            initial_target = self._score_target_snapshot(tool_context)
            progress_callback(
                {
                    "event": "run_started",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "progress_plot": str(self.config.progress_plot_path),
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
                "run_progress_plot": str(tool_context.progress_plot_path),
                "progress_plot": str(self.config.progress_plot_path),
            }
            if progress_callback:
                progress_callback({"event": "window_closed", "result": result})
            return result
        messages: list[ChatMessage] = [
            ChatMessage(role="system", content=self._system_protocol_text(policy)),
            ChatMessage(role="user", content=self._run_state_prompt(tool_context, policy, step=1, step_limit=effective_step_limit)),
        ]

        step_limit = effective_step_limit
        for step in range(1, step_limit + 1):
            self.last_created_profile_ref = None
            messages[1] = ChatMessage(
                role="user",
                content=self._run_state_prompt(tool_context, policy, step=step, step_limit=step_limit),
            )
            if step > 1 and not self._within_operating_window(policy):
                result = {
                    "status": "window_closed",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "progress_plot": str(self.config.progress_plot_path),
                }
                if progress_callback:
                    progress_callback({"event": "window_closed", "result": result})
                return result
            messages = self._maybe_compact_messages(messages, tool_context, policy, step, step_limit)
            try:
                raw_response = self.provider.complete_json(messages)
                response = self._normalize_model_response(raw_response)
            except (ProviderError, CliError) as exc:
                raise RuntimeError(str(exc)) from exc

            actions = response.get("actions")
            reasoning = str(response.get("reasoning", "")).strip()
            validation_errors = self._validate_actions(actions)
            if validation_errors:
                repaired = self._repair_invalid_response(messages, reasoning, actions if isinstance(actions, list) else [], validation_errors)
                if repaired is not None:
                    response = repaired
                    actions = response.get("actions")
                    reasoning = str(response.get("reasoning", "")).strip()
                    validation_errors = self._validate_actions(actions)
            if validation_errors:
                horizon_policy = self._horizon_policy_snapshot(step, step_limit, policy)
                step_payload = {
                    "step": step,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "phase": self._run_phase_info(step, step_limit, policy)["name"],
                    "horizon_target": horizon_policy["summary"],
                    "score_target": self._score_target_snapshot(tool_context)["summary"],
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
            for action in actions:
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
                step_payload["results"].append(result)
                hard_failure = bool(result.get("error"))
                if result.get("tool") == "run_cli" and not bool(result.get("ok", True)):
                    hard_failure = True
                if hard_failure:
                    step_payload["results"].append(
                        {
                            "tool": "step_guard",
                            "message": "Stopped executing remaining actions after the first failed action in this step.",
                        }
                    )
                    break
                if result.get("tool") == "finish":
                    proposed_summary = str(result.get("summary", ""))
                    allow, message = self._allow_finish(tool_context, step, step_limit, proposed_summary, policy)
                    if allow:
                        finished = True
                        finish_summary = proposed_summary
                    else:
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
                            guard_payload["supervisor_message"] = supervisor.get("message")
                            guard_payload["questions"] = supervisor.get("questions", [])
                            guard_payload["next_moves"] = supervisor.get("next_moves", [])
                        step_payload["results"].append(guard_payload)
                    break

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
                            ],
                            ensure_ascii=True,
                        )
                    ),
                )
            )

            if finished:
                return {
                    "status": "finished",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "progress_plot": str(self.config.progress_plot_path),
                    "summary": finish_summary,
                }

        return {
            "status": "step_limit_reached",
            "run_id": tool_context.run_id,
            "run_dir": str(tool_context.run_dir),
            "run_progress_plot": str(tool_context.progress_plot_path),
            "progress_plot": str(self.config.progress_plot_path),
        }
