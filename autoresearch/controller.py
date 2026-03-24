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
from .provider import ChatMessage, OpenAICompatibleProvider, ProviderError
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
- Only update profile refs that were created during this run.
- In profile JSON, `indicator.meta.id` must be an exact id from the sticky indicator catalog. Seed phrases and concept labels are not valid ids.
- After `profiles create`, use the returned profile id for later `--profile-ref` calls. A local `*.created.json` file is not itself a profile ref.
- After `profiles create`, the tool result will surface `created_profile_ref` directly. Use that exact value for the next evaluation step.
- Runtime placeholders like `<created_profile_ref>` may appear in tool arguments. Reuse them exactly when provided; the controller will substitute the real value.
- After `sensitivity-basket`, the expected artifact files are `sensitivity-response.json`, `deep-replay-job.json`, and sometimes `best-cell-path-detail.json`. Do not look for `summary.json`.
- `sensitivity` and `sensitivity-basket` now expose `requested_timeframe` and `effective_timeframe` in JSON output when you inspect stdout or saved responses.
- `finish` is terminal for the whole run. Never use it to mean "continue" or "step complete".
- Only call `finish` when you intend to stop the run now and can provide a concise non-empty final summary.
- This is an iterative research session, not a one-shot evaluation. Keep exploring unless you have reached the step limit or the controller explicitly allows finish.
- A strong result should usually trigger a contrasting follow-up candidate, not immediate finish.
- Even after the minimum exploration threshold is satisfied, prefer using most of the remaining step budget if there are still obvious contrasting branches to test.
- For run_cli, prefer this shape:
  { "tool": "run_cli", "args": ["auth", "whoami", "--pretty"] }
- A legacy string command may also work, but args arrays are preferred.
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
        self.provider = OpenAICompatibleProvider(app_config.provider)
        self.cli = FuzzfolioCli(app_config.fuzzfolio)
        self.profile_sources: dict[str, Path] = {}
        self.last_created_profile_ref: str | None = None
        self.finish_denials = 0
        self.profile_template_path = self.config.repo_root / "portable_profile_template.json"

    def _system_protocol_text(self, policy: RunPolicy) -> str:
        if policy.allow_finish:
            return SYSTEM_PROTOCOL
        return SYSTEM_PROTOCOL + "\n" + SUPERVISED_EXTRA_RULES

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
        indicator_catalog_summary = self._indicator_catalog_summary()
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
                f"score={attempt.get('composite_score')} artifact={attempt.get('artifact_dir')}"
            )
        return "\n".join(lines)

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
        best_cell = best_summary.get("best_cell") if isinstance(best_summary.get("best_cell"), dict) else {}
        positive_ratio = None
        matrix_summary = best_summary.get("matrix_summary") if isinstance(best_summary.get("matrix_summary"), dict) else {}
        if matrix_summary:
            positive_ratio = matrix_summary.get("positive_cell_ratio")

        lines.append("Current best frontier point:")
        lines.append(
            f"- seq={current_best.get('sequence')} score={current_best.get('composite_score')} "
            f"candidate={current_best.get('candidate_name')} profile_ref={current_best.get('profile_ref') or 'n/a'} "
            f"resolved_trades={best_cell.get('resolved_trades', 'n/a')} positive_cell_ratio={positive_ratio if positive_ratio is not None else 'n/a'}"
        )

        lines.append("Recent frontier points:")
        for attempt in frontier[-10:]:
            summary = attempt.get("best_summary") if isinstance(attempt.get("best_summary"), dict) else {}
            cell = summary.get("best_cell") if isinstance(summary.get("best_cell"), dict) else {}
            lines.append(
                f"- seq={attempt.get('sequence')} score={attempt.get('composite_score')} "
                f"candidate={attempt.get('candidate_name')} trades={cell.get('resolved_trades', 'n/a')} "
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
                lines.append(
                    f"- seq={attempt.get('sequence')} score={attempt.get('composite_score')} "
                    f"candidate={attempt.get('candidate_name')} artifact={attempt.get('artifact_dir')}"
                )

        return "\n".join(lines)

    def _indicator_catalog_summary(self) -> str:
        result = self.cli.run(["indicators", "--mode", "index"], check=False)
        if result.returncode != 0 or not isinstance(result.parsed_json, dict):
            return "Indicator catalog snapshot unavailable."
        data = result.parsed_json.get("data")
        if not isinstance(data, dict):
            return "Indicator catalog snapshot unavailable."
        ids = data.get("ids") if isinstance(data.get("ids"), list) else []
        timeframes = data.get("timeframes") if isinstance(data.get("timeframes"), list) else []
        tf_values = [
            str(item.get("value"))
            for item in timeframes
            if isinstance(item, dict) and item.get("value")
        ]
        indicator_preview = ", ".join(str(item) for item in ids) if ids else "unavailable"
        timeframe_preview = ", ".join(tf_values) if tf_values else "unavailable"
        return (
            f"Supported timeframes: {timeframe_preview}\n"
            "Only use exact ids from this catalog in indicator.meta.id. Do not invent ids from seed wording.\n"
            f"Indicator ids: {indicator_preview}"
        )

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

    def _seed_to_catalog_hints_text(self) -> str:
        return (
            "Seed phrase to valid-id hints:\n"
            "- trend strength -> ADX\n"
            "- stochastic trend / stochastic signal trend -> STOCH_TREND or STOCH_CROSSOVER\n"
            "- MACD signal / MACD cross -> MACD_CROSSOVER\n"
            "- MACD histogram momentum -> MACD_HISTOGRAM_PIPS_TREND\n"
            "- volatility filter / breakout volatility -> ATR_VOLATILITY_FILTER\n"
            "- bollinger mean reversion -> BBANDS_POSITION_MEAN_REVERSION\n"
            "- bollinger trend / expansion-style trend proxy -> BBANDS_POSITION_TREND\n"
            "- MA spread trend -> MA_SPREAD_TREND\n"
            "- momentum trend -> MOM_TREND\n"
            "- RSI trend -> RSI_TREND\n"
            "- RSI mean reversion -> RSI_MEAN_REVERSION\n"
            "If a seed phrase is not an exact catalog id, translate it to one of the valid ids above or from the catalog."
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

    def _step_log_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "controller-log.jsonl"

    def _run_state_prompt(self, tool_context: ToolContext, policy: RunPolicy) -> str:
        checkpoint_path = self._checkpoint_path(tool_context)
        checkpoint = (
            checkpoint_path.read_text(encoding="utf-8")
            if checkpoint_path.exists()
            else "No checkpoint summary exists yet."
        )
        cli_guide = (
            "Important CLI command shapes:\n"
            '- profiles create --file <ABS_FILE> --out <ABS_FILE>\n'
            '- profiles update --profile-ref <REF> --file <ABS_FILE> --out <ABS_FILE>\n'
            '- sweep submit --definition <ABS_FILE_OR_INLINE_JSON> --out <ABS_FILE> --pretty\n'
            '- sensitivity-basket --profile-ref <REF> --timeframe <TF> --instrument <INSTRUMENT> --output-dir <ABS_DIR>\n'
            '- compare-sensitivity --input <ABS_DIR> --pretty\n'
            "Notes:\n"
            "- profiles create/update require --file. They do not accept branch/indicator/timeframe flags.\n"
            "- Create fresh run-owned profile JSON from the portable template, then call profiles create.\n"
            "- Only exact indicator ids from the sticky indicator catalog are valid in indicator.meta.id.\n"
            "- The seed prompt is backed by the live indicator catalog, but seed concepts are still ideas, not ids.\n"
            "- Use the seed-to-valid-id hints when the seed uses semantic phrases instead of exact ids.\n"
            "- After profiles create, use the returned data.id as the profile ref for later commands.\n"
            "- The controller also returns created_profile_ref explicitly in the tool result. Prefer that field.\n"
            "- sensitivity and sensitivity-basket accept --pretty when printing JSON to stdout.\n"
            "- sensitivity-basket writes a directory when using --output-dir.\n"
            "- sensitivity-basket may auto-adjust the timeframe down to the profile's lowest active indicator timeframe.\n"
            "- Saved sensitivity responses now include requested_timeframe and effective_timeframe fields.\n"
            "- Invalid instrument aliases now fail fast with close-match suggestions.\n"
            "- A normal managed run should explore multiple candidates. Do not stop after the first strong score; branch and test at least a few follow-up ideas.\n"
            "- Do not finish the run as soon as the minimum threshold is reached if there is still room in the step budget for a couple more meaningful contrasts.\n"
            "- If a sensitivity run already auto-logged the attempt, avoid redundant log_attempt unless you are recovering from a missing ledger entry.\n"
            "- Use compare-sensitivity when comparing artifact directories or inspecting score details, not as a mandatory step after every successful sensitivity run.\n"
            "- Post-eval files are sensitivity-response.json, deep-replay-job.json, and best-cell-path-detail.json when available.\n"
            "- Do not try to read summary.json after sensitivity-basket.\n"
            "- Do not use old saved profiles as candidate seeds for this run.\n"
        )
        return (
            f"Repo root: {self.config.repo_root}\n"
            f"Mode: {policy.mode_name}\n"
            f"Run id: {tool_context.run_id}\n"
            f"Run dir: {tool_context.run_dir}\n"
            "Auth status: already verified by controller at run start.\n"
            f"Allow finish: {policy.allow_finish}\n"
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
            f"Sticky instrument context:\n{tool_context.instrument_catalog_summary or 'Unavailable'}\n\n"
            f"{self._seed_to_catalog_hints_text()}\n\n"
            f"{self._artifact_layout_text()}\n\n"
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
        score = build_attempt_score(compare_payload, self.config.research.adjustments)
        sensitivity_snapshot_path = artifact_dir / "sensitivity-response.json"
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
        render_progress_artifacts(
            load_attempts(self.config.attempts_path),
            tool_context.progress_plot_path,
            lower_is_better=self.config.research.plot_lower_is_better,
            mirror_output_path=self.config.progress_plot_path,
        )
        snapshot = load_sensitivity_snapshot(artifact_dir)
        return {
            "status": "logged",
            "attempt_id": record.attempt_id,
            "composite_score": record.composite_score,
            "primary_score": record.primary_score,
            "artifact_dir": record.artifact_dir,
            "run_progress_plot": str(tool_context.progress_plot_path),
            "progress_plot": str(self.config.progress_plot_path),
            "sensitivity_snapshot_loaded": snapshot is not None,
        }

    def _refresh_progress_artifacts(self, tool_context: ToolContext) -> None:
        render_progress_artifacts(
            load_attempts(self.config.attempts_path),
            tool_context.progress_plot_path,
            lower_is_better=self.config.research.plot_lower_is_better,
            mirror_output_path=self.config.progress_plot_path,
        )

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

    def _execute_action(self, tool_context: ToolContext, action: dict[str, Any]) -> dict[str, Any]:
        tool = action.get("tool")
        if tool == "run_cli":
            args = [
                self._substitute_runtime_placeholders(str(item))
                for item in self._normalize_cli_args(action)
            ]
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

    def _checkpoint_messages(self, tool_context: ToolContext, policy: RunPolicy) -> list[ChatMessage]:
        step_log_path = self._step_log_path(tool_context)
        log_tail = step_log_path.read_text(encoding="utf-8")[-12000:] if step_log_path.exists() else ""
        return [
            ChatMessage(role="system", content=COMPACTION_PROMPT),
            ChatMessage(
                role="user",
                content=(
                    f"Run state:\n{self._run_state_prompt(tool_context, policy)}\n\n"
                    f"Recent controller log tail:\n{log_tail}"
                ),
            ),
        ]

    def _refresh_checkpoint(self, tool_context: ToolContext, policy: RunPolicy) -> None:
        try:
            payload = self.provider.complete_json(self._checkpoint_messages(tool_context, policy))
        except ProviderError:
            return
        summary = payload.get("checkpoint_summary")
        if isinstance(summary, str) and summary.strip():
            self._checkpoint_path(tool_context).write_text(summary.strip(), encoding="utf-8")

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
        attempts = [
            attempt
            for attempt in load_attempts(self.config.attempts_path)
            if str(attempt.get("run_id", "")) == tool_context.run_id
        ]
        min_attempts_before_finish = min(4, step_limit)
        min_step_before_finish = min(step_limit, max(6, step_limit - 2))
        if len(attempts) >= min_attempts_before_finish and step >= min_step_before_finish:
            return True, ""
        if step >= step_limit:
            return True, ""
        return (
            False,
            (
                "Do not finish yet. This run should explore multiple candidates before stopping. "
                f"Keep working until you have logged at least {min_attempts_before_finish} evaluated candidates "
                f"and reached about step {min_step_before_finish}, or hit the step limit."
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
        if progress_callback:
            progress_callback(
                {
                    "event": "run_started",
                    "run_id": tool_context.run_id,
                    "run_dir": str(tool_context.run_dir),
                    "run_progress_plot": str(tool_context.progress_plot_path),
                    "progress_plot": str(self.config.progress_plot_path),
                    "max_steps": max_steps or self.config.research.max_steps,
                    "mode": policy.mode_name,
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
            ChatMessage(role="user", content=self._run_state_prompt(tool_context, policy)),
        ]

        step_limit = max_steps or self.config.research.max_steps
        for step in range(1, step_limit + 1):
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
            if step > 1 and step % 8 == 0:
                self._refresh_checkpoint(tool_context, policy)
            try:
                response = self.provider.complete_json(messages)
            except (ProviderError, CliError) as exc:
                raise RuntimeError(str(exc)) from exc

            actions = response.get("actions")
            reasoning = str(response.get("reasoning", "")).strip()
            if not isinstance(actions, list) or not actions:
                raise RuntimeError(f"Model returned invalid actions payload: {response}")
            if len(actions) > 3:
                raise RuntimeError(f"Model returned too many actions in one step: {len(actions)}")

            step_payload: dict[str, Any] = {
                "step": step,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "reasoning": reasoning,
                "actions": actions,
                "results": [],
            }

            finished = False
            finish_summary = ""
            for action in actions:
                try:
                    result = self._execute_action(tool_context, action)
                except Exception as exc:
                    result = {
                        "tool": str(action.get("tool", "unknown")),
                        "ok": False,
                        "error": str(exc),
                    }
                step_payload["results"].append(result)
                if result.get("tool") == "finish":
                    proposed_summary = str(result.get("summary", ""))
                    allow, message = self._allow_finish(tool_context, step, step_limit, proposed_summary, policy)
                    if allow:
                        finished = True
                        finish_summary = proposed_summary
                    else:
                        self.finish_denials += 1
                        step_payload["results"].append(
                            {
                                "tool": "yield_guard",
                                "message": message,
                                "finish_denials": self.finish_denials,
                            }
                        )
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
            messages.append(ChatMessage(role="assistant", content=json.dumps(response, ensure_ascii=True)))
            messages.append(
                ChatMessage(
                    role="user",
                    content="Tool results:\n" + json.dumps(step_payload["results"], ensure_ascii=True),
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
