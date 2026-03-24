from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .fuzzfolio import CliError, CommandResult, FuzzfolioCli
from .ledger import append_attempt, attempt_exists, load_attempts, make_attempt_record
from .plotting import render_progress_plot
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
    seed_prompt_path: Path | None


class ResearchController:
    def __init__(self, app_config: AppConfig):
        self.config = app_config
        self.provider = OpenAICompatibleProvider(app_config.provider)
        self.cli = FuzzfolioCli(app_config.fuzzfolio)
        self.profile_sources: dict[str, Path] = {}
        self.finish_denials = 0

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def create_run_context(self) -> ToolContext:
        run_id = f"{self._timestamp()}-{self.config.research.label_prefix}"
        run_dir = self.config.runs_root / run_id
        profiles_dir = run_dir / "profiles"
        evals_dir = run_dir / "evals"
        notes_dir = run_dir / "notes"
        for path in [profiles_dir, evals_dir, notes_dir]:
            path.mkdir(parents=True, exist_ok=True)
        self.config.latest_run_link.parent.mkdir(parents=True, exist_ok=True)
        self.config.latest_run_link.write_text(str(run_dir.resolve()), encoding="utf-8")
        seed_prompt_path = run_dir / "seed-prompt.json"
        if self.config.research.auto_seed_prompt:
            self.cli.seed_prompt(seed_prompt_path)
        return ToolContext(
            run_id=run_id,
            run_dir=run_dir,
            profiles_dir=profiles_dir,
            evals_dir=evals_dir,
            notes_dir=notes_dir,
            seed_prompt_path=seed_prompt_path if seed_prompt_path.exists() else None,
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

    def _checkpoint_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "checkpoint-summary.txt"

    def _step_log_path(self, tool_context: ToolContext) -> Path:
        return tool_context.run_dir / "controller-log.jsonl"

    def _run_state_prompt(self, tool_context: ToolContext) -> str:
        checkpoint_path = self._checkpoint_path(tool_context)
        checkpoint = (
            checkpoint_path.read_text(encoding="utf-8")
            if checkpoint_path.exists()
            else "No checkpoint summary exists yet."
        )
        return (
            f"Repo root: {self.config.repo_root}\n"
            f"Run id: {tool_context.run_id}\n"
            f"Run dir: {tool_context.run_dir}\n"
            f"Profiles dir: {tool_context.profiles_dir}\n"
            f"Evals dir: {tool_context.evals_dir}\n"
            f"Notes dir: {tool_context.notes_dir}\n"
            f"Attempts ledger: {self.config.attempts_path}\n"
            f"Progress plot: {self.config.progress_plot_path}\n"
            f"Program:\n{self._program_text()}\n\n"
            f"Current seed hand:\n{self._seed_text(tool_context)}\n\n"
            f"Checkpoint summary:\n{checkpoint}\n\n"
            f"Recent attempts:\n{self._recent_attempts_summary()}\n"
        )

    def _serialize_tool_result(self, result: Any) -> str:
        if isinstance(result, CommandResult):
            payload = {
                "argv": result.argv,
                "cwd": str(result.cwd),
                "returncode": result.returncode,
                "stdout": result.stdout[:4000],
                "stderr": result.stderr[:2000],
                "parsed_json": result.parsed_json,
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
        render_progress_plot(
            load_attempts(self.config.attempts_path),
            self.config.progress_plot_path,
            lower_is_better=self.config.research.plot_lower_is_better,
        )
        snapshot = load_sensitivity_snapshot(artifact_dir)
        return {
            "status": "logged",
            "attempt_id": record.attempt_id,
            "composite_score": record.composite_score,
            "primary_score": record.primary_score,
            "artifact_dir": record.artifact_dir,
            "progress_plot": str(self.config.progress_plot_path),
            "sensitivity_snapshot_loaded": snapshot is not None,
        }

    def _maybe_auto_log_attempt(
        self,
        tool_context: ToolContext,
        action: dict[str, Any],
    ) -> dict[str, Any] | None:
        args = action.get("args") or []
        if not isinstance(args, list) or not args:
            return None
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
            args = action.get("args")
            if not isinstance(args, list) or not args:
                raise ValueError("run_cli requires a non-empty args list.")
            result = self.cli.run([str(item) for item in args], cwd=Path(action["cwd"]) if action.get("cwd") else None)

            if args[:2] in (["profiles", "create"], ["profiles", "update"]):
                payload = result.parsed_json if isinstance(result.parsed_json, dict) else {}
                profile_ref = self._extract_profile_ref(payload)
                file_arg = None
                if "--file" in args:
                    file_index = args.index("--file") + 1
                    if file_index < len(args):
                        file_arg = Path(str(args[file_index])).resolve()
                if profile_ref and file_arg:
                    self.profile_sources[profile_ref] = file_arg

            auto_log = self._maybe_auto_log_attempt(tool_context, action)
            return {
                "tool": "run_cli",
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

    def _checkpoint_messages(self, tool_context: ToolContext) -> list[ChatMessage]:
        step_log_path = self._step_log_path(tool_context)
        log_tail = step_log_path.read_text(encoding="utf-8")[-12000:] if step_log_path.exists() else ""
        return [
            ChatMessage(role="system", content=COMPACTION_PROMPT),
            ChatMessage(
                role="user",
                content=(
                    f"Run state:\n{self._run_state_prompt(tool_context)}\n\n"
                    f"Recent controller log tail:\n{log_tail}"
                ),
            ),
        ]

    def _refresh_checkpoint(self, tool_context: ToolContext) -> None:
        try:
            payload = self.provider.complete_json(self._checkpoint_messages(tool_context))
        except ProviderError:
            return
        summary = payload.get("checkpoint_summary")
        if isinstance(summary, str) and summary.strip():
            self._checkpoint_path(tool_context).write_text(summary.strip(), encoding="utf-8")

    def _allow_finish(self, step: int) -> tuple[bool, str]:
        attempts = load_attempts(self.config.attempts_path)
        if attempts:
            return True, ""
        if step >= self.config.research.max_steps:
            return True, ""
        return (
            False,
            "Do not yield yet. No attempts have been logged. Keep working until you have produced and logged at least one evaluated candidate or hit the step limit.",
        )

    def run(self, max_steps: int | None = None) -> dict[str, Any]:
        self.cli.ensure_login()
        tool_context = self.create_run_context()
        messages: list[ChatMessage] = [
            ChatMessage(role="system", content=SYSTEM_PROTOCOL),
            ChatMessage(role="user", content=self._run_state_prompt(tool_context)),
        ]

        step_limit = max_steps or self.config.research.max_steps
        for step in range(1, step_limit + 1):
            if step > 1 and step % 8 == 0:
                self._refresh_checkpoint(tool_context)
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
                result = self._execute_action(tool_context, action)
                step_payload["results"].append(result)
                if result.get("tool") == "finish":
                    allow, message = self._allow_finish(step)
                    if allow:
                        finished = True
                        finish_summary = str(result.get("summary", ""))
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
                    "progress_plot": str(self.config.progress_plot_path),
                    "summary": finish_summary,
                }

        return {
            "status": "step_limit_reached",
            "run_id": tool_context.run_id,
            "run_dir": str(tool_context.run_dir),
            "progress_plot": str(self.config.progress_plot_path),
        }
