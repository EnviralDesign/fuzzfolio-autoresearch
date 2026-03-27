from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from autoresearch.config import load_config
    from autoresearch.controller import ResearchController, RunPolicy
    from autoresearch.fuzzfolio import FuzzfolioCli
    from autoresearch.ledger import (
        append_attempt,
        attempts_path_for_run_dir,
        latest_run_dir,
        load_all_run_attempts,
        load_attempts,
        load_run_metadata,
        load_run_attempts,
        make_attempt_record,
        write_attempts,
    )
    from autoresearch.plotting import render_leaderboard_artifacts, render_progress_artifacts
    from autoresearch.provider import ChatMessage, ProviderError, create_provider
    from autoresearch.scoring import build_attempt_score, load_sensitivity_snapshot
else:
    from .config import load_config
    from .controller import ResearchController, RunPolicy
    from .fuzzfolio import FuzzfolioCli
    from .ledger import (
        append_attempt,
        attempts_path_for_run_dir,
        latest_run_dir,
        load_all_run_attempts,
        load_attempts,
        load_run_metadata,
        load_run_attempts,
        make_attempt_record,
        write_attempts,
    )
    from .plotting import render_leaderboard_artifacts, render_progress_artifacts
    from .provider import ChatMessage, ProviderError, create_provider
    from .scoring import build_attempt_score, load_sensitivity_snapshot


console = Console(safe_box=True)
DISPLAY_CONTEXT: dict[str, Path | None] = {"repo_root": None, "run_dir": None}
PLAIN_PROGRESS_MODE = False
DISPLAY_ENCODING = getattr(getattr(console, "file", None), "encoding", None) or "utf-8"
DISPLAY_CHAR_REPLACEMENTS = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2015": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2022": "*",
        "\u2026": "...",
        "\u00a0": " ",
    }
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fuzzfolio autoresearch runtime.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="Verify config, CLI, auth, and seed prompt.")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    provider_test = subparsers.add_parser(
        "test-providers",
        help="Smoke-test configured LLM provider profiles against a few one-shot JSON scenarios.",
    )
    provider_test.add_argument(
        "--profile",
        action="append",
        default=None,
        help="Only test the named provider profile. Can be repeated.",
    )
    provider_test.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    run = subparsers.add_parser("run", help="Run the autonomous research controller.")
    run.add_argument("--max-steps", type=int, default=None)
    run.add_argument("--explorer-profile", default=None, help="Override the configured explorer provider profile for this run.")
    run.add_argument("--supervisor-profile", default=None, help="Override the configured supervisor provider profile for this run.")
    run.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of live console progress.")

    supervise = subparsers.add_parser("supervise", help="Run the supervised controller with config-backed policy defaults.")
    supervise.add_argument("--max-steps", type=int, default=None, help="Per-session step cap before supervise starts a fresh isolated session.")
    supervise.add_argument("--window", default=None, help="Operating window in HH:MM-HH:MM format.")
    supervise.add_argument("--timezone", default=None, help="IANA timezone for the operating window, e.g. America/Chicago.")
    supervise.add_argument("--explorer-profile", default=None, help="Override the configured explorer provider profile for this run.")
    supervise.add_argument("--supervisor-profile", default=None, help="Override the configured supervisor provider profile for this run.")
    supervise.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of live console progress.")

    plot = subparsers.add_parser("plot", help="Generate a run-local or all-runs derived progress plot.")
    plot.add_argument("--run-id", default=None, help="Specific run id to render. Defaults to latest discovered run.")
    plot.add_argument("--all-runs", action="store_true", help="Render a derived aggregate plot across all runs.")
    leaderboard = subparsers.add_parser("leaderboard", help="Generate a derived best-per-run leaderboard image and JSON.")
    leaderboard.add_argument("--limit", type=int, default=15, help="Maximum number of runs to include.")
    subparsers.add_parser("reset-runs", help="Delete all run artifacts and recreate a clean empty runs state.")
    stop_all = subparsers.add_parser(
        "stop-all-runs",
        help="Clear local queued Fuzzfolio research work and optionally stop local autoresearch processes.",
    )
    stop_all.add_argument(
        "--stop-autoresearch",
        action="store_true",
        help="Also stop local autoresearch run/supervise Python processes.",
    )
    stop_all.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    purge_profiles = subparsers.add_parser(
        "purge-cloud-profiles",
        help="Delete saved scoring profiles from the currently configured Fuzzfolio account.",
    )
    purge_profiles.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete the listed cloud profiles. Without this flag the command only performs a dry run.",
    )
    purge_profiles.add_argument(
        "--preview",
        type=int,
        default=10,
        help="How many profiles to include in the preview output.",
    )
    purge_profiles.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    score = subparsers.add_parser("score", help="Score one sensitivity artifact directory.")
    score.add_argument("artifact_dir", type=Path)

    record = subparsers.add_parser("record-attempt", help="Score and append one artifact directory to the attempts ledger.")
    record.add_argument("artifact_dir", type=Path)
    record.add_argument("--candidate-name", default=None)
    record.add_argument("--run-id", default="manual")
    record.add_argument("--profile-ref", default=None)
    record.add_argument("--note", default=None)

    subparsers.add_parser(
        "rescore-attempts",
        help="Recompute scores for the existing attempts ledger using the current scoring config.",
    )

    return parser


def _write_plain_line(text: str) -> None:
    normalized = _short_text(text, limit=2000)
    _write_plain_text(normalized + "\n")


def _write_plain_text(text: str) -> None:
    for stream in (sys.stdout, getattr(sys, "__stdout__", None)):
        if stream is None:
            continue
        try:
            stream.write(text)
            stream.flush()
            return
        except OSError:
            continue


def _use_plain_progress() -> None:
    global PLAIN_PROGRESS_MODE
    PLAIN_PROGRESS_MODE = True


def _safe_render(console_renderer: Any, plain_renderer: Any) -> None:
    if PLAIN_PROGRESS_MODE:
        plain_renderer()
        return
    try:
        console_renderer()
    except OSError:
        _use_plain_progress()
        plain_renderer()


def _render_run_header_plain(event: dict[str, object]) -> None:
    _write_plain_line(
        f"Run {event.get('run_id')} | mode={event.get('mode') or 'run'} | steps={event.get('max_steps')} | dir={_display_path(str(event.get('run_dir')))}"
    )
    horizon_target = event.get("horizon_target")
    if isinstance(horizon_target, str) and horizon_target.strip():
        _write_plain_line(f"Horizon: {horizon_target}")
    score_target = event.get("score_target")
    if isinstance(score_target, str) and score_target.strip():
        _write_plain_line(f"Target: {score_target}")


def _render_step_plain(step_payload: dict[str, Any]) -> None:
    _write_plain_line(f"Step {step_payload.get('step')}: {_short_text(str(step_payload.get('reasoning', '')), 420)}")
    actions = step_payload.get("actions")
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict):
                _write_plain_line(f"  plan: {_summarize_action(action)}")
    results = step_payload.get("results")
    if isinstance(results, list):
        for result in results:
            if isinstance(result, dict):
                _write_plain_line(f"  result: {_summarize_result(result)}")


def _render_run_footer_plain(result: dict[str, object]) -> None:
    _write_plain_line(
        f"Run complete | status={result.get('status')} | run={result.get('run_id')} | dir={_display_path(str(result.get('run_dir')))}"
    )
    summary = result.get("summary")
    if isinstance(summary, str) and summary.strip():
        _write_plain_line(f"Summary: {summary}")


def _print_json_payload(payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=True, indent=2)
    if PLAIN_PROGRESS_MODE:
        _write_plain_text(text + "\n")
        return
    try:
        console.print_json(text)
    except OSError:
        _use_plain_progress()
        _write_plain_text(text + "\n")


def cmd_doctor() -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    cli_path = cli.resolve_executable()
    auth = cli.ensure_login()
    seed = cli.seed_prompt()
    payload = {
        "repo_root": str(config.repo_root),
        "config_path": str(config.config_path),
        "secrets_path": str(config.secrets_path),
        "cli_command": config.fuzzfolio.cli_command,
        "cli_resolved_path": cli_path,
        "explorer_profile": config.llm.explorer_profile,
        "explorer_provider_type": config.provider.provider_type,
        "explorer_model": config.provider.model,
        "explorer_api_base": config.provider.api_base,
        "explorer_command": config.provider.command,
        "explorer_has_api_key": bool(config.provider.api_key),
        "explorer_uses_managed_auth": config.provider.provider_type.strip().lower() == "codex",
        "explorer_compact_trigger_tokens": config.compact_trigger_tokens_for(config.llm.explorer_profile),
        "supervisor_profile": config.llm.supervisor_profile,
        "supervisor_provider_type": config.supervisor_provider.provider_type,
        "supervisor_model": config.supervisor_provider.model,
        "supervisor_api_base": config.supervisor_provider.api_base,
        "supervisor_command": config.supervisor_provider.command,
        "supervisor_has_api_key": bool(config.supervisor_provider.api_key),
        "supervisor_uses_managed_auth": config.supervisor_provider.provider_type.strip().lower() == "codex",
        "supervisor_compact_trigger_tokens": config.compact_trigger_tokens_for(config.llm.supervisor_profile),
        "supervisor_max_steps": config.supervisor.max_steps,
        "supervisor_window_start": config.supervisor.window_start,
        "supervisor_window_end": config.supervisor.window_end,
        "supervisor_timezone": config.supervisor.timezone,
        "supervisor_soft_wrap_minutes": config.supervisor.soft_wrap_minutes,
        "auth_ok": auth.returncode == 0,
        "seed_ok": seed.returncode == 0,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


def _run_powershell_json(script: str) -> Any:
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = (completed.stdout or "").strip()
    if not stdout:
        return None
    return json.loads(stdout)


def _stop_local_autoresearch_processes() -> list[dict[str, Any]]:
    current_pid = os.getpid()
    script = rf"""
$current = {current_pid}
$targets = Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
    Where-Object {{
        $_.ProcessId -ne $current -and (
            $_.CommandLine -like '*autoresearch run*' -or
            $_.CommandLine -like '*autoresearch supervise*'
        )
    }} |
    Select-Object ProcessId, CommandLine
$stopped = @()
foreach ($proc in $targets) {{
    Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    $stopped += [PSCustomObject]@{{
        pid = [int]$proc.ProcessId
        command = [string]$proc.CommandLine
    }}
}}
$stopped | ConvertTo-Json -Depth 4
"""
    payload = _run_powershell_json(script)
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return [payload]


def _fuzzfolio_harness_dir(repo_root: Path) -> Path | None:
    candidate = repo_root.parent / "Trading-Dashboard" / "harness"
    if candidate.exists():
        return candidate
    return None


def _drain_local_fuzzfolio_queues(repo_root: Path) -> dict[str, Any]:
    harness_dir = _fuzzfolio_harness_dir(repo_root)
    if harness_dir is None:
        return {"ok": False, "warning": "Trading-Dashboard harness directory was not found."}

    queue_keys = ["QUEUE:sweep_jobs", "QUEUE:deep_replay_jobs", "QUEUE:sim_jobs"]
    deleted: list[dict[str, Any]] = []
    for key in queue_keys:
        completed = subprocess.run(
            ["uv", "run", "cli.py", "--env", ".env.redis", "redis", "kv", "del", "--key", key],
            cwd=harness_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        stdout = (completed.stdout or "").strip()
        payload = json.loads(stdout) if stdout else {}
        data = payload.get("data") if isinstance(payload, dict) else None
        deleted.append(
            {
                "key": key,
                "deleted": int((data or {}).get("deleted") or 0),
            }
        )
    return {"ok": True, "deleted_keys": deleted}


def cmd_stop_all_runs(*, stop_autoresearch: bool, as_json: bool) -> int:
    config = load_config()
    payload: dict[str, Any] = {}
    if stop_autoresearch:
        payload["stopped_autoresearch_processes"] = _stop_local_autoresearch_processes()
    else:
        payload["stopped_autoresearch_processes"] = {"ok": True, "skipped": True}

    payload["queue_drain"] = _drain_local_fuzzfolio_queues(config.repo_root)
    if not stop_autoresearch:
        payload["note"] = (
            "Only local Fuzzfolio queued work was cleared. "
            "Autoresearch controller processes were left running."
        )

    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    _print_json_payload(payload)
    return 0


def _extract_cloud_profiles(payload: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _profile_preview_row(item: dict[str, Any]) -> dict[str, Any]:
    profile = item.get("profile") if isinstance(item.get("profile"), dict) else {}
    return {
        "id": str(item.get("id") or ""),
        "name": str(profile.get("name") or ""),
        "created_at": item.get("$createdAt"),
        "updated_at": item.get("$updatedAt"),
        "is_active": bool(profile.get("isActive")) if isinstance(profile.get("isActive"), bool) else None,
    }


def cmd_purge_cloud_profiles(*, execute: bool, preview: int, as_json: bool) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    result = cli.run(["profiles", "list", "--pretty"])
    profiles = _extract_cloud_profiles(result.parsed_json)
    preview_items = [_profile_preview_row(item) for item in profiles[: max(0, preview)]]

    payload: dict[str, Any] = {
        "auth_profile": config.fuzzfolio.auth_profile,
        "count": len(profiles),
        "dry_run": not execute,
        "preview": preview_items,
    }

    if not execute:
        payload["message"] = "Dry run only. Re-run with --yes to delete these saved cloud profiles."
        if as_json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            return 0
        _print_json_payload(payload)
        return 0

    deleted: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for item in profiles:
        profile_id = str(item.get("id") or "").strip()
        if not profile_id:
            continue
        try:
            cli.run(["profiles", "delete", "--profile-ref", profile_id, "--pretty"])
            deleted.append({"id": profile_id, "name": str((item.get("profile") or {}).get("name") or "")})
        except Exception as exc:
            failures.append({"id": profile_id, "error": str(exc)})

    payload["deleted_count"] = len(deleted)
    payload["failed_count"] = len(failures)
    payload["deleted_preview"] = deleted[: max(0, preview)]
    if failures:
        payload["failures_preview"] = failures[: max(0, preview)]

    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0 if not failures else 1
    _print_json_payload(payload)
    return 0 if not failures else 1


def _provider_test_scenarios() -> list[tuple[str, list[ChatMessage], Callable[[dict[str, Any]], str | None]]]:
    def validate_minimal(payload: dict[str, Any]) -> str | None:
        if payload.get("probe") != "json_minimal":
            return "expected probe=json_minimal"
        if payload.get("status") != "ok":
            return "expected status=ok"
        if payload.get("value") != 7:
            return "expected value=7"
        return None

    def validate_runtime(payload: dict[str, Any]) -> str | None:
        reasoning = payload.get("reasoning")
        actions = payload.get("actions")
        if not isinstance(reasoning, str) or not reasoning.strip():
            return "expected non-empty reasoning string"
        if not isinstance(actions, list):
            return "expected actions list"
        if actions:
            return "expected empty actions list"
        mode = payload.get("mode")
        if mode != "runtime_shape":
            return "expected mode=runtime_shape"
        return None

    return [
        (
            "json_minimal",
            [
                ChatMessage(
                    role="system",
                    content="Return raw JSON only. No markdown.",
                ),
                ChatMessage(
                    role="user",
                    content=(
                        'Return exactly this JSON object and nothing else: '
                        '{"probe":"json_minimal","status":"ok","value":7}'
                    ),
                ),
            ],
            validate_minimal,
        ),
        (
            "runtime_shape",
            [
                ChatMessage(
                    role="system",
                    content="Return raw JSON only. No markdown.",
                ),
                ChatMessage(
                    role="user",
                    content=(
                        'Return a JSON object with exactly these top-level fields: '
                        '{"mode":"runtime_shape","reasoning":"one short sentence","actions":[]}. '
                        "Keep reasoning non-empty and actions as an empty array."
                    ),
                ),
            ],
            validate_runtime,
        ),
    ]


def cmd_test_providers(
    *,
    profile_names: list[str] | None,
    as_json: bool,
) -> int:
    config = load_config()
    requested = set(profile_names or [])
    selected = {
        name: profile
        for name, profile in config.providers.items()
        if not requested or name in requested
    }
    if requested:
        missing = sorted(requested - set(selected.keys()))
        if missing:
            raise SystemExit(f"Unknown provider profile(s): {', '.join(missing)}")
    scenarios = _provider_test_scenarios()
    results: list[dict[str, Any]] = []
    overall_ok = True

    for profile_name, profile in selected.items():
        provider = create_provider(profile)
        profile_result: dict[str, Any] = {
            "profile": profile_name,
            "provider_type": profile.provider_type,
            "model": profile.model,
            "api_base": profile.api_base,
            "command": profile.command,
            "has_api_key": bool(profile.api_key),
            "uses_managed_auth": profile.provider_type.strip().lower() == "codex",
            "scenarios": [],
            "ok": True,
        }
        for scenario_name, messages, validator in scenarios:
            scenario_result: dict[str, Any] = {"name": scenario_name}
            try:
                payload = provider.complete_json(messages)
                scenario_result["payload"] = payload
                validation_error = validator(payload)
                if validation_error:
                    scenario_result["ok"] = False
                    scenario_result["error"] = validation_error
                    profile_result["ok"] = False
                    overall_ok = False
                else:
                    scenario_result["ok"] = True
            except ProviderError as exc:
                scenario_result["ok"] = False
                scenario_result["error"] = str(exc)
                profile_result["ok"] = False
                overall_ok = False
            profile_result["scenarios"].append(scenario_result)
        results.append(profile_result)

    payload = {"ok": overall_ok, "profiles": results}
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if overall_ok else 1


def _short_text(value: str, limit: int = 220) -> str:
    compact = " ".join(value.split())
    compact = compact.translate(DISPLAY_CHAR_REPLACEMENTS)
    try:
        compact.encode(DISPLAY_ENCODING)
    except UnicodeEncodeError:
        compact = compact.encode(DISPLAY_ENCODING, errors="replace").decode(DISPLAY_ENCODING)
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _set_display_context(*, repo_root: Path | None = None, run_dir: Path | None = None) -> None:
    if repo_root is not None:
        DISPLAY_CONTEXT["repo_root"] = repo_root
    if run_dir is not None:
        DISPLAY_CONTEXT["run_dir"] = run_dir


def _display_path(value: str) -> str:
    path = Path(value)
    run_dir = DISPLAY_CONTEXT.get("run_dir")
    repo_root = DISPLAY_CONTEXT.get("repo_root")
    if run_dir:
        if path == run_dir:
            return str(Path("runs") / run_dir.name)
        try:
            return str(Path("run") / path.relative_to(run_dir))
        except ValueError:
            pass
    if repo_root:
        try:
            return str(path.relative_to(repo_root))
        except ValueError:
            pass
    if path.is_absolute() and len(path.parts) > 4:
        return str(Path(*path.parts[-4:]))
    return str(path)


def _display_value(value: str) -> str:
    if "\\" in value or "/" in value or (":" in value and len(value) > 2):
        return _display_path(value)
    return value


def _parse_window(window_text: str | None) -> tuple[str | None, str | None]:
    if not window_text:
        return None, None
    if "-" not in window_text:
        raise ValueError("Window must be formatted as HH:MM-HH:MM.")
    start, end = (part.strip() for part in window_text.split("-", 1))
    if not start or not end:
        raise ValueError("Window must be formatted as HH:MM-HH:MM.")
    return start, end


def _load_runtime_config(
    *,
    explorer_profile: str | None = None,
    supervisor_profile: str | None = None,
):
    config = load_config()
    effective_explorer = explorer_profile or config.llm.explorer_profile
    effective_supervisor = supervisor_profile or config.llm.supervisor_profile

    missing: list[str] = []
    if effective_explorer not in config.providers:
        missing.append(f"explorer profile {effective_explorer!r}")
    if effective_supervisor not in config.providers:
        missing.append(f"supervisor profile {effective_supervisor!r}")
    if missing:
        raise SystemExit(f"Unknown provider profile override(s): {', '.join(missing)}")

    config.llm.explorer_profile = effective_explorer
    config.llm.supervisor_profile = effective_supervisor
    return config


def _resolve_supervise_policy(
    config,
    *,
    max_steps: int | None,
    window: str | None,
    timezone_name: str | None,
) -> tuple[int, RunPolicy]:
    cfg = config.supervisor
    window_start, window_end = _parse_window(window)
    effective_max_steps = max_steps or cfg.max_steps or config.research.max_steps
    effective_window_start = window_start if window_start is not None else cfg.window_start
    effective_window_end = window_end if window_end is not None else cfg.window_end
    effective_timezone = timezone_name or cfg.timezone
    return effective_max_steps, RunPolicy(
        allow_finish=False,
        window_start=effective_window_start,
        window_end=effective_window_end,
        timezone_name=effective_timezone,
        stop_mode=cfg.stop_mode,
        mode_name="supervise",
        soft_wrap_minutes=cfg.soft_wrap_minutes,
    )


def _parse_wall_time(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def _window_state(policy: RunPolicy) -> tuple[bool, float | None]:
    if not policy.window_start or not policy.window_end:
        return True, None
    tz = ZoneInfo(policy.timezone_name)
    now_local = datetime.now(tz)
    start = _parse_wall_time(policy.window_start)
    end = _parse_wall_time(policy.window_end)
    current = now_local.time().replace(tzinfo=None)
    if start == end:
        return True, None
    if start < end:
        within = start <= current < end
        if not within:
            return False, None
        end_dt = datetime.combine(now_local.date(), end, tz)
    else:
        within = current >= start or current < end
        if not within:
            return False, None
        end_dt = datetime.combine(now_local.date(), end, tz)
        if current >= start:
            end_dt += timedelta(days=1)
    return True, max(0.0, (end_dt - now_local).total_seconds() / 60.0)


def _summarize_action(action: dict[str, object]) -> str:
    tool = str(action.get("tool", "unknown"))
    if tool == "run_cli":
        args = action.get("args")
        if isinstance(args, list) and args:
            return f"run_cli {' '.join(_display_value(str(item)) for item in args[:14])}"
        command = action.get("command")
        if isinstance(command, str) and command.strip():
            return f"run_cli {_short_text(command, 100)}"
    if tool == "write_file":
        path = str(action.get("path", ""))
        return f"write_file {_display_path(path)}"
    if tool == "read_file":
        path = str(action.get("path", ""))
        return f"read_file {_display_path(path)}"
    if tool == "list_dir":
        path = str(action.get("path", ""))
        return f"list_dir {_display_path(path)}"
    if tool == "log_attempt":
        return f"log_attempt {_display_path(str(action.get('artifact_dir', '')))}"
    if tool == "finish":
        return "finish"
    return tool


def _summarize_result(result: dict[str, object]) -> str:
    tool = str(result.get("tool", "unknown"))
    if result.get("error"):
        return f"{tool} failed | {_short_text(str(result.get('error')), 220)}"
    if tool == "run_cli":
        ok = bool(result.get("ok"))
        status = "ok" if ok else "failed"
        parts = [f"run_cli {status}"]
        created_profile_ref = result.get("created_profile_ref")
        if created_profile_ref:
            parts.append(f"profile={created_profile_ref}")
        auto_log = result.get("auto_log")
        if isinstance(auto_log, dict):
            if auto_log.get("status") == "logged":
                parts.append(
                    f"attempt={auto_log.get('attempt_id')} score={auto_log.get('composite_score')}"
                )
            elif auto_log.get("status") == "existing":
                attempt = auto_log.get("attempt")
                if isinstance(attempt, dict):
                    parts.append(f"attempt=existing score={attempt.get('composite_score')}")
        payload = result.get("result")
        if isinstance(payload, dict):
            stdout = payload.get("stdout")
            if isinstance(stdout, str) and "Auto-adjusted timeframe from" in stdout:
                parts.append("timeframe=auto-adjusted")
            stderr = payload.get("stderr")
            if isinstance(stderr, str) and stderr.strip() and not ok:
                parts.append(f"error={_short_text(stderr, 220)}")
        return " | ".join(parts)
    if tool == "write_file":
        path = str(result.get("path", ""))
        return f"write_file ok | {_display_path(path)}"
    if tool == "read_file":
        path = str(result.get("path", ""))
        return f"read_file ok | {_display_path(path)}"
    if tool == "list_dir":
        count = len(result.get("items", [])) if isinstance(result.get("items"), list) else 0
        return f"list_dir ok | items={count}"
    if tool == "log_attempt":
        payload = result.get("result")
        if isinstance(payload, dict):
            if payload.get("status") == "existing":
                attempt = payload.get("attempt")
                if isinstance(attempt, dict):
                    return f"log_attempt existing | score={attempt.get('composite_score')}"
            return f"log_attempt {payload.get('status')} | score={payload.get('composite_score')}"
    if tool == "yield_guard":
        base = str(result.get("supervisor_message") or result.get("message", ""))
        parts = [f"yield_guard | {_short_text(base, 300)}"]
        score_target = result.get("score_target")
        if isinstance(score_target, str) and score_target.strip():
            parts.append("target: " + _short_text(score_target, 140))
        questions = result.get("questions")
        if isinstance(questions, list) and questions:
            parts.append("q: " + " / ".join(_short_text(str(item), 120) for item in questions[:2]))
        next_moves = result.get("next_moves")
        if isinstance(next_moves, list) and next_moves:
            parts.append("next: " + _short_text(str(next_moves[0]), 160))
        return " | ".join(parts)
    if tool == "step_guard":
        return f"step_guard | {_short_text(str(result.get('message', '')), 220)}"
    if tool == "response_guard":
        return f"response_guard | {_short_text(str(result.get('error', '')), 220)}"
    if tool == "finish":
        return f"finish | {_short_text(str(result.get('summary', '')), 240)}"
    return tool


def _result_style(result: dict[str, object]) -> str:
    tool = str(result.get("tool", "unknown"))
    if tool == "yield_guard":
        return "yellow"
    if tool in {"step_guard", "response_guard"}:
        return "bold yellow"
    if result.get("error"):
        return "bold red"
    if tool == "run_cli":
        return "green" if bool(result.get("ok")) else "bold red"
    return "cyan"


def _action_style(action: dict[str, object]) -> str:
    tool = str(action.get("tool", "unknown"))
    if tool == "finish":
        return "magenta"
    if tool == "run_cli":
        return "cyan"
    return "white"


def _render_run_header(event: dict[str, object]) -> None:
    grid = Table.grid(padding=(0, 1))
    grid.add_column(style="bold cyan", justify="right")
    grid.add_column(style="white")
    grid.add_row("Run", str(event.get("run_id")))
    grid.add_row("Mode", str(event.get("mode") or "run"))
    session_index = event.get("session_index")
    if session_index is not None:
        grid.add_row("Session", str(session_index))
    grid.add_row("Steps", str(event.get("max_steps")))
    phase = event.get("phase")
    if isinstance(phase, str) and phase.strip():
        grid.add_row("Phase", phase)
    horizon_target = event.get("horizon_target")
    if isinstance(horizon_target, str) and horizon_target.strip():
        grid.add_row("Horizon", _short_text(horizon_target, 110))
    score_target = event.get("score_target")
    if isinstance(score_target, str) and score_target.strip():
        grid.add_row("Target", _short_text(score_target, 110))
    grid.add_row("Dir", _display_path(str(event.get("run_dir"))))
    attempts_path = event.get("attempts_path")
    if isinstance(attempts_path, str) and attempts_path.strip():
        grid.add_row("Ledger", _display_path(attempts_path))
    grid.add_row("Run Plot", _display_path(str(event.get("run_progress_plot"))))
    _safe_render(
        lambda: console.print(
            Panel(
                grid,
                title="[bold green]Autoresearch Run[/bold green]",
                border_style="green",
                box=box.ROUNDED,
            )
        ),
        lambda: _render_run_header_plain(event),
    )


def _render_step(step_payload: dict[str, Any]) -> None:
    step = step_payload.get("step")
    reasoning = _short_text(str(step_payload.get("reasoning", "")), 420)
    meta_bits: list[str] = []
    phase = step_payload.get("phase")
    if isinstance(phase, str) and phase.strip():
        meta_bits.append(f"phase={phase}")
    horizon_target = step_payload.get("horizon_target")
    if isinstance(horizon_target, str) and horizon_target.strip():
        meta_bits.append(_short_text(horizon_target, 120))
    score_target = step_payload.get("score_target")
    if isinstance(score_target, str) and score_target.strip():
        meta_bits.append(_short_text(score_target, 120))
    panel_body = reasoning
    if meta_bits:
        panel_body = panel_body + "\n\n" + " | ".join(meta_bits)
    reasoning_panel = Panel(
        Text(panel_body, style="white"),
        title=f"[bold blue]Step {step}[/bold blue]",
        border_style="blue",
        box=box.ROUNDED,
    )

    body: list[Any] = [reasoning_panel]

    actions = step_payload.get("actions")
    if isinstance(actions, list) and actions:
        action_table = Table(box=box.SIMPLE_HEAVY, expand=True)
        action_table.add_column("Action", style="bold cyan", width=10)
        action_table.add_column("Detail", style="white")
        for action in actions:
            if isinstance(action, dict):
                action_table.add_row(
                    Text("plan", style=_action_style(action)),
                    Text(_summarize_action(action), style=_action_style(action)),
                )
        body.append(action_table)

    results = step_payload.get("results")
    if isinstance(results, list) and results:
        result_table = Table(box=box.SIMPLE_HEAVY, expand=True)
        result_table.add_column("Result", style="bold", width=10)
        result_table.add_column("Detail", style="white")
        for result in results:
            if isinstance(result, dict):
                style = _result_style(result)
                label = "ok"
                if result.get("error"):
                    label = "error"
                elif str(result.get("tool", "")) == "yield_guard":
                    label = "guard"
                elif str(result.get("tool", "")) == "finish":
                    label = "finish"
                result_table.add_row(Text(label, style=style), Text(_summarize_result(result), style=style))
        body.append(result_table)

    _safe_render(
        lambda: console.print(Group(*body)),
        lambda: _render_step_plain(step_payload),
    )


def _render_run_footer(result: dict[str, object]) -> None:
    grid = Table.grid(padding=(0, 1))
    grid.add_column(style="bold green", justify="right")
    grid.add_column(style="white")
    grid.add_row("Status", str(result.get("status")))
    session_count = result.get("session_count")
    if session_count is not None:
        grid.add_row("Sessions", str(session_count))
    run_id = result.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        grid.add_row("Run", run_id)
    run_dir = result.get("run_dir")
    if isinstance(run_dir, str) and run_dir.strip():
        grid.add_row("Dir", _display_path(run_dir))
    attempts_path = result.get("attempts_path")
    if isinstance(attempts_path, str) and attempts_path.strip():
        grid.add_row("Ledger", _display_path(attempts_path))
    run_plot = result.get("run_progress_plot")
    if isinstance(run_plot, str) and run_plot.strip():
        grid.add_row("Run Plot", _display_path(run_plot))
    summary = result.get("summary")
    if isinstance(summary, str) and summary.strip():
        grid.add_row("Summary", _short_text(summary, 420))
    _safe_render(
        lambda: console.print(
            Panel(
                grid,
                title="[bold green]Run Complete[/bold green]",
                border_style="green",
                box=box.ROUNDED,
            )
        ),
        lambda: _render_run_footer_plain(result),
    )


def _emit_run_progress(event: dict[str, object]) -> None:
    kind = event.get("event")
    if kind == "run_started":
        run_dir = event.get("run_dir")
        if isinstance(run_dir, str):
            _set_display_context(run_dir=Path(run_dir))
        _render_run_header(event)
        return
    if kind == "window_closed":
        result = event.get("result")
        if isinstance(result, dict):
            _render_run_footer(result)
        return
    if kind != "step_completed":
        return
    step_payload = event.get("step_payload")
    if not isinstance(step_payload, dict):
        return
    _render_step(step_payload)


def cmd_run(
    max_steps: int | None,
    *,
    explorer_profile: str | None,
    supervisor_profile: str | None,
    as_json: bool,
) -> int:
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
        supervisor_profile=supervisor_profile,
    )
    _set_display_context(repo_root=config.repo_root, run_dir=None)
    controller = ResearchController(config)
    result = controller.run(
        max_steps=max_steps,
        progress_callback=None if as_json else _emit_run_progress,
        policy=RunPolicy(mode_name="run"),
    )
    if as_json:
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    _render_run_footer(result)
    return 0


def cmd_supervise(
    max_steps: int | None,
    *,
    window: str | None,
    timezone_name: str | None,
    explorer_profile: str | None,
    supervisor_profile: str | None,
    as_json: bool,
) -> int:
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
        supervisor_profile=supervisor_profile,
    )
    _set_display_context(repo_root=config.repo_root, run_dir=None)
    session_max_steps, policy = _resolve_supervise_policy(
        config,
        max_steps=max_steps,
        window=window,
        timezone_name=timezone_name,
    )
    session_results: list[dict[str, Any]] = []
    stop_reason = "window_closed"
    while True:
        within_window, minutes_remaining = _window_state(policy)
        if not within_window:
            stop_reason = "window_closed"
            break
        if session_results and policy.soft_wrap_minutes > 0:
            if minutes_remaining is not None and minutes_remaining <= policy.soft_wrap_minutes:
                stop_reason = "soft_wrap_reached"
                break
        session_index = len(session_results) + 1
        controller = ResearchController(config)

        def emit_progress(event: dict[str, object]) -> None:
            if as_json:
                return
            payload = dict(event)
            if payload.get("event") == "window_closed":
                return
            if payload.get("event") == "run_started":
                payload["session_index"] = session_index
                payload["mode"] = "supervise"
                payload["max_steps"] = session_max_steps
            _emit_run_progress(payload)

        result = controller.run(
            max_steps=session_max_steps,
            progress_callback=None if as_json else emit_progress,
            policy=policy,
        )
        result["session_index"] = session_index
        session_results.append(result)
        if not as_json and result.get("status") == "step_limit_reached":
            rollover_footer = dict(result)
            rollover_footer["summary"] = (
                "This supervised session reached its per-session step cap. "
                "Supervise will start a fresh isolated session if time remains in the outer window."
            )
            _render_run_footer(rollover_footer)
        if result.get("status") != "step_limit_reached":
            stop_reason = str(result.get("status") or "supervise_stopped")
            break

    last_result = session_results[-1] if session_results else {}
    if stop_reason == "soft_wrap_reached":
        summary = (
            f"Completed {len(session_results)} isolated supervise session(s). "
            "The outer supervise window entered soft-wrap territory, so no new session was started."
        )
    elif session_results:
        summary = (
            f"Completed {len(session_results)} isolated supervise session(s). "
            f"Stopped because {stop_reason}."
        )
    else:
        summary = "The supervise window is currently closed, so no session was started."
    result = {
        "status": stop_reason,
        "session_count": len(session_results),
        "sessions": session_results,
        "run_id": last_result.get("run_id"),
        "run_dir": last_result.get("run_dir"),
        "attempts_path": last_result.get("attempts_path"),
        "run_progress_plot": last_result.get("run_progress_plot"),
        "summary": summary,
    }
    if as_json:
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    _render_run_footer(result)
    return 0


def _resolve_run_dir(config, run_id: str | None) -> Path:
    if run_id:
        run_dir = config.runs_root / run_id
        if not run_dir.exists():
            raise SystemExit(f"Run directory does not exist: {run_dir}")
        return run_dir
    run_dir = latest_run_dir(config.runs_root)
    if run_dir is None:
        raise SystemExit("No run directories exist yet.")
    return run_dir


def cmd_plot(*, run_id: str | None, all_runs: bool) -> int:
    config = load_config()
    if all_runs:
        attempts = load_all_run_attempts(config.runs_root)
        output_path = config.aggregate_plot_path
        render_progress_artifacts(
            attempts,
            output_path,
            lower_is_better=config.research.plot_lower_is_better,
        )
        payload = {
            "mode": "all_runs",
            "attempts": len(attempts),
            "plot": str(output_path),
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    run_dir = _resolve_run_dir(config, run_id)
    attempts = load_run_attempts(run_dir)
    output_path = run_dir / "progress.png"
    render_progress_artifacts(
        attempts,
        output_path,
        run_metadata_path=run_dir / "run-metadata.json",
        lower_is_better=config.research.plot_lower_is_better,
    )
    print(
        json.dumps(
            {
                "mode": "run",
                "run_id": run_dir.name,
                "attempts": len(attempts),
                "plot": str(output_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_leaderboard(*, limit: int) -> int:
    config = load_config()
    attempts = load_all_run_attempts(config.runs_root)
    run_metadata_by_run_id = {
        run_dir.name: load_run_metadata(run_dir)
        for run_dir in sorted(path for path in config.runs_root.iterdir() if path.is_dir() and path.name != "derived")
    } if config.runs_root.exists() else {}
    ranked = render_leaderboard_artifacts(
        attempts,
        config.leaderboard_plot_path,
        config.leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
        limit=limit,
    )
    print(
        json.dumps(
            {
                "runs_ranked": len(ranked),
                "leaderboard_plot": str(config.leaderboard_plot_path),
                "leaderboard_json": str(config.leaderboard_json_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_reset_runs() -> int:
    config = load_config()
    cleared: list[str] = []
    blocked: list[dict[str, str]] = []
    config.runs_root.mkdir(parents=True, exist_ok=True)

    for child in sorted(config.runs_root.iterdir()):
        try:
            cleared.append(str(child))
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        except OSError as exc:
            blocked.append({"path": str(child), "error": str(exc)})

    print(
        json.dumps(
            {
                "runs_root": str(config.runs_root),
                "cleared_entries": len(cleared),
                "blocked_entries": blocked,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_score(artifact_dir: Path) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    compare_payload = cli.score_artifact(artifact_dir.resolve())
    snapshot = load_sensitivity_snapshot(artifact_dir.resolve())
    score = build_attempt_score(compare_payload, snapshot)
    print(
        json.dumps(
            {
                "artifact_dir": str(artifact_dir.resolve()),
                "primary_score": score.primary_score,
                "composite_score": score.composite_score,
                "score_basis": score.score_basis,
                "metrics": score.metrics,
                "best_summary": score.best_summary,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_record_attempt(
    artifact_dir: Path,
    candidate_name: str | None,
    run_id: str,
    profile_ref: str | None,
    note: str | None,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    run_dir = config.runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = attempts_path_for_run_dir(run_dir)
    progress_plot_path = run_dir / "progress.png"
    compare_payload = cli.score_artifact(artifact_dir.resolve())
    snapshot_path = artifact_dir.resolve() / "sensitivity-response.json"
    snapshot = load_sensitivity_snapshot(artifact_dir.resolve()) if snapshot_path.exists() else None
    score = build_attempt_score(compare_payload, snapshot)
    record = make_attempt_record(
        config,
        attempts_path,
        run_id,
        artifact_dir.resolve(),
        score,
        candidate_name=candidate_name,
        profile_ref=profile_ref,
        sensitivity_snapshot_path=snapshot_path if snapshot_path.exists() else None,
        note=note,
    )
    append_attempt(attempts_path, record)
    attempts = load_attempts(attempts_path)
    render_progress_artifacts(
        attempts,
        progress_plot_path,
        run_metadata_path=run_dir / "run-metadata.json",
        lower_is_better=config.research.plot_lower_is_better,
    )
    print(
        json.dumps(
            {
                "attempt_id": record.attempt_id,
                "sequence": record.sequence,
                "candidate_name": record.candidate_name,
                "composite_score": record.composite_score,
                "score_basis": record.score_basis,
                "metrics": record.metrics,
                "attempts_path": str(attempts_path),
                "progress_plot": str(progress_plot_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_rescore_attempts() -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    rescored: list[dict[str, object]] = []
    updated = 0
    skipped = 0
    run_count = 0

    if not config.runs_root.exists():
        print(json.dumps({"runs_updated": 0, "updated": 0, "skipped": 0, "attempts": 0}, ensure_ascii=True, indent=2))
        return 0

    for run_dir in sorted(path for path in config.runs_root.iterdir() if path.is_dir() and path.name != "derived"):
        attempts_path = attempts_path_for_run_dir(run_dir)
        attempts = load_attempts(attempts_path)
        if not attempts:
            continue
        run_count += 1
        run_rescored: list[dict[str, object]] = []
        for attempt in attempts:
            artifact_dir = Path(str(attempt.get("artifact_dir", "")))
            if not artifact_dir.exists():
                run_rescored.append(attempt)
                rescored.append(attempt)
                skipped += 1
                continue
            compare_payload = cli.score_artifact(artifact_dir.resolve())
            snapshot = load_sensitivity_snapshot(artifact_dir.resolve())
            score = build_attempt_score(compare_payload, snapshot)
            refreshed = dict(attempt)
            refreshed["primary_score"] = score.primary_score
            refreshed["composite_score"] = score.composite_score
            refreshed["score_basis"] = score.score_basis
            refreshed["metrics"] = score.metrics
            refreshed["best_summary"] = score.best_summary
            run_rescored.append(refreshed)
            rescored.append(refreshed)
            updated += 1
        write_attempts(attempts_path, run_rescored)
        render_progress_artifacts(
            run_rescored,
            run_dir / "progress.png",
            run_metadata_path=run_dir / "run-metadata.json",
            lower_is_better=config.research.plot_lower_is_better,
        )
    print(
        json.dumps(
            {
                "runs_updated": run_count,
                "updated": updated,
                "skipped": skipped,
                "attempts": len(rescored),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "doctor":
        return cmd_doctor()
    if args.command == "test-providers":
        return cmd_test_providers(profile_names=args.profile, as_json=bool(args.json))
    if args.command == "run":
        return cmd_run(
            max_steps=args.max_steps,
            explorer_profile=args.explorer_profile,
            supervisor_profile=args.supervisor_profile,
            as_json=bool(args.json),
        )
    if args.command == "supervise":
        return cmd_supervise(
            max_steps=args.max_steps,
            window=args.window,
            timezone_name=args.timezone,
            explorer_profile=args.explorer_profile,
            supervisor_profile=args.supervisor_profile,
            as_json=bool(args.json),
        )
    if args.command == "plot":
        return cmd_plot(run_id=args.run_id, all_runs=bool(args.all_runs))
    if args.command == "leaderboard":
        return cmd_leaderboard(limit=args.limit)
    if args.command == "reset-runs":
        return cmd_reset_runs()
    if args.command == "stop-all-runs":
        return cmd_stop_all_runs(
            stop_autoresearch=bool(args.stop_autoresearch),
            as_json=bool(args.json),
        )
    if args.command == "purge-cloud-profiles":
        return cmd_purge_cloud_profiles(
            execute=bool(args.yes),
            preview=int(args.preview),
            as_json=bool(args.json),
        )
    if args.command == "score":
        return cmd_score(args.artifact_dir)
    if args.command == "record-attempt":
        return cmd_record_attempt(
            args.artifact_dir,
            args.candidate_name,
            args.run_id,
            args.profile_ref,
            args.note,
        )
    if args.command == "rescore-attempts":
        return cmd_rescore_attempts()
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
