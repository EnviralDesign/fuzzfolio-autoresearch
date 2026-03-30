from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time as pytime
from datetime import datetime, time, timedelta
from math import ceil
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from autoresearch.config import load_config
    from autoresearch.controller import ResearchController, RunPolicy
    from autoresearch.dashboard import serve_dashboard
    from autoresearch.fuzzfolio import CliError, FuzzfolioCli
    from autoresearch.ledger import (
        append_attempt,
        attempts_path_for_run_dir,
        list_run_dirs,
        latest_run_dir,
        load_all_run_attempts,
        load_attempts,
        load_run_metadata,
        load_run_attempts,
        make_attempt_record,
        write_attempts,
    )
    from autoresearch.plotting import (
        _attempt_effective_window_months,
        _attempt_trade_count,
        _attempt_trades_per_month,
        _best_scored_attempts_by_run,
        render_leaderboard_artifacts,
        render_model_leaderboard_artifacts,
        render_progress_artifacts,
        render_similarity_heatmap_artifacts,
        render_similarity_scatter_artifacts,
        render_tradeoff_leaderboard_artifacts,
        render_validation_delta_artifacts,
        render_validation_scatter_artifacts,
    )
    from autoresearch.provider import ChatMessage, ProviderError, create_provider
    from autoresearch.scoring import build_attempt_score, load_sensitivity_snapshot
else:
    from .config import load_config
    from .controller import ResearchController, RunPolicy
    from .dashboard import serve_dashboard
    from .fuzzfolio import CliError, FuzzfolioCli
    from .ledger import (
        append_attempt,
        attempts_path_for_run_dir,
        list_run_dirs,
        latest_run_dir,
        load_all_run_attempts,
        load_attempts,
        load_run_metadata,
        load_run_attempts,
        make_attempt_record,
        write_attempts,
    )
    from .plotting import (
        _attempt_effective_window_months,
        _attempt_trade_count,
        _attempt_trades_per_month,
        _best_scored_attempts_by_run,
        render_leaderboard_artifacts,
        render_model_leaderboard_artifacts,
        render_progress_artifacts,
        render_similarity_heatmap_artifacts,
        render_similarity_scatter_artifacts,
        render_tradeoff_leaderboard_artifacts,
        render_validation_delta_artifacts,
        render_validation_scatter_artifacts,
    )
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


class _SafeTextStream:
    def __init__(self, stream: Any):
        self._stream = stream

    def write(self, data: str) -> int:
        try:
            return self._stream.write(data)
        except OSError:
            return 0

    def flush(self) -> None:
        try:
            self._stream.flush()
        except OSError:
            return

    def writelines(self, lines: Any) -> None:
        for line in lines:
            self.write(line)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


def _install_safe_std_streams() -> None:
    for attr_name in ("stdout", "stderr", "__stdout__", "__stderr__"):
        stream = getattr(sys, attr_name, None)
        if stream is None or isinstance(stream, _SafeTextStream):
            continue
        setattr(sys, attr_name, _SafeTextStream(stream))


_install_safe_std_streams()


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
    run.add_argument("--advisor-profile", action="append", default=None, help="Override advisor provider profiles for this run. Can be repeated.")
    run.add_argument("--advisor-every", type=int, default=None, help="Inject advisor guidance every N steps.")
    run.add_argument("--no-advisor", action="store_true", help="Disable periodic advisor guidance for this run.")
    run.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of live console progress.")
    run.add_argument("--plain-progress", action="store_true", help="Use plain line-oriented progress output instead of Rich panels.")

    supervise = subparsers.add_parser("supervise", help="Run the supervised controller with config-backed policy defaults.")
    supervise.add_argument("--max-steps", type=int, default=None, help="Per-session step cap before supervise starts a fresh isolated session.")
    supervise.add_argument("--window", default=None, help="Operating window in HH:MM-HH:MM format.")
    supervise.add_argument("--no-window", action="store_true", help="Disable supervise windowing and run sessions around the clock.")
    supervise.add_argument("--timezone", default=None, help="IANA timezone for the operating window, e.g. America/Chicago.")
    supervise.add_argument("--explorer-profile", default=None, help="Override the configured explorer provider profile for this run.")
    supervise.add_argument("--supervisor-profile", default=None, help="Override the configured supervisor provider profile for this run.")
    supervise.add_argument("--advisor-profile", action="append", default=None, help="Override advisor provider profiles for this supervise session. Can be repeated.")
    supervise.add_argument("--advisor-every", type=int, default=None, help="Inject advisor guidance every N steps.")
    supervise.add_argument("--no-advisor", action="store_true", help="Disable periodic advisor guidance for this supervise session.")
    supervise.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of live console progress.")
    supervise.add_argument("--plain-progress", action="store_true", help="Use plain line-oriented progress output instead of Rich panels.")

    plot = subparsers.add_parser("plot", help="Generate a run-local or all-runs derived progress plot.")
    plot.add_argument("--run-id", default=None, help="Specific run id to render. Defaults to latest discovered run.")
    plot.add_argument("--all-runs", action="store_true", help="Render a derived aggregate plot across all runs.")
    leaderboard = subparsers.add_parser("leaderboard", help="Generate a derived best-per-run leaderboard image and JSON.")
    leaderboard.add_argument(
        "--limit",
        type=int,
        default=15,
        help="Maximum number of runs to show in the classic bar leaderboard. Validation and similarity analyze the full best-per-run set.",
    )
    leaderboard.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Ignore cached validation artifacts and rebuild all derived validation/similarity inputs.",
    )
    dashboard = subparsers.add_parser("dashboard", help="Serve a local SPA for run, leaderboard, and backtest drilldown.")
    dashboard.add_argument("--host", default="127.0.0.1", help="Bind host. Default: 127.0.0.1")
    dashboard.add_argument("--port", type=int, default=47832, help="Bind port. Default: 47832")
    dashboard.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Classic bar leaderboard display limit used during refresh. Validation and similarity still analyze the full best-per-run set. Default: 25",
    )
    dashboard.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Ignore cached validation artifacts when refreshing derived dashboard data.",
    )
    dashboard.add_argument(
        "--no-refresh-on-start",
        action="store_true",
        help="Serve immediately using current derived artifacts instead of rebuilding them on startup.",
    )
    profile_drop_pngs = subparsers.add_parser(
        "sync-profile-drop-pngs",
        help="Rebuild run-local profile-drop PNGs for each run's best scored attempt.",
    )
    profile_drop_pngs.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only process the named run id. Can be repeated.",
    )
    profile_drop_pngs.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary package bundles under each run directory instead of deleting them after a successful render.",
    )
    profile_drop_pngs.add_argument(
        "--lookback-months",
        type=int,
        default=12,
        help="Fixed deep-replay lookback window in months for rebuilt profile-drop cards. Default: 12.",
    )
    profile_drop_pngs.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Ignore existing profile-drop PNG/manifests and rerender every requested horizon.",
    )
    profile_drop_pngs.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    subparsers.add_parser("reset-runs", help="Delete all run artifacts and recreate a clean empty runs state.")
    prune_runs = subparsers.add_parser(
        "prune-runs",
        help="Delete low-signal run directories, such as smoke tests or early dead runs.",
    )
    prune_runs.add_argument(
        "--min-mapped-points",
        type=int,
        default=2,
        help="Keep runs with at least this many mapped points (scored attempts). Default: 2.",
    )
    prune_runs.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete the matched runs. Without this flag the command only performs a dry run.",
    )
    prune_runs.add_argument(
        "--preview",
        type=int,
        default=20,
        help="How many matched runs to include in the preview output.",
    )
    prune_runs.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
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


def _set_plain_progress_mode(enabled: bool) -> None:
    global PLAIN_PROGRESS_MODE
    PLAIN_PROGRESS_MODE = bool(enabled)


def _plain_separator(label: str | None = None) -> str:
    width = 78
    if not label:
        return "-" * width
    compact = _short_text(label, 28)
    decorated = f"---- {compact} "
    return decorated + ("-" * max(0, width - len(decorated)))


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
    _write_plain_line(_plain_separator("Autoresearch Run"))
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
    _write_plain_line(_plain_separator(f"Step {step_payload.get('step')}"))
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
    _write_plain_line(_plain_separator("Run Complete"))
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
        "supervisor_window_enabled": config.supervisor.window_enabled,
        "supervisor_window_start": config.supervisor.window_start,
        "supervisor_window_end": config.supervisor.window_end,
        "supervisor_timezone": config.supervisor.timezone,
        "supervisor_soft_wrap_minutes": config.supervisor.soft_wrap_minutes,
        "supervisor_auto_restart_terminal_sessions": config.supervisor.auto_restart_terminal_sessions,
        "advisor_enabled": config.advisor.enabled,
        "advisor_every_n_steps": config.advisor.every_n_steps,
        "advisor_profiles": config.advisor.profiles,
        "advisor_max_recent_steps": config.advisor.max_recent_steps,
        "advisor_max_recent_attempts": config.advisor.max_recent_attempts,
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
        except (CliError, OSError, ValueError, json.JSONDecodeError) as exc:
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


def _mapped_point_count(attempts: list[dict[str, Any]]) -> int:
    return sum(1 for attempt in attempts if attempt.get("composite_score") is not None)


def cmd_prune_runs(
    *,
    min_mapped_points: int,
    execute: bool,
    preview: int,
    as_json: bool,
) -> int:
    config = load_config()
    runs = list_run_dirs(config.runs_root)
    matched: list[dict[str, Any]] = []
    for run_dir in runs:
        attempts = load_run_attempts(run_dir)
        mapped_points = _mapped_point_count(attempts)
        if mapped_points >= min_mapped_points:
            continue
        matched.append(
            {
                "run_id": run_dir.name,
                "run_dir": str(run_dir),
                "logged_attempts": len(attempts),
                "mapped_points": mapped_points,
            }
        )

    payload: dict[str, Any] = {
        "runs_root": str(config.runs_root),
        "min_mapped_points": int(min_mapped_points),
        "total_runs": len(runs),
        "matched_runs": len(matched),
        "dry_run": not execute,
        "preview": matched[: max(0, preview)],
    }

    if not execute:
        payload["message"] = (
            "Dry run only. Re-run with --yes to delete the matched low-signal runs."
        )
        if as_json:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            return 0
        _print_json_payload(payload)
        return 0

    deleted: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for item in matched:
        run_dir = Path(str(item["run_dir"]))
        try:
            shutil.rmtree(run_dir)
            deleted.append(item)
        except OSError as exc:
            blocked.append(
                {
                    "run_id": item["run_id"],
                    "run_dir": item["run_dir"],
                    "error": str(exc),
                }
            )

    payload["deleted_runs"] = len(deleted)
    payload["blocked_runs"] = len(blocked)
    payload["deleted_preview"] = deleted[: max(0, preview)]
    if blocked:
        payload["blocked_preview"] = blocked[: max(0, preview)]

    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0 if not blocked else 1
    _print_json_payload(payload)
    return 0 if not blocked else 1


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
    advisor_profiles: list[str] | None = None,
    advisor_every: int | None = None,
    no_advisor: bool = False,
):
    config = load_config()
    effective_explorer = explorer_profile or config.llm.explorer_profile
    effective_supervisor = supervisor_profile or config.llm.supervisor_profile
    effective_advisors = list(config.advisor.profiles)
    if advisor_profiles is not None:
        effective_advisors = []
        seen: set[str] = set()
        for item in advisor_profiles:
            token = str(item or "").strip()
            if not token or token in seen:
                continue
            seen.add(token)
            effective_advisors.append(token)

    missing: list[str] = []
    if effective_explorer not in config.providers:
        missing.append(f"explorer profile {effective_explorer!r}")
    if effective_supervisor not in config.providers:
        missing.append(f"supervisor profile {effective_supervisor!r}")
    for advisor_profile in effective_advisors:
        if advisor_profile not in config.providers:
            missing.append(f"advisor profile {advisor_profile!r}")
    if missing:
        raise SystemExit(f"Unknown provider profile override(s): {', '.join(missing)}")

    config.llm.explorer_profile = effective_explorer
    config.llm.supervisor_profile = effective_supervisor
    config.advisor.profiles = effective_advisors
    if advisor_every is not None:
        config.advisor.every_n_steps = int(advisor_every)
        if advisor_every > 0:
            config.advisor.enabled = True
    if advisor_profiles is not None:
        config.advisor.enabled = bool(effective_advisors)
    if no_advisor:
        config.advisor.enabled = False
    if config.advisor.enabled and not config.advisor.profiles:
        raise SystemExit("Advisor guidance is enabled but no advisor profiles are configured.")
    return config


def _resolve_supervise_policy(
    config,
    *,
    max_steps: int | None,
    window: str | None,
    no_window: bool,
    timezone_name: str | None,
) -> tuple[int, RunPolicy]:
    cfg = config.supervisor
    window_start, window_end = _parse_window(window)
    effective_max_steps = max_steps or cfg.max_steps or config.research.max_steps
    window_enabled = bool(cfg.window_enabled) and not no_window
    effective_window_start = None if not window_enabled else (window_start if window_start is not None else cfg.window_start)
    effective_window_end = None if not window_enabled else (window_end if window_end is not None else cfg.window_end)
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
    if tool == "advisor_guidance":
        advisors = result.get("advisors")
        profiles = []
        if isinstance(advisors, list):
            for item in advisors[:3]:
                if isinstance(item, dict):
                    label = str(item.get("label") or "").strip()
                    if label:
                        profiles.append(label)
        parts = ["advisor_guidance"]
        if profiles:
            parts.append("profiles=" + ", ".join(profiles))
        summary = str(result.get("message", "")).strip()
        if summary:
            parts.append(_short_text(summary, 200))
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
    if tool == "advisor_guidance":
        return "magenta"
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
    advisor_profiles: list[str] | None,
    advisor_every: int | None,
    no_advisor: bool,
    as_json: bool,
    plain_progress: bool,
) -> int:
    _set_plain_progress_mode(plain_progress and not as_json)
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
        supervisor_profile=supervisor_profile,
        advisor_profiles=advisor_profiles,
        advisor_every=advisor_every,
        no_advisor=no_advisor,
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
    no_window: bool,
    timezone_name: str | None,
    explorer_profile: str | None,
    supervisor_profile: str | None,
    advisor_profiles: list[str] | None,
    advisor_every: int | None,
    no_advisor: bool,
    as_json: bool,
    plain_progress: bool,
) -> int:
    _set_plain_progress_mode(plain_progress and not as_json)
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
        supervisor_profile=supervisor_profile,
        advisor_profiles=advisor_profiles,
        advisor_every=advisor_every,
        no_advisor=no_advisor,
    )
    _set_display_context(repo_root=config.repo_root, run_dir=None)
    session_max_steps, policy = _resolve_supervise_policy(
        config,
        max_steps=max_steps,
        window=window,
        no_window=no_window,
        timezone_name=timezone_name,
    )
    auto_restart_terminal = bool(config.supervisor.auto_restart_terminal_sessions)
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

        try:
            result = controller.run(
                max_steps=session_max_steps,
                progress_callback=None if as_json else emit_progress,
                policy=policy,
            )
        except Exception as exc:
            result = {
                "status": "session_error",
                "run_id": None,
                "run_dir": None,
                "attempts_path": None,
                "run_progress_plot": None,
                "summary": str(exc),
                "error": str(exc),
            }
        result["session_index"] = session_index
        session_results.append(result)
        if not as_json and result.get("status") == "step_limit_reached":
            rollover_footer = dict(result)
            rollover_footer["summary"] = (
                "This supervised session reached its per-session step cap. "
                "Supervise will start a fresh isolated session if time remains in the outer window."
            )
            _render_run_footer(rollover_footer)
        elif not as_json and result.get("status") == "session_error":
            error_footer = dict(result)
            if auto_restart_terminal:
                error_footer["summary"] = (
                    "This supervised session failed, but terminal-session auto-restart is enabled. "
                    "Supervise will start a fresh isolated session if time remains in the outer window."
                )
            _render_run_footer(error_footer)
        elif (
            not as_json
            and auto_restart_terminal
            and result.get("status") not in {"window_closed", "step_limit_reached"}
        ):
            restart_footer = dict(result)
            restart_footer["summary"] = (
                "This supervised session ended normally, but terminal-session auto-restart is enabled. "
                "Supervise will start a fresh isolated session if time remains in the outer window."
            )
            _render_run_footer(restart_footer)

        status = str(result.get("status") or "supervise_stopped")
        if status == "step_limit_reached":
            continue
        if auto_restart_terminal and status not in {"window_closed"}:
            pytime.sleep(2.0)
            continue
        stop_reason = status
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


def cmd_leaderboard(*, limit: int, force_rebuild: bool) -> int:
    config = load_config()
    def emit(message: str) -> None:
        _write_plain_line(message)

    emit("leaderboard: loading run attempts")
    attempts = load_all_run_attempts(config.runs_root)
    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()
    emit(f"leaderboard: loaded {len(attempts)} attempts")
    run_metadata_by_run_id = {
        run_dir.name: load_run_metadata(run_dir)
        for run_dir in sorted(path for path in config.runs_root.iterdir() if path.is_dir() and path.name != "derived")
    } if config.runs_root.exists() else {}
    emit("leaderboard: rendering best-per-run leaderboard")
    ranked = render_leaderboard_artifacts(
        attempts,
        config.leaderboard_plot_path,
        config.leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
        limit=limit,
    )
    analysis_ranked = sorted(
        _best_scored_attempts_by_run(
            attempts,
            lower_is_better=config.research.plot_lower_is_better,
        ),
        key=lambda attempt: float(attempt.get("composite_score")),
        reverse=not config.research.plot_lower_is_better,
    )
    emit("leaderboard: rendering model averages")
    model_ranked = render_model_leaderboard_artifacts(
        attempts,
        config.model_leaderboard_plot_path,
        config.model_leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
    )
    emit("leaderboard: rendering tradeoff map")
    tradeoff_ranked = render_tradeoff_leaderboard_artifacts(
        attempts,
        config.tradeoff_leaderboard_plot_path,
        config.tradeoff_leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
    )
    emit(f"leaderboard: validating {len(analysis_ranked)} best-per-run leaders at 12mo and 36mo")
    validation_rows = _build_validation_rows(
        config=config,
        cli=cli,
        ranked_attempts=analysis_ranked,
        run_metadata_by_run_id=run_metadata_by_run_id,
        force_rebuild=force_rebuild,
        emit=emit,
    )
    skipped_validation_rows = max(0, len(analysis_ranked) - len(validation_rows))
    if skipped_validation_rows:
        emit(f"leaderboard: skipped {skipped_validation_rows} validation candidate(s) after recoverable errors")
    emit("leaderboard: rendering validation scatter")
    validation_ranked = render_validation_scatter_artifacts(
        validation_rows,
        config.validation_scatter_plot_path,
        config.validation_leaderboard_json_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    emit("leaderboard: rendering validation delta")
    render_validation_delta_artifacts(
        validation_rows,
        config.validation_delta_plot_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    emit("leaderboard: computing 36mo similarity payload")
    similarity_payload = _build_similarity_payload(validation_rows)
    emit("leaderboard: rendering similarity heatmap")
    similarity_rendered = render_similarity_heatmap_artifacts(
        similarity_payload,
        config.similarity_heatmap_plot_path,
        config.similarity_leaderboard_json_path,
    )
    emit("leaderboard: rendering score-vs-sameness map")
    similarity_leaders = render_similarity_scatter_artifacts(
        similarity_payload,
        config.similarity_scatter_plot_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    emit("leaderboard: done")
    print(
        json.dumps(
            {
                "runs_ranked": len(ranked),
                "analysis_runs_ranked": len(analysis_ranked),
                "leaderboard_plot": str(config.leaderboard_plot_path),
                "leaderboard_json": str(config.leaderboard_json_path),
                "models_ranked": len(model_ranked),
                "model_leaderboard_plot": str(config.model_leaderboard_plot_path),
                "model_leaderboard_json": str(config.model_leaderboard_json_path),
                "tradeoff_runs_ranked": len(tradeoff_ranked),
                "tradeoff_leaderboard_plot": str(config.tradeoff_leaderboard_plot_path),
                "tradeoff_leaderboard_json": str(config.tradeoff_leaderboard_json_path),
                "validation_rows": len(validation_ranked),
                "validation_skipped": skipped_validation_rows,
                "validation_leaderboard_json": str(config.validation_leaderboard_json_path),
                "validation_scatter_plot": str(config.validation_scatter_plot_path),
                "validation_delta_plot": str(config.validation_delta_plot_path),
                "similarity_leaders": len(similarity_leaders),
                "similarity_pairs": len(similarity_rendered.get("pairs") or []),
                "similarity_leaderboard_json": str(config.similarity_leaderboard_json_path),
                "similarity_heatmap_plot": str(config.similarity_heatmap_plot_path),
                "similarity_scatter_plot": str(config.similarity_scatter_plot_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_dashboard(*, host: str, port: int, limit: int, refresh_on_start: bool, force_rebuild: bool) -> int:
    config = load_config()
    serve_dashboard(
        config,
        host=host,
        port=port,
        limit=limit,
        refresh_on_start=refresh_on_start,
        force_rebuild=force_rebuild,
    )
    return 0


def _trading_dashboard_roots(config) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    raw_candidates = [
        config.fuzzfolio.workspace_root,
        config.repo_root.parent / "Trading-Dashboard",
    ]
    for candidate in raw_candidates:
        if candidate is None:
            continue
        resolved = candidate.resolve()
        key = str(resolved).lower()
        if key in seen or not resolved.exists():
            continue
        seen.add(key)
        candidates.append(resolved)
    return candidates


def _resolve_drop_renderer_executable(config) -> tuple[Path, Path | None]:
    env_override = os.environ.get("AUTORESEARCH_DROP_RENDERER")
    if env_override:
        path = Path(env_override).expanduser()
        if path.exists():
            return path.resolve(), next(iter(_trading_dashboard_roots(config)), None)
    resolved = shutil.which("fuzzfolio-drop-renderer")
    if resolved:
        return Path(resolved).resolve(), next(iter(_trading_dashboard_roots(config)), None)

    exe_name = "fuzzfolio-drop-renderer.exe" if os.name == "nt" else "fuzzfolio-drop-renderer"
    for workspace_root in _trading_dashboard_roots(config):
        candidate = workspace_root / "harness" / "fuzzfolio_drop_renderer" / "cli" / "target" / "release" / exe_name
        if candidate.exists():
            return candidate.resolve(), workspace_root
    raise FileNotFoundError(
        "Could not resolve fuzzfolio-drop-renderer. Set AUTORESEARCH_DROP_RENDERER or build the renderer under Trading-Dashboard."
    )


def _run_external(argv: list[str], *, cwd: Path) -> None:
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        encoding="utf-8",
    )
    if proc.returncode == 0:
        return
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    raise RuntimeError(
        f"Command failed: {' '.join(argv)}\n"
        f"cwd: {cwd}\n"
        f"exit: {proc.returncode}\n"
        f"stdout:\n{stdout[:1600]}\n\nstderr:\n{stderr[:1600]}"
    )


def _best_attempt_for_run(attempts: list[dict[str, Any]], *, lower_is_better: bool = False) -> dict[str, Any] | None:
    scored = [attempt for attempt in attempts if attempt.get("composite_score") is not None]
    if not scored:
        return None
    return sorted(
        scored,
        key=lambda attempt: float(attempt.get("composite_score")),
        reverse=not lower_is_better,
    )[0]


def _nested_get(payload: dict[str, Any], path: list[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def _attempt_max_drawdown_r(attempt: dict[str, Any]) -> float | None:
    best_summary = attempt.get("best_summary")
    if not isinstance(best_summary, dict):
        return None
    candidates = [
        best_summary.get("best_cell_path_metrics"),
        best_summary.get("quality_score_payload"),
    ]
    for payload in candidates:
        if not isinstance(payload, dict):
            continue
        for path in (
            ["max_drawdown_r"],
            ["inputs", "max_drawdown_r"],
        ):
            current: Any = payload
            for key in path:
                if not isinstance(current, dict):
                    current = None
                    break
                current = current.get(key)
            try:
                if current is not None:
                    return float(current)
            except (TypeError, ValueError):
                continue
    return None


def _coerce_profile_instruments(profile_path: Path) -> list[str]:
    payload = _load_json_if_exists(profile_path)
    instruments = _nested_get(payload, ["profile", "instruments"])
    if not isinstance(instruments, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in instruments:
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _coerce_profile_timeframe(profile_path: Path) -> str:
    payload = _load_json_if_exists(profile_path)
    indicators = _nested_get(payload, ["profile", "indicators"])
    if not isinstance(indicators, list):
        return ""
    seen: list[str] = []
    for indicator in indicators:
        if not isinstance(indicator, dict):
            continue
        token = str(_nested_get(indicator, ["config", "timeframe"]) or "").strip().upper()
        if not token or token in seen:
            continue
        seen.append(token)
    return seen[0] if seen else ""


def _normalize_tokens(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _attempt_request_payload(attempt: dict[str, Any]) -> dict[str, Any]:
    artifact_dir = Path(str(attempt.get("artifact_dir", ""))).resolve()
    request_payload = _nested_get(_load_json_if_exists(artifact_dir / "deep-replay-job.json"), ["request"]) or {}
    return request_payload if isinstance(request_payload, dict) else {}


def _candidate_sweep_stems(candidate_name: str) -> list[str]:
    token = candidate_name.strip().lower()
    if not token:
        return []
    stems = [token]
    for marker in ["_new_eval", "_eval"]:
        index = token.find(marker)
        if index > 0:
            stems.append(token[:index])
    ordered: list[str] = []
    seen: set[str] = set()
    for stem in stems:
        if stem and stem not in seen:
            seen.add(stem)
            ordered.append(stem)
    return ordered


def _find_sweep_definition(run_dir: Path, attempt: dict[str, Any]) -> dict[str, Any]:
    candidate_name = str(attempt.get("candidate_name") or "")
    stems = _candidate_sweep_stems(candidate_name)
    search_roots = [run_dir / "profiles", run_dir / "profiles" / "sweeps"]
    candidates: list[tuple[int, Path, dict[str, Any]]] = []
    for root in search_roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.json")):
            payload = _load_json_if_exists(path)
            if not payload:
                continue
            base_profile_id = str(payload.get("base_profile_id") or "").strip()
            if not base_profile_id:
                continue
            lowered_name = path.stem.lower()
            score = 0
            for index, stem in enumerate(stems):
                if lowered_name == stem:
                    score = max(score, 100 - index)
                elif lowered_name.startswith(stem):
                    score = max(score, 80 - index)
            if score <= 0 and "sweep" in lowered_name:
                score = 1
            if score > 0:
                candidates.append((score, path, payload))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: (-item[0], len(str(item[1]))))
    return candidates[0][2]


def _find_attempt_for_profile_ref(attempts: list[dict[str, Any]], profile_ref: str) -> dict[str, Any] | None:
    profile_ref = profile_ref.strip()
    if not profile_ref:
        return None
    for attempt in attempts:
        if str(attempt.get("profile_ref") or "").strip() == profile_ref:
            return attempt
        request_payload = _attempt_request_payload(attempt)
        if str(request_payload.get("profile_id") or "").strip() == profile_ref:
            return attempt
    return None


def _recover_package_inputs_from_sweep(
    run_dir: Path,
    attempt: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    sweep_payload = _find_sweep_definition(run_dir, attempt)
    if not sweep_payload:
        return {}
    base_profile_id = str(sweep_payload.get("base_profile_id") or "").strip()
    if not base_profile_id:
        return {}
    base_attempt = _find_attempt_for_profile_ref(attempts, base_profile_id)
    if base_attempt is None:
        return {}

    base_profile_path_raw = str(base_attempt.get("profile_path") or "").strip()
    base_profile_path = Path(base_profile_path_raw).resolve() if base_profile_path_raw else None
    base_request_payload = _attempt_request_payload(base_attempt)

    instruments = _normalize_tokens(list(sweep_payload.get("instruments") or []))
    if not instruments and base_profile_path is not None and base_profile_path.exists():
        instruments = _coerce_profile_instruments(base_profile_path)

    timeframe = str(
        base_request_payload.get("timeframe")
        or _nested_get(base_attempt, ["best_summary", "timeframe"])
        or (_coerce_profile_timeframe(base_profile_path) if base_profile_path is not None and base_profile_path.exists() else "")
        or ""
    ).strip().upper()

    if not timeframe or not instruments:
        return {}

    return {
        "artifact_dir": Path(str(attempt.get("artifact_dir", ""))).resolve(),
        "profile_path": base_profile_path,
        "profile_ref": base_profile_id,
        "timeframe": timeframe,
        "instruments": instruments,
        "lookback_months": _derive_lookback_months(base_request_payload, _load_json_if_exists(Path(str(base_attempt.get("artifact_dir", ""))).resolve() / "sensitivity-response.json")),
        "recovered_from_sweep": True,
    }


def _derive_lookback_months(request_payload: dict[str, Any], sensitivity_payload: dict[str, Any]) -> int:
    raw_months = request_payload.get("lookback_months")
    if isinstance(raw_months, int) and raw_months > 0:
        return raw_months
    effective_months = _nested_get(
        sensitivity_payload,
        ["data", "aggregate", "market_data_window", "effective_window_months"],
    )
    if effective_months is None:
        effective_months = _nested_get(
            sensitivity_payload,
            ["data", "market_data_window", "effective_window_months"],
        )
    try:
        numeric = float(effective_months)
    except (TypeError, ValueError):
        numeric = 3.0
    return max(1, int(ceil(numeric)))


def _build_package_inputs(
    attempt: dict[str, Any],
    *,
    run_dir: Path | None = None,
    attempts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    artifact_dir = Path(str(attempt.get("artifact_dir", ""))).resolve()
    profile_path_raw = str(attempt.get("profile_path", "")).strip()
    profile_path = Path(profile_path_raw).resolve() if profile_path_raw else None

    request_payload = _attempt_request_payload(attempt)
    sensitivity_payload = _load_json_if_exists(artifact_dir / "sensitivity-response.json")

    timeframe = str(
        request_payload.get("timeframe")
        or _nested_get(sensitivity_payload, ["data", "aggregate", "timeframe"])
        or _nested_get(sensitivity_payload, ["data", "timeframe"])
        or _nested_get(attempt, ["best_summary", "timeframe"])
        or (_coerce_profile_timeframe(profile_path) if profile_path is not None and profile_path.exists() else "")
        or ""
    ).strip().upper()

    instruments_raw = request_payload.get("instruments")
    instruments = _normalize_tokens(instruments_raw if isinstance(instruments_raw, list) else [])
    if not instruments and profile_path is not None and profile_path.exists():
        instruments = _coerce_profile_instruments(profile_path)

    if (not timeframe or not instruments or not (profile_path is not None and profile_path.exists())) and run_dir is not None and attempts is not None:
        recovered = _recover_package_inputs_from_sweep(run_dir, attempt, attempts)
        if recovered:
            merged = {
                "artifact_dir": artifact_dir,
                "profile_path": profile_path,
                "timeframe": timeframe,
                "instruments": instruments,
                "lookback_months": _derive_lookback_months(request_payload, sensitivity_payload),
            }
            merged.update({key: value for key, value in recovered.items() if value})
            return merged

    if not timeframe:
        raise RuntimeError(f"Could not resolve timeframe for attempt {attempt.get('attempt_id')}")
    if not instruments:
        raise RuntimeError(f"Could not resolve instruments for attempt {attempt.get('attempt_id')}")

    return {
        "artifact_dir": artifact_dir,
        "profile_path": profile_path,
        "timeframe": timeframe,
        "instruments": instruments,
        "lookback_months": _derive_lookback_months(request_payload, sensitivity_payload),
    }


def _profile_export_missing(text: str) -> bool:
    lowered = text.lower()
    return "not found" in lowered or "404" in lowered or "no document" in lowered


def _cloud_profile_exists(cli: FuzzfolioCli, profile_ref: str) -> bool:
    result = cli.run(["export-profile", "--profile-ref", profile_ref], check=False)
    if result.returncode == 0:
        return True
    combined = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
    if _profile_export_missing(combined):
        return False
    raise CliError(FuzzfolioCli.format_result(result))


def _create_cloud_profile(cli: FuzzfolioCli, profile_path: Path) -> str:
    result = cli.run(["profiles", "create", "--file", str(profile_path), "--pretty"])
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else None
    profile_id = str(_nested_get(payload or {}, ["data", "id"]) or "").strip()
    if not profile_id:
        raise CliError(f"profiles create did not return a profile id for {profile_path}")
    return profile_id


def _update_attempt_profile_ref(run_dir: Path, attempt_id: str, profile_ref: str) -> None:
    attempts_path = attempts_path_for_run_dir(run_dir)
    attempts = load_attempts(attempts_path)
    changed = False
    for attempt in attempts:
        if str(attempt.get("attempt_id") or "") != attempt_id:
            continue
        if str(attempt.get("profile_ref") or "").strip() == profile_ref:
            return
        attempt["profile_ref"] = profile_ref
        changed = True
    if changed:
        write_attempts(attempts_path, attempts)


def _discover_bundle_dir(package_output_root: Path) -> Path:
    bundle_dirs = [path for path in package_output_root.iterdir() if path.is_dir()]
    if not bundle_dirs:
        raise RuntimeError(f"Package command did not create a bundle under {package_output_root}")
    return sorted(bundle_dirs, key=lambda path: path.stat().st_mtime, reverse=True)[0]


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _validation_cache_dir(config, run_id: str, lookback_months: int) -> Path:
    return config.validation_cache_root / run_id / f"{int(lookback_months)}mo"


def _validation_manifest_path(config, run_id: str, lookback_months: int) -> Path:
    return _validation_cache_dir(config, run_id, lookback_months) / "manifest.json"


def _profile_drop_manifest_path(run_dir: Path, lookback_months: int) -> Path:
    return run_dir / f"profile-drop-{int(lookback_months)}mo.manifest.json"


def _load_validation_score(artifact_dir: Path) -> dict[str, Any]:
    sensitivity_payload = _load_json_if_exists(artifact_dir / "sensitivity-response.json")
    aggregate = _nested_get(sensitivity_payload, ["data", "aggregate"])
    if not isinstance(aggregate, dict):
        aggregate = _nested_get(sensitivity_payload, ["data"])
    compare_payload = {"best": aggregate or {}}
    score = build_attempt_score(compare_payload, sensitivity_payload if sensitivity_payload else None)
    synthetic_attempt = {
        "best_summary": score.best_summary,
        "composite_score": score.composite_score,
    }
    return {
        "score": score.composite_score,
        "score_basis": score.score_basis,
        "metrics": score.metrics,
        "best_summary": score.best_summary,
        "trade_count": _attempt_trade_count(synthetic_attempt),
        "trades_per_month": _attempt_trades_per_month(synthetic_attempt),
        "effective_window_months": _attempt_effective_window_months(synthetic_attempt),
        "max_drawdown_r": _attempt_max_drawdown_r(synthetic_attempt),
    }


def _load_validation_curve_series(artifact_dir: Path) -> dict[str, float]:
    payload = _load_json_if_exists(artifact_dir / "best-cell-path-detail.json")
    points = _nested_get(payload, ["curve", "points"])
    if not isinstance(points, list):
        return {}
    series: dict[str, float] = {}
    for point in points:
        if not isinstance(point, dict):
            continue
        date_key = str(point.get("date") or "").strip()
        if not date_key:
            continue
        try:
            realized_r = float(point.get("realized_r"))
        except (TypeError, ValueError):
            continue
        series[date_key] = realized_r
    return series


def _load_validation_request(artifact_dir: Path) -> dict[str, Any]:
    payload = _load_json_if_exists(artifact_dir / "deep-replay-job.json")
    request_payload = payload.get("request") if isinstance(payload, dict) else None
    return request_payload if isinstance(request_payload, dict) else {}


def _pearson_correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 3:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0.0 or right_var <= 0.0:
        return None
    covariance = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    return covariance / (left_var ** 0.5 * right_var ** 0.5)


def _build_similarity_payload(validation_rows: list[dict[str, Any]]) -> dict[str, Any]:
    prepared: list[dict[str, Any]] = []
    for row in validation_rows:
        artifact_dir_raw = str(row.get("artifact_dir_36m") or "").strip()
        if not artifact_dir_raw:
            continue
        artifact_dir = Path(artifact_dir_raw)
        curve_series = _load_validation_curve_series(artifact_dir)
        if not curve_series:
            continue
        request_payload = _load_validation_request(artifact_dir)
        instruments = _normalize_tokens(list(request_payload.get("instruments") or []))
        timeframe = str(request_payload.get("timeframe") or "").strip() or None
        active_dates = {date for date, value in curve_series.items() if abs(float(value)) > 1e-9}
        prepared.append(
            {
                **row,
                "curve_series": curve_series,
                "instruments_36m": instruments,
                "timeframe_36m": timeframe,
                "active_dates": active_dates,
            }
        )

    if not prepared:
        return {"leaders": [], "pairs": [], "matrix_labels": [], "matrix_values": []}

    prepared.sort(key=lambda item: float(item.get("score_36m", float("-inf"))), reverse=True)
    pair_records: list[dict[str, Any]] = []

    for left_index, left in enumerate(prepared):
        left_dates = set(left["curve_series"].keys())
        left_values_map = left["curve_series"]
        left_instruments = set(str(item) for item in left.get("instruments_36m") or [])
        for right_index in range(left_index + 1, len(prepared)):
            right = prepared[right_index]
            right_dates = set(right["curve_series"].keys())
            common_dates = sorted(left_dates & right_dates)
            if len(common_dates) < 30:
                continue
            left_values = [float(left_values_map[date]) for date in common_dates]
            right_values = [float(right["curve_series"][date]) for date in common_dates]
            corr = _pearson_correlation(left_values, right_values)
            positive_corr = max(0.0, float(corr)) if corr is not None else 0.0
            right_instruments = set(str(item) for item in right.get("instruments_36m") or [])
            union_instruments = left_instruments | right_instruments
            instrument_overlap = (
                len(left_instruments & right_instruments) / len(union_instruments)
                if union_instruments
                else 0.0
            )
            active_left = set(left.get("active_dates") or set())
            active_right = set(right.get("active_dates") or set())
            active_union = active_left | active_right
            shared_active_ratio = (
                len(active_left & active_right) / len(active_union)
                if active_union
                else 0.0
            )
            similarity_score = max(
                0.0,
                min(1.0, positive_corr * 0.75 + shared_active_ratio * 0.25),
            )
            pair_records.append(
                {
                    "left_run_id": left["run_id"],
                    "left_attempt_id": left["attempt_id"],
                    "left_label": left.get("leaderboard_label") or left["run_id"],
                    "right_run_id": right["run_id"],
                    "right_attempt_id": right["attempt_id"],
                    "right_label": right.get("leaderboard_label") or right["run_id"],
                    "left_score_36m": left.get("score_36m"),
                    "right_score_36m": right.get("score_36m"),
                    "correlation": corr,
                    "positive_correlation": positive_corr,
                    "shared_active_ratio": shared_active_ratio,
                    "instrument_overlap_ratio": instrument_overlap,
                    "same_timeframe": str(left.get("timeframe_36m") or "") == str(right.get("timeframe_36m") or ""),
                    "overlap_days": len(common_dates),
                    "similarity_score": similarity_score,
                }
            )

    adjacency: dict[str, list[dict[str, Any]]] = {str(item["run_id"]): [] for item in prepared}
    for pair in pair_records:
        adjacency[pair["left_run_id"]].append(pair)
        adjacency[pair["right_run_id"]].append(pair)

    leaders: list[dict[str, Any]] = []
    for row in prepared:
        related = adjacency.get(str(row["run_id"]), [])
        max_pair = max(related, key=lambda item: float(item.get("similarity_score", 0.0)), default=None)
        avg_sameness = (
            sum(float(item.get("similarity_score", 0.0)) for item in related) / len(related)
            if related
            else 0.0
        )
        closest_match_run_id = None
        closest_match_label = None
        if max_pair:
            if max_pair["left_run_id"] == row["run_id"]:
                closest_match_run_id = max_pair["right_run_id"]
                closest_match_label = max_pair["right_label"]
            else:
                closest_match_run_id = max_pair["left_run_id"]
                closest_match_label = max_pair["left_label"]
        leaders.append(
            {
                "run_id": row["run_id"],
                "attempt_id": row["attempt_id"],
                "candidate_name": row.get("candidate_name"),
                "leaderboard_label": row.get("leaderboard_label"),
                "score_36m": row.get("score_36m"),
                "score_12m": row.get("score_12m"),
                "score_delta": row.get("score_delta"),
                "trades_per_month_36m": row.get("trades_per_month_36m"),
                "trade_count_36m": row.get("trade_count_36m"),
                "instrument_count_36m": len(row.get("instruments_36m") or []),
                "instruments_36m": list(row.get("instruments_36m") or []),
                "timeframe_36m": row.get("timeframe_36m"),
                "avg_sameness": avg_sameness,
                "max_sameness": float(max_pair.get("similarity_score", 0.0)) if max_pair else 0.0,
                "closest_match_run_id": closest_match_run_id,
                "closest_match_label": closest_match_label,
            }
        )

    matrix_labels = [
        str(item.get("leaderboard_label") or item.get("run_id") or "run")
        for item in prepared
    ]
    pair_lookup: dict[tuple[str, str], float] = {}
    for pair in pair_records:
        key = tuple(sorted([str(pair["left_run_id"]), str(pair["right_run_id"])]))
        pair_lookup[key] = float(pair.get("similarity_score", 0.0))

    matrix_values: list[list[float]] = []
    for left in prepared:
        row_values: list[float] = []
        for right in prepared:
            if left["run_id"] == right["run_id"]:
                row_values.append(1.0)
                continue
            key = tuple(sorted([str(left["run_id"]), str(right["run_id"])]))
            row_values.append(float(pair_lookup.get(key, 0.0)))
        matrix_values.append(row_values)

    pair_records.sort(key=lambda item: float(item.get("similarity_score", 0.0)), reverse=True)
    return {
        "leaders": leaders,
        "pairs": pair_records,
        "matrix_labels": matrix_labels,
        "matrix_values": matrix_values,
    }


def _ensure_validation_artifacts(
    *,
    config,
    cli: FuzzfolioCli,
    run_dir: Path,
    attempts: list[dict[str, Any]],
    best_attempt: dict[str, Any],
    lookback_months: int,
    force_rebuild: bool = False,
    emit: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    package_inputs = _build_package_inputs(best_attempt, run_dir=run_dir, attempts=attempts)
    profile_path = package_inputs.get("profile_path")
    profile_ref = str(package_inputs.get("profile_ref") or best_attempt.get("profile_ref") or "").strip()
    recreated_profile = False

    if profile_ref and not _cloud_profile_exists(cli, profile_ref):
        profile_ref = ""
    if not profile_ref:
        if not isinstance(profile_path, Path) or not profile_path.exists():
            raise RuntimeError(
                f"Best attempt is missing a valid cloud profile ref and local profile file: {profile_path}"
            )
        profile_ref = _create_cloud_profile(cli, profile_path)
        _update_attempt_profile_ref(run_dir, str(best_attempt.get("attempt_id") or ""), profile_ref)
        recreated_profile = True

    cache_dir = _validation_cache_dir(config, run_dir.name, lookback_months)
    manifest_path = _validation_manifest_path(config, run_dir.name, lookback_months)
    manifest_payload = {
        "run_id": run_dir.name,
        "attempt_id": str(best_attempt.get("attempt_id") or ""),
        "candidate_name": str(best_attempt.get("candidate_name") or ""),
        "profile_ref": profile_ref,
        "timeframe": str(package_inputs["timeframe"]),
        "instruments": list(package_inputs["instruments"]),
        "lookback_months": int(lookback_months),
        "quality_score_preset": str(config.research.quality_score_preset),
    }
    sensitivity_path = cache_dir / "sensitivity-response.json"
    if (not force_rebuild) and sensitivity_path.exists() and manifest_path.exists():
        existing_manifest = _load_json_if_exists(manifest_path)
        if existing_manifest == manifest_payload:
            payload = _load_validation_score(cache_dir)
            payload["artifact_dir"] = str(cache_dir)
            payload["profile_ref"] = profile_ref
            payload["recreated_profile"] = recreated_profile
            payload["cache_hit"] = True
            return payload

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if emit:
        emit(
            f"  validating {lookback_months}mo: "
            f"timeframe={package_inputs['timeframe']} instruments={','.join(package_inputs['instruments'])}"
        )

    args = [
        "sensitivity-basket",
        "--profile-ref",
        profile_ref,
        "--timeframe",
        str(package_inputs["timeframe"]),
        "--lookback-months",
        str(int(lookback_months)),
        "--output-dir",
        str(cache_dir),
        "--allow-timeframe-mismatch",
        "--quality-score-preset",
        str(config.research.quality_score_preset),
    ]
    for instrument in package_inputs["instruments"]:
        args.extend(["--instrument", str(instrument)])
    cli.run(args, timeout_seconds=420)
    _write_json_file(manifest_path, manifest_payload)

    payload = _load_validation_score(cache_dir)
    payload["artifact_dir"] = str(cache_dir)
    payload["profile_ref"] = profile_ref
    payload["recreated_profile"] = recreated_profile
    payload["cache_hit"] = False
    return payload


def _build_validation_rows(
    *,
    config,
    cli: FuzzfolioCli,
    ranked_attempts: list[dict[str, Any]],
    run_metadata_by_run_id: dict[str, dict[str, Any]],
    force_rebuild: bool = False,
    emit: Callable[[str], None] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for attempt in ranked_attempts:
        run_id = str(attempt.get("run_id") or "").strip()
        if not run_id:
            continue
        run_dir = config.runs_root / run_id
        attempts = load_run_attempts(run_dir)
        if not attempts:
            continue
        best_attempt = _best_attempt_for_run(attempts, lower_is_better=config.research.plot_lower_is_better)
        if best_attempt is None or str(best_attempt.get("attempt_id") or "") != str(attempt.get("attempt_id") or ""):
            best_attempt = attempt
        if emit:
            emit(f"validate {run_id} {best_attempt.get('attempt_id')}")
        row = {
            "run_id": run_id,
            "attempt_id": str(best_attempt.get("attempt_id") or ""),
            "candidate_name": best_attempt.get("candidate_name"),
            "leaderboard_label": attempt.get("leaderboard_label"),
            "explorer_model": (run_metadata_by_run_id.get(run_id) or {}).get("explorer_model"),
            "explorer_profile": (run_metadata_by_run_id.get(run_id) or {}).get("explorer_profile"),
        }
        try:
            validation_12 = _ensure_validation_artifacts(
                config=config,
                cli=cli,
                run_dir=run_dir,
                attempts=attempts,
                best_attempt=best_attempt,
                lookback_months=12,
                force_rebuild=force_rebuild,
                emit=emit,
            )
            validation_36 = _ensure_validation_artifacts(
                config=config,
                cli=cli,
                run_dir=run_dir,
                attempts=attempts,
                best_attempt=best_attempt,
                lookback_months=36,
                force_rebuild=force_rebuild,
                emit=emit,
            )
        except Exception as exc:
            if emit:
                detail = str(exc).splitlines()[0].strip() if str(exc).strip() else exc.__class__.__name__
                emit(f"validate skip {run_id} {best_attempt.get('attempt_id')}: {detail}")
            continue
        row.update(
            {
                "score_12m": validation_12.get("score"),
                "score_basis_12m": validation_12.get("score_basis"),
                "trades_per_month_12m": validation_12.get("trades_per_month"),
                "trade_count_12m": validation_12.get("trade_count"),
                "effective_window_months_12m": validation_12.get("effective_window_months"),
                "max_drawdown_r_12m": validation_12.get("max_drawdown_r"),
                "artifact_dir_12m": validation_12.get("artifact_dir"),
                "score_36m": validation_36.get("score"),
                "score_basis_36m": validation_36.get("score_basis"),
                "trades_per_month_36m": validation_36.get("trades_per_month"),
                "trade_count_36m": validation_36.get("trade_count"),
                "effective_window_months_36m": validation_36.get("effective_window_months"),
                "max_drawdown_r_36m": validation_36.get("max_drawdown_r"),
                "artifact_dir_36m": validation_36.get("artifact_dir"),
            }
        )
        try:
            score_12 = float(row["score_12m"])
            score_36 = float(row["score_36m"])
        except (TypeError, ValueError):
            pass
        else:
            row["score_delta"] = score_36 - score_12
            row["score_retention_ratio"] = (score_36 / score_12) if score_12 not in {0.0, -0.0} else None
        rows.append(row)
    return rows


def cmd_sync_profile_drop_pngs(
    *,
    run_ids: list[str] | None,
    keep_temp: bool,
    lookback_months: int,
    force_rebuild: bool,
    as_json: bool,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()
    renderer_executable, workspace_root = _resolve_drop_renderer_executable(config)
    working_dir = workspace_root or config.repo_root

    all_run_dirs = list_run_dirs(config.runs_root)
    if run_ids:
        wanted = {token.strip() for token in run_ids if str(token).strip()}
        run_dirs = [run_dir for run_dir in all_run_dirs if run_dir.name in wanted]
        missing = sorted(wanted - {run_dir.name for run_dir in run_dirs})
        if missing:
            raise SystemExit(f"Run directories do not exist: {', '.join(missing)}")
    else:
        run_dirs = all_run_dirs

    results: list[dict[str, Any]] = []
    rendered = 0
    skipped = 0
    failed = 0

    total_runs = len(run_dirs)
    use_progress = (not as_json) and (not PLAIN_PROGRESS_MODE) and bool(getattr(console, "is_terminal", False))
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=32),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
        disable=not use_progress,
    )

    def emit(message: str) -> None:
        if as_json:
            return
        if use_progress:
            progress.console.print(message)
            return
        _write_plain_line(message)

    with progress:
        task_id = progress.add_task("sync profile drops", total=total_runs or 1)
        for index, run_dir in enumerate(run_dirs, start=1):
            progress.update(
                task_id,
                description=(
                    f"sync {index}/{total_runs} "
                    f"[green]ok={rendered}[/green] "
                    f"[yellow]skip={skipped}[/yellow] "
                    f"[red]fail={failed}[/red] "
                    f"{run_dir.name}"
                ),
            )
            temp_root = run_dir / ".profile-drop-sync"
            result: dict[str, Any] = {"run_id": run_dir.name}
            try:
                emit(
                    f"sync {index}/{total_runs} {run_dir.name}"
                )
                attempts = load_run_attempts(run_dir)
                best_attempt = _best_attempt_for_run(
                    attempts,
                    lower_is_better=config.research.plot_lower_is_better,
                )
                if best_attempt is None:
                    skipped += 1
                    result["status"] = "skipped"
                    result["reason"] = "No scored attempts exist for this run."
                    emit("  skipped: no scored attempts")
                    results.append(result)
                    progress.advance(task_id, 1)
                    continue

                emit(
                    "  best attempt: "
                    f"{best_attempt.get('attempt_id')} "
                    f"score={float(best_attempt.get('composite_score')):.3f}"
                )
                package_inputs = _build_package_inputs(best_attempt, run_dir=run_dir, attempts=attempts)
                profile_path = package_inputs.get("profile_path")
                profile_ref = str(package_inputs.get("profile_ref") or best_attempt.get("profile_ref") or "").strip()
                recreated_profile = False

                if profile_ref:
                    emit(f"  checking cloud profile: {profile_ref}")
                    if not _cloud_profile_exists(cli, profile_ref):
                        profile_ref = ""
                if not profile_ref:
                    if not isinstance(profile_path, Path) or not profile_path.exists():
                        raise RuntimeError(
                            f"Best attempt is missing a valid cloud profile ref and local profile file: {profile_path}"
                        )
                    emit("  cloud profile missing, recreating from local profile")
                    profile_ref = _create_cloud_profile(cli, profile_path)
                    _update_attempt_profile_ref(run_dir, str(best_attempt.get("attempt_id") or ""), profile_ref)
                    recreated_profile = True

                if temp_root.exists():
                    shutil.rmtree(temp_root)
                rendered_pngs: list[str] = []
                skipped_horizons: list[int] = []
                requested_horizons = sorted({12, int(lookback_months), 36})
                for horizon_months in requested_horizons:
                    horizon_manifest_payload = {
                        "version": 1,
                        "run_id": run_dir.name,
                        "attempt_id": str(best_attempt.get("attempt_id") or ""),
                        "candidate_name": str(best_attempt.get("candidate_name") or ""),
                        "profile_ref": profile_ref,
                        "timeframe": str(package_inputs["timeframe"]),
                        "instruments": list(package_inputs["instruments"]),
                        "lookback_months": int(horizon_months),
                        "quality_score_preset": str(config.research.quality_score_preset),
                    }
                    horizon_manifest_path = _profile_drop_manifest_path(run_dir, horizon_months)
                    png_path = run_dir / f"profile-drop-{horizon_months}mo.png"
                    if (
                        not force_rebuild
                        and png_path.exists()
                        and horizon_manifest_path.exists()
                        and _load_json_if_exists(horizon_manifest_path) == horizon_manifest_payload
                    ):
                        emit(f"  skipping {png_path.name}: up to date")
                        rendered_pngs.append(str(png_path))
                        skipped_horizons.append(horizon_months)
                        continue
                    package_output_root = temp_root / f"package-root-{horizon_months}mo"
                    package_output_root.mkdir(parents=True, exist_ok=True)
                    emit(
                        "  packaging: "
                        f"timeframe={package_inputs['timeframe']} "
                        f"instruments={','.join(package_inputs['instruments'])} "
                        f"lookback={horizon_months}mo"
                    )
                    if package_inputs.get("recovered_from_sweep"):
                        emit("  recovered package inputs from sweep base profile")
                    package_args = [
                        "package",
                        "--profile-ref",
                        profile_ref,
                        "--timeframe",
                        str(package_inputs["timeframe"]),
                        "--lookback-months",
                        str(horizon_months),
                        "--output-root",
                        str(package_output_root),
                        "--label",
                        f"{run_dir.name}-{horizon_months}mo",
                        "--skip-catalogs",
                        "--skip-render-capture",
                        "--allow-timeframe-mismatch",
                        "--quality-score-preset",
                        str(config.research.quality_score_preset),
                    ]
                    for instrument in package_inputs["instruments"]:
                        package_args.extend(["--instrument", str(instrument)])
                    cli.run(package_args, cwd=working_dir)

                    bundle_dir = _discover_bundle_dir(package_output_root)
                    png_path = run_dir / f"profile-drop-{horizon_months}mo.png"
                    emit(f"  rendering {png_path.name}")
                    renderer_argv = [str(renderer_executable)]
                    if workspace_root is not None:
                        renderer_argv.extend(["--workspace-root", str(workspace_root)])
                    renderer_argv.extend(
                        [
                            "render",
                            "--bundle",
                            str(bundle_dir),
                            "--out",
                            str(png_path),
                        ]
                    )
                    _run_external(renderer_argv, cwd=working_dir)
                    _write_json_file(horizon_manifest_path, horizon_manifest_payload)
                    rendered_pngs.append(str(png_path))

                if not keep_temp:
                    if temp_root.exists():
                        shutil.rmtree(temp_root)

                rendered_horizons = [months for months in requested_horizons if months not in skipped_horizons]
                if rendered_horizons or recreated_profile:
                    rendered += 1
                    emit(
                        "  done"
                        + (" (recreated cloud profile)" if recreated_profile else "")
                    )
                    status = "rendered"
                else:
                    skipped += 1
                    emit("  skipped: all requested horizons already up to date")
                    status = "skipped"
                result.update(
                    {
                        "status": status,
                        "png_paths": rendered_pngs,
                        "profile_ref": profile_ref,
                        "recreated_profile": recreated_profile,
                        "lookback_months": lookback_months,
                        "rendered_horizons": rendered_horizons,
                        "skipped_horizons": skipped_horizons,
                        "attempt_id": best_attempt.get("attempt_id"),
                        "candidate_name": best_attempt.get("candidate_name"),
                    }
                )
            except Exception as exc:
                failed += 1
                result["status"] = "failed"
                result["error"] = str(exc)
                emit(f"  failed: {exc}")
                if temp_root.exists():
                    result["temp_root"] = str(temp_root)
            results.append(result)
            progress.advance(task_id, 1)

    payload = {
        "runs_considered": len(run_dirs),
        "rendered": rendered,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if failed == 0 else 1


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
            advisor_profiles=args.advisor_profile,
            advisor_every=args.advisor_every,
            no_advisor=bool(args.no_advisor),
            as_json=bool(args.json),
            plain_progress=bool(args.plain_progress),
        )
    if args.command == "supervise":
        return cmd_supervise(
            max_steps=args.max_steps,
            window=args.window,
            no_window=bool(args.no_window),
            timezone_name=args.timezone,
            explorer_profile=args.explorer_profile,
            supervisor_profile=args.supervisor_profile,
            advisor_profiles=args.advisor_profile,
            advisor_every=args.advisor_every,
            no_advisor=bool(args.no_advisor),
            as_json=bool(args.json),
            plain_progress=bool(args.plain_progress),
        )
    if args.command == "plot":
        return cmd_plot(run_id=args.run_id, all_runs=bool(args.all_runs))
    if args.command == "leaderboard":
        return cmd_leaderboard(limit=args.limit, force_rebuild=bool(args.force_rebuild))
    if args.command == "dashboard":
        return cmd_dashboard(
            host=str(args.host),
            port=int(args.port),
            limit=int(args.limit),
            refresh_on_start=not bool(args.no_refresh_on_start),
            force_rebuild=bool(args.force_rebuild),
        )
    if args.command == "sync-profile-drop-pngs":
        return cmd_sync_profile_drop_pngs(
            run_ids=args.run_id,
            keep_temp=bool(args.keep_temp),
            lookback_months=int(args.lookback_months),
            force_rebuild=bool(args.force_rebuild),
            as_json=bool(args.json),
        )
    if args.command == "reset-runs":
        return cmd_reset_runs()
    if args.command == "prune-runs":
        return cmd_prune_runs(
            min_mapped_points=int(args.min_mapped_points),
            execute=bool(args.yes),
            preview=int(args.preview),
            as_json=bool(args.json),
        )
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
