from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from rich.console import Console

from .play_hand import INSTRUMENT_POOL_PRESET_NAMES
from .play_hand_lab import (
    DEFAULT_LAB_GATEWAY_URL,
    PlayHandLabRuntimeConfig,
    cmd_play_hand_lab,
)
from .play_hand_lab_gateway import (
    DEFAULT_MAX_BODY_BYTES,
    cmd_play_hand_lab_gateway,
    cmd_play_hand_lab_http_sim,
    cmd_play_hand_lab_sim,
    cmd_play_hand_lab_ws_sim,
)


PLAY_HAND_LAB_COMMANDS = {
    "play-hand-lab",
    "play-hand-lab-gateway",
    "play-hand-lab-http-sim",
    "play-hand-lab-sim",
    "play-hand-lab-ws-sim",
    "play-hand-massive-v2",
    "play-hand-massive-v2-gateway",
    "play-hand-massive-v2-http-sim",
    "play-hand-massive-v2-sim",
    "play-hand-massive-v2-ws-sim",
}

PLAY_HAND_LAB_COORDINATOR_COMMANDS = {"play-hand-lab", "play-hand-massive-v2"}
PLAY_HAND_LAB_GATEWAY_COMMANDS = {"play-hand-lab-gateway", "play-hand-massive-v2-gateway"}
PLAY_HAND_LAB_IN_PROCESS_SIM_COMMANDS = {"play-hand-lab-sim", "play-hand-massive-v2-sim"}
PLAY_HAND_LAB_HTTP_SIM_COMMANDS = {"play-hand-lab-http-sim", "play-hand-massive-v2-http-sim"}
PLAY_HAND_LAB_WS_SIM_COMMANDS = {"play-hand-lab-ws-sim", "play-hand-massive-v2-ws-sim"}


def add_play_hand_lab_subparsers(subparsers: Any) -> None:
    play_hand_lab = subparsers.add_parser(
        "play-hand-massive-v2",
        aliases=["play-hand-lab"],
        help="Run first-class PlayHand Lab work through the in-memory lab gateway.",
    )
    play_hand_lab.add_argument(
        "--gateway-url",
        default=os.environ.get("FUZZFOLIO_LAB_GATEWAY_URL", DEFAULT_LAB_GATEWAY_URL),
        help=f"Lab gateway base URL. Default: {DEFAULT_LAB_GATEWAY_URL}.",
    )
    play_hand_lab.add_argument(
        "--gateway-token",
        default=os.environ.get("FUZZFOLIO_LAB_GATEWAY_TOKEN"),
        help="Optional bearer token for the lab gateway.",
    )
    play_hand_lab.add_argument(
        "--task-mode",
        choices=["fake_compute", "deep_replay"],
        default="deep_replay",
        help="Task kind to enqueue. Default: deep_replay.",
    )
    play_hand_lab.add_argument(
        "--pipeline-mode",
        choices=["screen", "play_hand"],
        default="play_hand",
        help="Lane lifecycle. play_hand runs staged baseline/sweep/scout/final; screen runs one replay.",
    )
    play_hand_lab.add_argument(
        "--mode",
        choices=["finite", "continuous"],
        default="finite",
        help="Campaign mode. finite exits after --target-runs drain; continuous replaces completed runs until stopped.",
    )
    play_hand_lab.add_argument(
        "--target-runs",
        "--lanes",
        dest="target_runs",
        type=int,
        default=None,
        help="Total candidate run folders to create in finite mode. --lanes is a compatibility alias. Default: 4.",
    )
    play_hand_lab.add_argument(
        "--active-runs",
        type=int,
        default=None,
        help="Candidate runs to keep in flight at once. Defaults to min(target-runs, 64).",
    )
    play_hand_lab.add_argument(
        "--tasks-per-lane",
        type=int,
        default=1,
        help="Tasks queued for each lane. Default: 1.",
    )
    play_hand_lab.add_argument("--timeframe", default="M5", help="Requested base timeframe. Default: M5.")
    play_hand_lab.add_argument(
        "--instrument",
        action="append",
        default=None,
        help="Instrument or comma-separated instruments. Repeatable.",
    )
    play_hand_lab.add_argument(
        "--instrument-pool",
        action="append",
        default=None,
        help="Explicit instrument-pool symbol or comma-separated symbols. Repeatable.",
    )
    play_hand_lab.add_argument(
        "--instrument-pool-preset",
        "--instrument-pool-set",
        action="append",
        default=None,
        help=(
            "Named instrument-pool preset. Repeatable or comma-separated. "
            "Available: " + ", ".join(INSTRUMENT_POOL_PRESET_NAMES) + "."
        ),
    )
    play_hand_lab.add_argument(
        "--indicator",
        action="append",
        default=None,
        help="Indicator id or comma-separated ids to pin. Repeatable.",
    )
    play_hand_lab.add_argument(
        "--profile-path",
        type=Path,
        default=None,
        help="Optional full scoring profile JSON to reuse for every lane.",
    )
    play_hand_lab.add_argument(
        "--min-indicators",
        type=int,
        default=1,
        help="Minimum indicators dealt per generated lane profile. Default: 1.",
    )
    play_hand_lab.add_argument(
        "--max-indicators",
        type=int,
        default=4,
        help="Maximum indicators dealt per generated lane profile. Default: 4.",
    )
    play_hand_lab.add_argument("--seed", type=int, default=None, help="Optional deterministic lane-deal seed.")
    play_hand_lab.add_argument(
        "--lookback-months",
        type=int,
        default=3,
        help="Deep-replay lookback window. Default: 3.",
    )
    play_hand_lab.add_argument(
        "--bar-limit",
        type=int,
        default=5000,
        help="Maximum bars read by each deep-replay task. Default: 5000.",
    )
    play_hand_lab.add_argument(
        "--max-reward-r",
        type=float,
        default=None,
        help="Optional max reward R for the replay reward matrix.",
    )
    play_hand_lab.add_argument(
        "--sweep-budget",
        choices=["low", "medium", "high"],
        default="high",
        help="PlayHand sweep budget preset. Default: high.",
    )
    play_hand_lab.add_argument(
        "--max-sweep-permutations",
        type=int,
        default=None,
        help="Override PlayHand sweep permutation budget.",
    )
    play_hand_lab.add_argument(
        "--sweep-shard-size",
        type=int,
        default=8,
        help="Permutations per lab sweep-shard task. Default: 8.",
    )
    play_hand_lab.add_argument(
        "--early-exit-mode",
        choices=["off", "report", "enforce"],
        default="enforce",
        help="PlayHand early-exit policy. Default: enforce.",
    )
    play_hand_lab.add_argument(
        "--coarse-halving-mode",
        choices=["off", "enforce"],
        default="enforce",
        help="Run a coarse probe before full expansion. Default: enforce.",
    )
    play_hand_lab.add_argument(
        "--coarse-probe-budget",
        type=int,
        default=128,
        help="Permutation budget for the coarse probe. Default: 128.",
    )
    play_hand_lab.add_argument(
        "--scrutiny-months",
        type=int,
        default=36,
        help="Final scrutiny lookback months. Default: 36.",
    )
    play_hand_lab.add_argument(
        "--instrument-scout-size",
        type=int,
        default=5,
        help="Additional instruments to scout before final scrutiny. Default: 5.",
    )
    play_hand_lab.add_argument(
        "--instrument-scout-max-selected",
        type=int,
        default=3,
        help="Maximum instruments selected by scout. Default: 3.",
    )
    play_hand_lab.add_argument(
        "--fake-work-seconds",
        type=float,
        default=1.0,
        help="Fake-compute task duration. Default: 1.",
    )
    play_hand_lab.add_argument(
        "--deadline-seconds",
        type=float,
        default=3600.0,
        help="Gateway lease deadline per task. Default: 3600.",
    )
    play_hand_lab.add_argument(
        "--max-attempts",
        type=int,
        default=2,
        help="Maximum gateway lease attempts before failure. Default: 2.",
    )
    play_hand_lab.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="Coordinator result-drain interval. Default: 1.",
    )
    play_hand_lab.add_argument(
        "--max-wait-seconds",
        type=float,
        default=3600.0,
        help="Maximum coordinator wait after enqueue. Default: 3600.",
    )
    play_hand_lab.add_argument(
        "--result-batch-size",
        type=int,
        default=25,
        help="Maximum completed results drained per poll. Default: 25.",
    )
    play_hand_lab.add_argument(
        "--result-read-failure-limit",
        type=int,
        default=5,
        help="Consecutive gateway result-read failures before ending the campaign. Default: 5.",
    )
    play_hand_lab.add_argument(
        "--worker-contract-hash",
        default=os.environ.get("FUZZFOLIO_REPLAY_WORKER_CONTRACT_HASH")
        or os.environ.get("FUZZFOLIO_WORKER_CONTRACT_HASH"),
        help="Required replay worker contract hash. Auto-resolved from Trading-Dashboard when omitted.",
    )
    play_hand_lab.add_argument(
        "--worker-contract-schema",
        default=os.environ.get("FUZZFOLIO_REPLAY_WORKER_CONTRACT_SCHEMA", "replay-worker-contract-v1"),
        help="Required replay worker contract schema. Default: replay-worker-contract-v1.",
    )
    play_hand_lab.add_argument(
        "--trading-dashboard-root",
        type=Path,
        default=Path(os.environ["TRADING_DASHBOARD_ROOT"]).resolve()
        if os.environ.get("TRADING_DASHBOARD_ROOT")
        else None,
        help="Trading-Dashboard root used to resolve the worker contract hash.",
    )
    play_hand_lab.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare run folders and tasks but do not enqueue work.",
    )
    play_hand_lab.add_argument(
        "--strict-scoring",
        action="store_true",
        help="Fail if deep-replay artifact scoring fails.",
    )
    play_hand_lab.add_argument(
        "--retain-raw-lab-artifacts",
        action="store_true",
        help=(
            "Retain verbose lab-result/lab-worker-result/sweep-shard-result debug JSON. "
            "Default is compact canonical artifacts only."
        ),
    )
    play_hand_lab.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary.",
    )

    play_hand_lab_sim = subparsers.add_parser(
        "play-hand-massive-v2-sim",
        aliases=["play-hand-lab-sim"],
        help="Run an in-process PlayHand Lab Gateway saturation simulation.",
    )
    play_hand_lab_sim.add_argument(
        "--workers",
        type=int,
        default=100,
        help="Logical worker count to simulate. Default: 100.",
    )
    play_hand_lab_sim.add_argument(
        "--target-completions",
        type=int,
        default=None,
        help="Completion target before stopping. Defaults to workers times backlog multiplier.",
    )
    play_hand_lab_sim.add_argument(
        "--fixed-work-seconds",
        type=float,
        default=10.0,
        help="Simulated task runtime before time scaling. Default: 10.",
    )
    play_hand_lab_sim.add_argument(
        "--time-scale",
        type=float,
        default=0.001,
        help="Scale factor applied to simulated task runtimes. Default: 0.001.",
    )
    play_hand_lab_sim.add_argument(
        "--max-wall-seconds",
        type=float,
        default=15.0,
        help="Maximum wall-clock seconds before the simulation fails. Default: 15.",
    )
    play_hand_lab_sim.add_argument(
        "--runtime-distribution",
        choices=["fixed", "lognormal"],
        default="fixed",
        help="Task runtime distribution. Default: fixed.",
    )
    play_hand_lab_sim.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary.",
    )

    play_hand_lab_gateway = subparsers.add_parser(
        "play-hand-massive-v2-gateway",
        aliases=["play-hand-lab-gateway"],
        help="Serve the lab-only PlayHand worker gateway.",
    )
    play_hand_lab_gateway.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind. Default: 127.0.0.1.",
    )
    play_hand_lab_gateway.add_argument(
        "--port",
        type=int,
        default=8799,
        help="TCP port to bind. Default: 8799.",
    )
    play_hand_lab_gateway.add_argument(
        "--token",
        default=None,
        help="Optional bearer token required for worker requests.",
    )
    play_hand_lab_gateway.add_argument(
        "--max-body-mb",
        type=float,
        default=DEFAULT_MAX_BODY_BYTES / (1024 * 1024),
        help="Maximum HTTP/WebSocket request body size in MiB. Default: 64.",
    )

    play_hand_lab_http_sim = subparsers.add_parser(
        "play-hand-massive-v2-http-sim",
        aliases=["play-hand-lab-http-sim"],
        help="Run an HTTP loopback PlayHand Lab Gateway saturation simulation.",
    )
    _add_loopback_sim_args(play_hand_lab_http_sim, transport_label="HTTP", jitter_help="worker registration")

    play_hand_lab_ws_sim = subparsers.add_parser(
        "play-hand-massive-v2-ws-sim",
        aliases=["play-hand-lab-ws-sim"],
        help="Run a WebSocket PlayHand Lab Gateway saturation simulation.",
    )
    _add_loopback_sim_args(play_hand_lab_ws_sim, transport_label="WebSocket", jitter_help="worker connection")


def _add_loopback_sim_args(parser: Any, *, transport_label: str, jitter_help: str) -> None:
    parser.add_argument(
        "--workers",
        type=int,
        default=100,
        help=f"Logical {transport_label} worker count to simulate. Default: 100.",
    )
    parser.add_argument(
        "--target-completions",
        type=int,
        default=None,
        help="Completion target before stopping. Defaults to workers times backlog multiplier.",
    )
    parser.add_argument(
        "--work-seconds",
        type=float,
        default=0.01,
        help="Sleep duration per fake task. Default: 0.01.",
    )
    parser.add_argument(
        "--runtime-distribution",
        choices=["fixed", "lognormal"],
        default="fixed",
        help="Fake task runtime distribution. Default: fixed.",
    )
    parser.add_argument(
        "--startup-jitter-seconds",
        type=float,
        default=0.5,
        help=f"Maximum initial {jitter_help} jitter. Default: 0.5.",
    )
    parser.add_argument(
        "--max-wall-seconds",
        type=float,
        default=30.0,
        help="Maximum wall-clock seconds before the simulation fails. Default: 30.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary.",
    )


def dispatch_play_hand_lab_command(args: Any, *, console: Console) -> int | None:
    if args.command in PLAY_HAND_LAB_COORDINATOR_COMMANDS:
        return cmd_play_hand_lab(
            PlayHandLabRuntimeConfig(
                gateway_url=args.gateway_url,
                gateway_token=args.gateway_token,
                campaign_mode=args.mode,
                task_mode=args.task_mode,
                pipeline_mode=args.pipeline_mode,
                target_runs=args.target_runs,
                active_runs=args.active_runs,
                lanes=args.target_runs or 4,
                tasks_per_lane=args.tasks_per_lane,
                timeframe=args.timeframe,
                instrument=args.instrument,
                instrument_pool_preset=args.instrument_pool_preset,
                instrument_pool=args.instrument_pool,
                indicator=args.indicator,
                profile_path=args.profile_path,
                min_indicators=args.min_indicators,
                max_indicators=args.max_indicators,
                seed=args.seed,
                lookback_months=args.lookback_months,
                bar_limit=args.bar_limit,
                max_reward_r=args.max_reward_r,
                sweep_budget=args.sweep_budget,
                max_sweep_permutations=args.max_sweep_permutations,
                sweep_shard_size=args.sweep_shard_size,
                early_exit_mode=args.early_exit_mode,
                coarse_halving_mode=args.coarse_halving_mode,
                coarse_probe_budget=args.coarse_probe_budget,
                scrutiny_months=args.scrutiny_months,
                instrument_scout_size=args.instrument_scout_size,
                instrument_scout_max_selected=args.instrument_scout_max_selected,
                fake_work_seconds=args.fake_work_seconds,
                deadline_seconds=args.deadline_seconds,
                max_attempts=args.max_attempts,
                poll_interval_seconds=args.poll_interval_seconds,
                max_wait_seconds=args.max_wait_seconds,
                result_batch_size=args.result_batch_size,
                result_read_failure_limit=args.result_read_failure_limit,
                dry_run=bool(args.dry_run),
                strict_scoring=bool(args.strict_scoring),
                retain_raw_lab_artifacts=bool(args.retain_raw_lab_artifacts),
                json_output=bool(args.json),
                worker_contract_hash=args.worker_contract_hash,
                worker_contract_schema=args.worker_contract_schema,
                trading_dashboard_root=args.trading_dashboard_root,
            )
        )
    if args.command in PLAY_HAND_LAB_IN_PROCESS_SIM_COMMANDS:
        result = cmd_play_hand_lab_sim(
            workers=args.workers,
            target_completions=args.target_completions,
            fixed_work_seconds=args.fixed_work_seconds,
            time_scale=args.time_scale,
            max_wall_seconds=args.max_wall_seconds,
            runtime_distribution=args.runtime_distribution,
        )
        _print_sim_result(
            result,
            args=args,
            console=console,
            title="PlayHand Lab simulation",
            include_latency=False,
        )
        return 0 if bool(result.get("ok")) else 1
    if args.command in PLAY_HAND_LAB_GATEWAY_COMMANDS:
        return cmd_play_hand_lab_gateway(
            host=str(args.host),
            port=int(args.port),
            token=args.token,
            max_body_bytes=max(int(float(args.max_body_mb) * 1024 * 1024), 1024),
        )
    if args.command in PLAY_HAND_LAB_HTTP_SIM_COMMANDS:
        result = cmd_play_hand_lab_http_sim(
            workers=args.workers,
            target_completions=args.target_completions,
            work_seconds=args.work_seconds,
            runtime_distribution=args.runtime_distribution,
            startup_jitter_seconds=args.startup_jitter_seconds,
            max_wall_seconds=args.max_wall_seconds,
        )
        _print_sim_result(
            result,
            args=args,
            console=console,
            title="PlayHand Lab HTTP simulation",
            include_latency=True,
        )
        return 0 if bool(result.get("ok")) else 1
    if args.command in PLAY_HAND_LAB_WS_SIM_COMMANDS:
        result = cmd_play_hand_lab_ws_sim(
            workers=args.workers,
            target_completions=args.target_completions,
            work_seconds=args.work_seconds,
            runtime_distribution=args.runtime_distribution,
            startup_jitter_seconds=args.startup_jitter_seconds,
            max_wall_seconds=args.max_wall_seconds,
        )
        _print_sim_result(
            result,
            args=args,
            console=console,
            title="PlayHand Lab WebSocket simulation",
            include_latency=True,
        )
        return 0 if bool(result.get("ok")) else 1
    return None


def _print_sim_result(
    result: dict[str, Any],
    *,
    args: Any,
    console: Console,
    title: str,
    include_latency: bool,
) -> None:
    if bool(args.json):
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    snapshot = result.get("snapshot", {})
    message = (
        f"[bold]{title}[/bold] "
        f"workers={args.workers} "
        f"completed={snapshot.get('completed_tasks', 0)}/"
        f"{result.get('target_completions', 0)} "
        f"saturated_busy_avg={float(result.get('saturated_busy_rate_avg', 0.0)):.3f}"
    )
    if include_latency:
        message += (
            f" claim_p95_ms={float(result.get('claim_latency_p95_ms', 0.0)):.3f} "
            f"complete_p95_ms={float(result.get('completion_latency_p95_ms', 0.0)):.3f}"
        )
    else:
        message += f" wall_seconds={float(result.get('wall_seconds', 0.0)):.3f}"
    console.print(message)


__all__ = [
    "PLAY_HAND_LAB_COMMANDS",
    "add_play_hand_lab_subparsers",
    "dispatch_play_hand_lab_command",
]
