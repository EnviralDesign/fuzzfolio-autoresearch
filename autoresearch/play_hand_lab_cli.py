from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from rich.console import Console

from .play_hand import INSTRUMENT_POOL_PRESET_NAMES
from .play_hand_lab import (
    DEFAULT_LAB_ENQUEUE_FAILURE_LIMIT,
    DEFAULT_LAB_ENQUEUE_RETRY_BASE_SECONDS,
    DEFAULT_LAB_FINAL_MIN_SCORE,
    DEFAULT_LAB_GATEWAY_URL,
    DEFAULT_LAB_MAX_DRAIN_SECONDS,
    DEFAULT_LAB_MAX_RESULTS_PER_CYCLE,
    DEFAULT_LAB_SCRUTINY_MONTHS,
    DEFAULT_LAB_SCREEN_ANCHOR_ENVELOPE_MONTHS,
    DEFAULT_LAB_SCREEN_ANCHOR_MODE,
    DEFAULT_LAB_TERMINAL_LANE_RETENTION,
    DEFAULT_LAB_VALIDATION_MIN_SCORE,
    DEFAULT_LAB_VALIDATION_MONTHS,
    PlayHandLabRuntimeConfig,
    cmd_play_hand_lab,
)
from .play_hand_lab_gateway import (
    DEFAULT_MAX_RESULT_BACKLOG_BYTES,
    DEFAULT_MAX_BODY_BYTES,
    DEFAULT_LAB_WS_PING_INTERVAL_SECONDS,
    DEFAULT_LAB_WS_PING_TIMEOUT_SECONDS,
    DEFAULT_LAKE_MUTATION_RETRY_AFTER_SECONDS,
    DEFAULT_LAKE_TIMEOUT_RETRY_AFTER_SECONDS,
    DEFAULT_RESULT_BACKPRESSURE_BYTES,
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
        "--execution-plan",
        type=Path,
        default=None,
        help="Authoritative Level-C execution plan. Required for formal historical execution.",
    )
    play_hand_lab.add_argument(
        "--resume",
        action="store_true",
        help="Resume a matching formal campaign from its durable task graph.",
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
        "--seed-plan-path",
        type=Path,
        default=None,
        help="Optional PlayHand seed-plan JSON or recipe-priors directory override.",
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
        help="Cheap screen/discovery lookback window. Default: 3.",
    )
    play_hand_lab.add_argument(
        "--screen-anchor-mode",
        choices=["now", "random"],
        default=DEFAULT_LAB_SCREEN_ANCHOR_MODE,
        help=(
            "Screen window anchor for cheap discovery phases. "
            f"Default: {DEFAULT_LAB_SCREEN_ANCHOR_MODE}."
        ),
    )
    play_hand_lab.add_argument(
        "--screen-anchor-envelope-months",
        type=int,
        default=DEFAULT_LAB_SCREEN_ANCHOR_ENVELOPE_MONTHS,
        help=(
            "Rolling envelope used by random screen anchoring. "
            f"Default: {DEFAULT_LAB_SCREEN_ANCHOR_ENVELOPE_MONTHS}."
        ),
    )
    play_hand_lab.add_argument(
        "--as-of-date",
        default=None,
        help="Hard UTC data cutoff applied to every discovery, validation, scout, and scrutiny replay.",
    )
    play_hand_lab.add_argument(
        "--campaign-id",
        default=None,
        help="Explicit campaign identity. Required with --as-of-date; otherwise a timestamp ID is generated.",
    )
    play_hand_lab.add_argument(
        "--lake-manifest-sha256",
        default=None,
        help="Exact promoted lake coverage identity. Required with --as-of-date.",
    )
    play_hand_lab.add_argument(
        "--research-generation-id",
        default=None,
        help="Formal research-generation lineage. Required with --as-of-date.",
    )
    play_hand_lab.add_argument(
        "--level-c-protocol-id",
        default=None,
        help="Formal Level-C protocol lineage. Required with --as-of-date.",
    )
    play_hand_lab.add_argument(
        "--cutoff-key",
        default=None,
        help="Formal data-cutoff lineage key. Required with --as-of-date.",
    )
    play_hand_lab.add_argument(
        "--expected-seed-plan-sha256",
        default=None,
        help="Exact SHA-256 of --seed-plan-path. Required with --as-of-date.",
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
        "--validation-months",
        type=int,
        default=DEFAULT_LAB_VALIDATION_MONTHS,
        help=(
            "Intermediate validation lookback months before instrument scout/final. "
            f"Default: {DEFAULT_LAB_VALIDATION_MONTHS}."
        ),
    )
    play_hand_lab.add_argument(
        "--validation-min-score",
        type=float,
        default=DEFAULT_LAB_VALIDATION_MIN_SCORE,
        help=(
            "Minimum validation score required to continue. "
            f"Default: {DEFAULT_LAB_VALIDATION_MIN_SCORE:g}."
        ),
    )
    play_hand_lab.add_argument(
        "--scrutiny-months",
        type=int,
        default=DEFAULT_LAB_SCRUTINY_MONTHS,
        help=f"Final scrutiny lookback months. Default: {DEFAULT_LAB_SCRUTINY_MONTHS}.",
    )
    play_hand_lab.add_argument(
        "--final-min-score",
        type=float,
        default=DEFAULT_LAB_FINAL_MIN_SCORE,
        help=(
            "Minimum final scrutiny score required for promotion. "
            f"Default: {DEFAULT_LAB_FINAL_MIN_SCORE:g}."
        ),
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
        default=8,
        help="Maximum gateway lease attempts before failure. Default: 8.",
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
        "--max-results-per-cycle",
        type=int,
        default=DEFAULT_LAB_MAX_RESULTS_PER_CYCLE,
        help=(
            "Maximum completed results processed before yielding to snapshots/sleep. "
            f"Default: {DEFAULT_LAB_MAX_RESULTS_PER_CYCLE}."
        ),
    )
    play_hand_lab.add_argument(
        "--max-drain-seconds",
        type=float,
        default=DEFAULT_LAB_MAX_DRAIN_SECONDS,
        help=(
            "Maximum wall seconds spent draining consecutive full result batches per coordinator cycle. "
            f"Default: {DEFAULT_LAB_MAX_DRAIN_SECONDS:g}."
        ),
    )
    play_hand_lab.add_argument(
        "--result-read-failure-limit",
        type=int,
        default=5,
        help="Consecutive gateway result-read failures before ending the campaign. Default: 5.",
    )
    play_hand_lab.add_argument(
        "--enqueue-failure-limit",
        type=int,
        default=DEFAULT_LAB_ENQUEUE_FAILURE_LIMIT,
        help=f"Consecutive gateway task enqueue failures before ending the campaign. Default: {DEFAULT_LAB_ENQUEUE_FAILURE_LIMIT}.",
    )
    play_hand_lab.add_argument(
        "--enqueue-retry-base-seconds",
        type=float,
        default=DEFAULT_LAB_ENQUEUE_RETRY_BASE_SECONDS,
        help=(
            "Base sleep for gateway task enqueue retries; retry N sleeps base*N seconds, capped at 30. "
            f"Default: {DEFAULT_LAB_ENQUEUE_RETRY_BASE_SECONDS:g}."
        ),
    )
    play_hand_lab.add_argument(
        "--terminal-lane-retention",
        type=int,
        default=DEFAULT_LAB_TERMINAL_LANE_RETENTION,
        help=(
            "Number of terminal lane states retained in coordinator memory before rolling older lanes into "
            f"campaign counters. Default: {DEFAULT_LAB_TERMINAL_LANE_RETENTION}."
        ),
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
        help="Print a machine-readable JSON summary after coordinator logs.",
    )
    play_hand_lab.add_argument(
        "--log-mode",
        choices=["barrier", "stream", "quiet"],
        default="barrier",
        help=(
            "Coordinator stdout style. barrier prints bounded periodic snapshots plus explicit failures; "
            "stream prints every event; quiet suppresses event chatter. Default: barrier."
        ),
    )
    play_hand_lab.add_argument(
        "--barrier-interval-seconds",
        type=float,
        default=5.0,
        help="Seconds between barrier snapshot boxes in --log-mode barrier. Default: 5.",
    )
    play_hand_lab.add_argument(
        "--barrier-lane-limit",
        type=int,
        default=24,
        help="Maximum lane rows shown in each barrier snapshot. Default: 24.",
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
    play_hand_lab_gateway.add_argument(
        "--lease-ttl-seconds",
        type=float,
        default=600.0,
        help="Minimum gateway lease TTL in seconds. Task deadlines can extend this. Default: 600.",
    )
    play_hand_lab_gateway.add_argument(
        "--max-recent-completions",
        type=int,
        default=20_000,
        help="Recent completion receipts retained for idempotent duplicate completion handling. Default: 20000.",
    )
    play_hand_lab_gateway.add_argument(
        "--max-result-backlog",
        type=int,
        default=100_000,
        help="Maximum unacked result count retained before oldest results are dropped. Default: 100000.",
    )
    play_hand_lab_gateway.add_argument(
        "--max-result-backlog-mb",
        type=float,
        default=DEFAULT_MAX_RESULT_BACKLOG_BYTES / (1024 * 1024),
        help="Maximum approximate unacked result backlog size in MiB. Default: 2048.",
    )
    play_hand_lab_gateway.add_argument(
        "--result-backpressure-mb",
        type=float,
        default=DEFAULT_RESULT_BACKPRESSURE_BYTES / (1024 * 1024),
        help="Approximate result backlog size in MiB that pauses new claims until drained. Default: 1024.",
    )
    play_hand_lab_gateway.add_argument(
        "--max-recent-terminal-task-ids",
        type=int,
        default=100_000,
        help="Recent terminal task ids retained for idempotent enqueue retries. Default: 100000.",
    )
    play_hand_lab_gateway.add_argument(
        "--worker-stale-after-seconds",
        type=float,
        default=600.0,
        help="Seconds without worker heartbeat before active lab leases are requeued. Default: 600.",
    )
    play_hand_lab_gateway.add_argument(
        "--worker-prune-after-seconds",
        type=float,
        default=1800.0,
        help="Seconds without worker heartbeat before idle lab workers are pruned. Default: 1800.",
    )
    play_hand_lab_gateway.add_argument(
        "--lake-mutation-retry-after-seconds",
        type=float,
        default=DEFAULT_LAKE_MUTATION_RETRY_AFTER_SECONDS,
        help=(
            "Delay before requeueing retryable remote-lake mutation failures. "
            f"Default: {DEFAULT_LAKE_MUTATION_RETRY_AFTER_SECONDS:g}."
        ),
    )
    play_hand_lab_gateway.add_argument(
        "--lake-timeout-retry-after-seconds",
        type=float,
        default=DEFAULT_LAKE_TIMEOUT_RETRY_AFTER_SECONDS,
        help=(
            "Delay before requeueing retryable remote-lake read timeout failures. "
            f"Default: {DEFAULT_LAKE_TIMEOUT_RETRY_AFTER_SECONDS:g}."
        ),
    )
    play_hand_lab_gateway.add_argument(
        "--ws-ping-interval-seconds",
        type=float,
        default=DEFAULT_LAB_WS_PING_INTERVAL_SECONDS,
        help=f"Gateway WebSocket keepalive ping interval. Default: {DEFAULT_LAB_WS_PING_INTERVAL_SECONDS:g}.",
    )
    play_hand_lab_gateway.add_argument(
        "--ws-ping-timeout-seconds",
        type=float,
        default=DEFAULT_LAB_WS_PING_TIMEOUT_SECONDS,
        help=f"Gateway WebSocket keepalive ping timeout. Default: {DEFAULT_LAB_WS_PING_TIMEOUT_SECONDS:g}.",
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
        formal_values = {
            "as_of_date": args.as_of_date,
            "campaign_id": args.campaign_id,
            "lake_manifest_sha256": args.lake_manifest_sha256,
            "research_generation_id": args.research_generation_id,
            "level_c_protocol_id": args.level_c_protocol_id,
            "cutoff_key": args.cutoff_key,
            "expected_seed_plan_sha256": args.expected_seed_plan_sha256,
            "seed_plan_path": args.seed_plan_path,
            "seed": args.seed,
        }
        plan_arguments: dict[str, Any] = {}
        if args.execution_plan is not None:
            conflicts = sorted(key for key, value in formal_values.items() if value is not None)
            if conflicts:
                raise ValueError(
                    "Formal PlayHand lineage must come only from --execution-plan; remove: "
                    + ", ".join("--" + key.replace("_", "-") for key in conflicts)
                )
            from .level_c_operator import executor_arguments_from_plan

            plan_arguments, _plan = executor_arguments_from_plan(
                args.execution_plan, executor="playhand"
            )
        elif args.as_of_date:
            raise ValueError("Formal historical PlayHand requires --execution-plan.")
        return cmd_play_hand_lab(
            PlayHandLabRuntimeConfig(
                gateway_url=args.gateway_url,
                gateway_token=args.gateway_token,
                campaign_mode=plan_arguments.get("campaign_mode", args.mode),
                task_mode=plan_arguments.get("task_mode", args.task_mode),
                pipeline_mode=plan_arguments.get("pipeline_mode", args.pipeline_mode),
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
                seed_plan_path=(Path(plan_arguments["seed_plan_path"]) if plan_arguments else args.seed_plan_path),
                min_indicators=args.min_indicators,
                max_indicators=args.max_indicators,
                seed=plan_arguments.get("seed", args.seed),
                lookback_months=args.lookback_months,
                bar_limit=args.bar_limit,
                max_reward_r=args.max_reward_r,
                sweep_budget=args.sweep_budget,
                max_sweep_permutations=args.max_sweep_permutations,
                sweep_shard_size=args.sweep_shard_size,
                early_exit_mode=args.early_exit_mode,
                coarse_halving_mode=args.coarse_halving_mode,
                coarse_probe_budget=args.coarse_probe_budget,
                validation_months=args.validation_months,
                validation_min_score=args.validation_min_score,
                scrutiny_months=args.scrutiny_months,
                final_min_score=args.final_min_score,
                screen_anchor_mode=args.screen_anchor_mode,
                screen_anchor_envelope_months=args.screen_anchor_envelope_months,
                as_of_date=plan_arguments.get("as_of_date", args.as_of_date),
                campaign_id=plan_arguments.get("campaign_id", args.campaign_id),
                lake_manifest_sha256=plan_arguments.get("lake_manifest_sha256", args.lake_manifest_sha256),
                research_generation_id=plan_arguments.get("research_generation_id", args.research_generation_id),
                level_c_protocol_id=plan_arguments.get("level_c_protocol_id", args.level_c_protocol_id),
                cutoff_key=plan_arguments.get("cutoff_key", args.cutoff_key),
                source_snapshot_sha256=plan_arguments.get("source_snapshot_sha256"),
                universe_id=plan_arguments.get("universe_id"),
                universe_manifest_sha256=plan_arguments.get("universe_manifest_sha256"),
                expected_seed_plan_sha256=plan_arguments.get("expected_seed_plan_sha256", args.expected_seed_plan_sha256),
                execution_plan_path=(Path(plan_arguments["execution_plan_path"]) if plan_arguments else None),
                execution_plan_id=plan_arguments.get("execution_plan_id"),
                resume=bool(args.resume),
                instrument_scout_size=args.instrument_scout_size,
                instrument_scout_max_selected=args.instrument_scout_max_selected,
                fake_work_seconds=args.fake_work_seconds,
                deadline_seconds=args.deadline_seconds,
                max_attempts=args.max_attempts,
                poll_interval_seconds=args.poll_interval_seconds,
                max_wait_seconds=args.max_wait_seconds,
                result_batch_size=args.result_batch_size,
                max_results_per_cycle=args.max_results_per_cycle,
                max_drain_seconds=args.max_drain_seconds,
                result_read_failure_limit=args.result_read_failure_limit,
                enqueue_failure_limit=args.enqueue_failure_limit,
                enqueue_retry_base_seconds=args.enqueue_retry_base_seconds,
                terminal_lane_retention=args.terminal_lane_retention,
                dry_run=bool(args.dry_run),
                strict_scoring=bool(plan_arguments.get("strict_scoring", args.strict_scoring)),
                retain_raw_lab_artifacts=bool(args.retain_raw_lab_artifacts),
                json_output=bool(args.json),
                log_mode=args.log_mode,
                barrier_interval_seconds=args.barrier_interval_seconds,
                barrier_lane_limit=args.barrier_lane_limit,
                worker_contract_hash=plan_arguments.get("worker_contract_hash", args.worker_contract_hash),
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
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_recent_completions=args.max_recent_completions,
            max_result_backlog=args.max_result_backlog,
            max_result_backlog_bytes=max(int(float(args.max_result_backlog_mb) * 1024 * 1024), 0),
            result_backpressure_bytes=max(int(float(args.result_backpressure_mb) * 1024 * 1024), 0),
            max_recent_terminal_task_ids=args.max_recent_terminal_task_ids,
            worker_stale_after_seconds=args.worker_stale_after_seconds,
            worker_prune_after_seconds=args.worker_prune_after_seconds,
            lake_mutation_retry_after_seconds=args.lake_mutation_retry_after_seconds,
            lake_timeout_retry_after_seconds=args.lake_timeout_retry_after_seconds,
            ws_ping_interval_seconds=args.ws_ping_interval_seconds,
            ws_ping_timeout_seconds=args.ws_ping_timeout_seconds,
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
