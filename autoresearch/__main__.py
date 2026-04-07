from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import redirect_stdout
import io
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time as pytime
import urllib.error
import urllib.request
from datetime import datetime, time, timedelta
from math import ceil
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from autoresearch.config import load_config
    from autoresearch.corpus_tools import (
        build_full_backtest_audit,
        catalog_summary,
        build_similarity_payload as build_candidate_similarity_payload,
        extract_attempt_catalog_row,
        full_backtest_provisional_reasons,
        legacy_validation_cache_dir,
        load_json_if_exists,
        normalize_tokens,
        resolve_attempt_scrutiny_source,
        scrutiny_cache_dir_for_artifact_dir,
        select_promotion_board,
        subset_similarity_payload,
        write_csv,
        write_json,
    )
    from autoresearch.controller import (
        ResearchController,
        RunPolicy,
        set_runtime_trace_stderr_mode,
    )
    from autoresearch.dashboard import _has_full_backtest, _run_full_backtest_for_attempt
    from autoresearch.dashboard_viewer import serve_dashboard
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
        render_attempt_tradeoff_overlay_artifacts,
        render_attempt_drawdown_scatter_artifacts,
        render_progress_artifacts,
        render_attempt_tradeoff_scatter_artifacts,
        render_similarity_heatmap_artifacts,
        render_similarity_scatter_artifacts,
        render_tradeoff_leaderboard_artifacts,
        render_validation_delta_artifacts,
        render_validation_scatter_artifacts,
    )
    from autoresearch.portfolio import (
        build_sleeve_selection,
        filter_selection_candidate_rows,
        load_portfolio_spec,
        merge_portfolio_sleeves,
    )
    from autoresearch.provider import (
        ChatMessage,
        ProviderError,
        create_provider,
        set_provider_trace_stderr_mode,
    )
    from autoresearch.scoring import build_attempt_score, load_sensitivity_snapshot
    from autoresearch.typed_tools import CLI_OK_TOOLS
else:
    from .config import load_config
    from .corpus_tools import (
        build_full_backtest_audit,
        catalog_summary,
        build_similarity_payload as build_candidate_similarity_payload,
        extract_attempt_catalog_row,
        full_backtest_provisional_reasons,
        legacy_validation_cache_dir,
        load_json_if_exists,
        normalize_tokens,
        resolve_attempt_scrutiny_source,
        scrutiny_cache_dir_for_artifact_dir,
        select_promotion_board,
        subset_similarity_payload,
        write_csv,
        write_json,
    )
    from .controller import ResearchController, RunPolicy, set_runtime_trace_stderr_mode
    from .dashboard import _has_full_backtest, _run_full_backtest_for_attempt
    from .dashboard_viewer import serve_dashboard
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
        render_attempt_tradeoff_overlay_artifacts,
        render_attempt_drawdown_scatter_artifacts,
        render_progress_artifacts,
        render_attempt_tradeoff_scatter_artifacts,
        render_similarity_heatmap_artifacts,
        render_similarity_scatter_artifacts,
        render_tradeoff_leaderboard_artifacts,
        render_validation_delta_artifacts,
        render_validation_scatter_artifacts,
    )
    from .portfolio import (
        build_sleeve_selection,
        filter_selection_candidate_rows,
        load_portfolio_spec,
        merge_portfolio_sleeves,
    )
    from .provider import (
        ChatMessage,
        ProviderError,
        create_provider,
        set_provider_trace_stderr_mode,
    )
    from .scoring import build_attempt_score, load_sensitivity_snapshot
    from .typed_tools import CLI_OK_TOOLS


console = Console(safe_box=True)
DISPLAY_CONTEXT: dict[str, Path | None] = {"repo_root": None, "run_dir": None}
PLAIN_PROGRESS_MODE = False
PLAIN_PROGRESS_STATE: dict[str, Any] = {
    "best_score": None,
    "last_score": None,
    "run_id": None,
}
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

    doctor = subparsers.add_parser(
        "doctor", help="Verify config, CLI, auth, and seed prompt."
    )
    doctor.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

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
    provider_test.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    run = subparsers.add_parser("run", help="Run the autonomous research controller.")
    run.add_argument("--max-steps", type=int, default=None)
    run.add_argument(
        "--explorer-profile",
        default=None,
        help="Override the configured explorer provider profile for this run.",
    )
    run.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of live console progress.",
    )
    run.add_argument(
        "--plain-progress",
        action="store_true",
        help="Use plain line-oriented progress output instead of Rich panels.",
    )

    supervise = subparsers.add_parser(
        "supervise",
        help="Run the supervised controller with config-backed policy defaults.",
    )
    supervise.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Per-session step cap before supervise starts a fresh isolated session.",
    )
    supervise.add_argument(
        "--window", default=None, help="Operating window in HH:MM-HH:MM format."
    )
    supervise.add_argument(
        "--no-window",
        action="store_true",
        help="Disable supervise windowing and run sessions around the clock.",
    )
    supervise.add_argument(
        "--timezone",
        default=None,
        help="IANA timezone for the operating window, e.g. America/Chicago.",
    )
    supervise.add_argument(
        "--explorer-profile",
        default=None,
        help="Override the configured explorer provider profile for this run.",
    )
    supervise.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of live console progress.",
    )
    supervise.add_argument(
        "--plain-progress",
        action="store_true",
        help="Use plain line-oriented progress output instead of Rich panels.",
    )

    plot = subparsers.add_parser(
        "plot", help="Generate a run-local or all-runs derived progress plot."
    )
    plot.add_argument(
        "--run-id",
        default=None,
        help="Specific run id to render. Defaults to latest discovered run.",
    )
    plot.add_argument(
        "--all-runs",
        action="store_true",
        help="Render a derived aggregate plot across all runs.",
    )
    leaderboard = subparsers.add_parser(
        "leaderboard",
        help="Generate a derived best-per-run leaderboard image and JSON.",
    )
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
    dashboard = subparsers.add_parser(
        "dashboard",
        help="Serve the read-only dashboard viewer from already-computed derived artifacts.",
    )
    dashboard.add_argument(
        "--host", default="0.0.0.0", help="Bind host. Default: 0.0.0.0"
    )
    dashboard.add_argument(
        "--port", type=int, default=47832, help="Bind port. Default: 47832"
    )
    dashboard.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Legacy no-op kept for compatibility. The dashboard no longer rebuilds or limits data on startup.",
    )
    dashboard.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Legacy no-op kept for compatibility. The dashboard no longer triggers rebuilds.",
    )
    dashboard.add_argument(
        "--no-refresh-on-start",
        action="store_true",
        help="Legacy no-op kept for compatibility. The dashboard is always read-only and serves current derived artifacts.",
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
    profile_drop_pngs.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )
    subparsers.add_parser(
        "reset-runs",
        help="Delete all run artifacts and recreate a clean empty runs state.",
    )
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
    prune_runs.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )
    stop_all = subparsers.add_parser(
        "stop-all-runs",
        help="Clear local queued Fuzzfolio research work and optionally stop local autoresearch processes.",
    )
    stop_all.add_argument(
        "--stop-autoresearch",
        action="store_true",
        help="Also stop local autoresearch run/supervise Python processes.",
    )
    stop_all.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )
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
    purge_profiles.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    calc_backtests = subparsers.add_parser(
        "calculate-full-backtests",
        help="Calculate 3yr backtest curves for all attempts that don't have them yet.",
    )
    calc_backtests.add_argument(
        "--run-ids",
        nargs="*",
        default=None,
        help="Specific run IDs to process. Defaults to all runs.",
    )
    calc_backtests.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only process the named attempt id. Can be repeated.",
    )
    calc_backtests.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on how many matched attempts to process after score sorting.",
    )
    calc_backtests.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum concurrent full-backtest jobs. Defaults to the detected running dev Sim Worker count, falling back to validation_max_concurrency.",
    )
    calc_backtests.add_argument(
        "--no-use-dev-sim-worker-count",
        action="store_true",
        help="Disable dev sim-worker auto sizing and fall back to validation_max_concurrency unless --max-workers is set.",
    )
    calc_backtests.add_argument(
        "--require-scrutiny-36",
        action="store_true",
        help="Only backtest attempts that already have 36mo scrutiny artifacts.",
    )
    calc_backtests.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Recalculate even if full-backtest file already exists.",
    )
    calc_backtests.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    attempt_catalog = subparsers.add_parser(
        "build-attempt-catalog",
        help="Build a corpus-wide attempt catalog with scrutiny/cache coverage audit.",
    )
    attempt_catalog.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only catalog the named run id. Can be repeated.",
    )
    attempt_catalog.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    full_backtest_audit = subparsers.add_parser(
        "audit-full-backtests",
        help="Audit current 36mo full-backtest coverage and artifact trust without generating new cache.",
    )
    full_backtest_audit.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only audit the named run id. Can be repeated.",
    )
    full_backtest_audit.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only audit the named attempt id. Can be repeated.",
    )
    full_backtest_audit.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    corpus_tradeoff = subparsers.add_parser(
        "plot-corpus-score-vs-trades",
        help="Render an attempt-level 36mo score vs trades/month scatter plot to runs/derived.",
    )
    corpus_tradeoff.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only include the named run id. Can be repeated.",
    )
    corpus_tradeoff.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only include the named attempt id. Can be repeated.",
    )
    corpus_tradeoff.add_argument(
        "--require-full-backtest-36",
        action="store_true",
        help="Only plot attempts with valid local 36mo full-backtest artifacts.",
    )
    corpus_tradeoff.add_argument(
        "--x-axis-max",
        type=float,
        default=300.0,
        help="Cap the trades/month axis at this value. Use a negative number to disable. Default: 300",
    )
    corpus_tradeoff.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    scrutiny_cache = subparsers.add_parser(
        "hydrate-scrutiny-cache",
        help="Heal or rebuild attempt-local 12mo/36mo scrutiny caches.",
    )
    scrutiny_cache.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only process attempts from the named run id. Can be repeated.",
    )
    scrutiny_cache.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only process the named attempt id. Can be repeated.",
    )
    scrutiny_cache.add_argument(
        "--lookback-months",
        action="append",
        type=int,
        default=None,
        help="Scrutiny horizon in months. Can be repeated. Defaults to 12 and 36.",
    )
    scrutiny_cache.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on how many matched attempts to process after sorting by score.",
    )
    scrutiny_cache.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Ignore existing attempt-local scrutiny artifacts and rebuild them.",
    )
    scrutiny_cache.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    promotion_board = subparsers.add_parser(
        "build-promotion-board",
        help="Build a similarity-aware long-horizon promotion board from attempt-level scrutiny.",
    )
    promotion_board.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only consider attempts from the named run id. Can be repeated.",
    )
    promotion_board.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only consider the named attempt id. Can be repeated.",
    )
    promotion_board.add_argument(
        "--candidate-limit",
        type=int,
        default=250,
        help="Maximum number of candidate attempts to consider after score sorting. Default: 250",
    )
    promotion_board.add_argument(
        "--board-size",
        type=int,
        default=12,
        help="How many promotion candidates to select. Default: 12",
    )
    promotion_board.add_argument(
        "--min-score-36",
        type=float,
        default=40.0,
        help="Minimum 36mo score required for inclusion. Default: 40.0",
    )
    promotion_board.add_argument(
        "--min-retention-ratio",
        type=float,
        default=0.0,
        help="Minimum 36m/12m score retention ratio when 12mo scrutiny exists. Default: 0.0",
    )
    promotion_board.add_argument(
        "--min-trades-per-month",
        type=float,
        default=0.0,
        help="Minimum 36mo trade cadence. Default: 0.0",
    )
    promotion_board.add_argument(
        "--novelty-penalty",
        type=float,
        default=18.0,
        help="Penalty applied to max sameness during greedy board selection. Default: 18.0",
    )
    promotion_board.add_argument(
        "--max-per-run",
        type=int,
        default=2,
        help="Maximum selected candidates per run. Use -1 to disable. Default: 2",
    )
    promotion_board.add_argument(
        "--max-per-strategy-key",
        type=int,
        default=2,
        help="Maximum selected candidates per normalized 36mo timeframe+instrument set. Use -1 to disable. Default: 2",
    )
    promotion_board.add_argument(
        "--max-sameness-to-board",
        type=float,
        default=0.85,
        help="Stop selecting candidates once their max sameness to the current board exceeds this ceiling. Default: 0.85",
    )
    promotion_board.add_argument(
        "--require-full-backtest-36",
        action="store_true",
        help="Only consider attempts with attempt-local 36mo full-backtest artifacts.",
    )
    promotion_board.add_argument(
        "--hydrate-missing",
        action="store_true",
        help="Heal missing 36mo scrutiny for the candidate pool before ranking.",
    )
    promotion_board.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rebuild scrutiny for hydrated candidates instead of reusing caches.",
    )
    promotion_board.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    shortlist_report = subparsers.add_parser(
        "build-shortlist-report",
        help="Build a diversified 36mo shortlist, render charts, and generate official profile-drop PNGs for the selected candidates.",
    )
    shortlist_report.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only consider attempts from the named run id. Can be repeated.",
    )
    shortlist_report.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only consider the named attempt id. Can be repeated.",
    )
    shortlist_report.add_argument(
        "--candidate-limit",
        type=int,
        default=-1,
        help="Optional cap on ranked candidates before similarity/selection. Use -1 for all qualified candidates. Default: -1",
    )
    shortlist_report.add_argument(
        "--shortlist-size",
        type=int,
        default=12,
        help="How many candidates to put on the shortlist. Default: 12",
    )
    shortlist_report.add_argument(
        "--min-score-36",
        type=float,
        default=40.0,
        help="Minimum 36mo score required for shortlist consideration. Default: 40.0",
    )
    shortlist_report.add_argument(
        "--min-retention-ratio",
        type=float,
        default=0.0,
        help="Minimum 36m/12m score retention ratio when 12mo scrutiny exists. Default: 0.0",
    )
    shortlist_report.add_argument(
        "--min-trades-per-month",
        type=float,
        default=0.0,
        help="Minimum 36mo trade cadence. Default: 0.0",
    )
    shortlist_report.add_argument(
        "--max-drawdown-r",
        type=float,
        default=-1.0,
        help="Maximum allowed 36mo drawdown in R. Use -1 to disable. Default: -1",
    )
    shortlist_report.add_argument(
        "--drawdown-penalty",
        type=float,
        default=0.65,
        help="Penalty applied per R of 36mo max drawdown during shortlist selection. Default: 0.65",
    )
    shortlist_report.add_argument(
        "--trade-rate-bonus-weight",
        type=float,
        default=0.0,
        help="Optional positive utility bonus for higher 36mo trade cadence. Default: 0.0",
    )
    shortlist_report.add_argument(
        "--trade-rate-bonus-target",
        type=float,
        default=8.0,
        help="Trade cadence level where the bonus saturates when trade-rate bonus is enabled. Default: 8.0",
    )
    shortlist_report.add_argument(
        "--novelty-penalty",
        type=float,
        default=18.0,
        help="Penalty applied to max sameness during shortlist selection. Default: 18.0",
    )
    shortlist_report.add_argument(
        "--max-per-run",
        type=int,
        default=1,
        help="Maximum shortlisted candidates per run. Use -1 to disable. Default: 1",
    )
    shortlist_report.add_argument(
        "--max-per-strategy-key",
        type=int,
        default=1,
        help="Maximum shortlisted candidates per normalized 36mo timeframe+instrument set. Use -1 to disable. Default: 1",
    )
    shortlist_report.add_argument(
        "--max-sameness-to-board",
        type=float,
        default=0.78,
        help="Stop selecting once a candidate's max sameness to the board exceeds this ceiling. Default: 0.78",
    )
    shortlist_report.add_argument(
        "--require-full-backtest-36",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require valid local 36mo full-backtest artifacts for shortlist candidates. Default: true",
    )
    shortlist_report.add_argument(
        "--generate-profile-drops",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Render official profile-drop PNGs for shortlisted candidates. Default: true",
    )
    shortlist_report.add_argument(
        "--profile-drop-lookback-months",
        type=int,
        default=36,
        help="Lookback used for shortlisted profile-drop PNG generation. Default: 36",
    )
    shortlist_report.add_argument(
        "--chart-trades-x-max",
        type=float,
        default=300.0,
        help="Default cap for trades/month charts. Use a negative number to disable. Default: 300",
    )
    shortlist_report.add_argument(
        "--profile-drop-timeout-seconds",
        type=int,
        default=1800,
        help="Per-candidate timeout for packaging/rendering profile-drop PNGs. Default: 1800",
    )
    shortlist_report.add_argument(
        "--profile-drop-workers",
        type=int,
        default=4,
        help="Concurrent workers for shortlisted profile-drop packaging/rendering. Default: 4",
    )
    shortlist_report.add_argument(
        "--force-rebuild-profile-drops",
        action="store_true",
        help="Re-render shortlisted profile-drop PNGs even if the derived shortlist copies already exist.",
    )
    shortlist_report.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    portfolio_report = subparsers.add_parser(
        "build-portfolio",
        help="Build a config-driven multi-sleeve portfolio report, charts, and optional profile-drop PNGs.",
    )
    portfolio_report.add_argument(
        "--run-id",
        action="append",
        default=None,
        help="Only consider attempts from the named run id. Can be repeated.",
    )
    portfolio_report.add_argument(
        "--attempt-id",
        action="append",
        default=None,
        help="Only consider the named attempt id. Can be repeated.",
    )
    portfolio_report.add_argument(
        "--portfolio-config",
        default=None,
        help="Path to a JSON portfolio config. Defaults to repo-root portfolio.config.json, falling back to built-in defaults if missing.",
    )
    portfolio_report.add_argument(
        "--catch-up-full-backtests",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override the portfolio config and catch up missing 36mo full-backtests before building the portfolio.",
    )
    portfolio_report.add_argument(
        "--catch-up-force-rebuild",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override the portfolio config and force full-backtest rebuilds during the optional catch-up phase.",
    )
    portfolio_report.add_argument(
        "--catch-up-require-scrutiny-36",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override the portfolio config and only catch up attempts that already have 36mo scrutiny.",
    )
    portfolio_report.add_argument(
        "--generate-profile-drops",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override the portfolio config and enable or disable final portfolio profile-drop PNG generation.",
    )
    portfolio_report.add_argument(
        "--export-bundle",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override the portfolio config and export a dated portfolio bundle with selected profiles and rendered drops.",
    )
    portfolio_report.add_argument(
        "--profile-drop-workers",
        type=int,
        default=None,
        help="Override the portfolio config worker count for profile-drop packaging/rendering.",
    )
    portfolio_report.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    export_portfolio_bundle = subparsers.add_parser(
        "export-portfolio-bundle",
        help="Export the latest portfolio selection into a dated derived bundle with profile JSONs and rendered drops.",
    )
    export_portfolio_bundle.add_argument(
        "--portfolio-report",
        default=None,
        help="Path to a portfolio-report.json. Defaults to the latest derived portfolio report.",
    )
    export_portfolio_bundle.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    nuke_deep_caches = subparsers.add_parser(
        "nuke-deep-caches",
        help="Delete rebuildable deep/backtest/scrutiny/profile-drop/derived cache artifacts. Rebuild afterward with build-portfolio.",
    )
    nuke_deep_caches.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )

    score = subparsers.add_parser(
        "score", help="Score one sensitivity artifact directory."
    )
    score.add_argument("artifact_dir", type=Path)

    record = subparsers.add_parser(
        "record-attempt",
        help="Score and append one artifact directory to the attempts ledger.",
    )
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


def _set_trace_console_mode(*, plain_progress: bool, as_json: bool) -> None:
    if plain_progress and not as_json:
        set_runtime_trace_stderr_mode("warnings_only")
        set_provider_trace_stderr_mode("warnings_only")
        return
    set_runtime_trace_stderr_mode("verbose")
    set_provider_trace_stderr_mode("verbose")


def _plain_separator(label: str | None = None, *, fill: str = "-") -> str:
    width = 110
    fill_char = str(fill or "-")[:1] or "-"
    if not label:
        return fill_char * width
    compact = _short_text(label, 96)
    decorated = f"{fill_char * 4} {compact} "
    return decorated + (fill_char * max(0, width - len(decorated)))


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
    PLAIN_PROGRESS_STATE["best_score"] = None
    PLAIN_PROGRESS_STATE["last_score"] = None
    PLAIN_PROGRESS_STATE["run_id"] = event.get("run_id")
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


def _coerce_score(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "n/a"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_result_score(result: dict[str, object]) -> float | None:
    auto_log = result.get("auto_log")
    if isinstance(auto_log, dict):
        status = str(auto_log.get("status") or "").strip().lower()
        if status == "logged":
            score = _coerce_score(auto_log.get("composite_score"))
            if score is not None:
                return score
        if status == "existing":
            attempt = auto_log.get("attempt")
            if isinstance(attempt, dict):
                score = _coerce_score(attempt.get("composite_score"))
                if score is not None:
                    return score
    payload = result.get("result")
    if isinstance(payload, dict):
        score = _coerce_score(payload.get("composite_score"))
        if score is not None:
            return score
    score = _coerce_score(result.get("score"))
    if score is not None:
        return score
    return None


def _warning_count(step_payload: dict[str, Any]) -> int:
    count = 0
    results = step_payload.get("results")
    if not isinstance(results, list):
        return count
    for result in results:
        if not isinstance(result, dict):
            continue
        tool = str(result.get("tool", ""))
        if result.get("error"):
            count += 1
            continue
        if tool in CLI_OK_TOOLS and not bool(result.get("ok", True)):
            count += 1
            continue
        if tool in {"yield_guard", "step_guard", "response_guard"}:
            count += 1
    return count


def _step_focus_text(step_payload: dict[str, Any]) -> str:
    reasoning = " ".join(str(step_payload.get("reasoning", "")).split()).strip()
    if reasoning:
        sentence = reasoning.split(". ", 1)[0].strip()
        if sentence:
            if not sentence.endswith(".") and len(sentence) < len(reasoning):
                sentence += "."
            return _short_text(sentence, 180)
        return _short_text(reasoning, 180)
    actions = step_payload.get("actions")
    if isinstance(actions, list) and actions:
        first = actions[0]
        if isinstance(first, dict):
            return _short_text(_summarize_action(first), 180)
    return ""


def _step_header_label(step_payload: dict[str, Any]) -> str:
    results = step_payload.get("results")
    if isinstance(results, list):
        for result in results:
            if not isinstance(result, dict):
                continue
            score = _extract_result_score(result)
            if score is None:
                continue
            PLAIN_PROGRESS_STATE["last_score"] = score
            best_score = _coerce_score(PLAIN_PROGRESS_STATE.get("best_score"))
            if best_score is None or score > best_score:
                PLAIN_PROGRESS_STATE["best_score"] = score
    best_score = _coerce_score(PLAIN_PROGRESS_STATE.get("best_score"))
    last_score = _coerce_score(PLAIN_PROGRESS_STATE.get("last_score"))
    action_count = (
        len(step_payload.get("actions", []))
        if isinstance(step_payload.get("actions"), list)
        else 0
    )
    warning_count = _warning_count(step_payload)
    parts = [f"Step {step_payload.get('step')}"]
    parts.append(f"best={best_score:.4f}" if best_score is not None else "best=n/a")
    if last_score is not None:
        parts.append(f"last={last_score:.4f}")
    parts.append(f"actions={action_count}")
    if warning_count:
        parts.append(f"warnings={warning_count}")
    return " | ".join(parts)


def _plain_result_details(result: dict[str, object]) -> list[str]:
    details: list[str] = []
    tool = str(result.get("tool", ""))
    error = result.get("error")
    if error:
        details.append(f"error: {str(error)}")
    payload = result.get("result")
    if isinstance(payload, dict):
        stderr = payload.get("stderr")
        if isinstance(stderr, str) and stderr.strip():
            for line in stderr.splitlines():
                text = line.strip()
                if text:
                    details.append(f"stderr: {text}")
        if (
            error or (tool in CLI_OK_TOOLS and not bool(result.get("ok", True)))
        ) and isinstance(payload.get("stdout"), str):
            stdout = str(payload.get("stdout")).strip()
            if stdout:
                for line in stdout.splitlines():
                    text = line.strip()
                    if text:
                        details.append(f"stdout: {text}")
    if tool == "yield_guard":
        message = str(result.get("message") or "").strip()
        if message:
            details.append(f"warning: {message}")
        questions = result.get("questions")
        if isinstance(questions, list):
            for item in questions:
                text = str(item).strip()
                if text:
                    details.append(f"question: {text}")
        next_moves = result.get("next_moves")
        if isinstance(next_moves, list):
            for item in next_moves:
                text = str(item).strip()
                if text:
                    details.append(f"next: {text}")
    if tool in {"step_guard", "response_guard"}:
        message = str(result.get("message") or result.get("error") or "").strip()
        if message:
            details.append(f"warning: {message}")
    return details


def _summarize_manager_event(event: dict[str, object]) -> str:
    hook = str(event.get("hook") or "unknown")
    status = str(event.get("status") or "unknown")
    parts = [f"{hook} | status={status}"]
    action_count = event.get("action_count")
    if isinstance(action_count, int):
        parts.append(f"actions={action_count}")
    error = str(event.get("error") or "").strip()
    if error:
        parts.append(f"error={_short_text(error, 120)}")
    return " | ".join(parts)


def _plain_manager_event_details(event: dict[str, object]) -> list[str]:
    details: list[str] = []
    rationale = str(event.get("rationale") or "").strip()
    if rationale:
        details.append(f"rationale: {_short_text(rationale, 300)}")
    return details


def _render_step_plain(step_payload: dict[str, Any]) -> None:
    _write_plain_line(_plain_separator(_step_header_label(step_payload)))
    focus = _step_focus_text(step_payload)
    if focus:
        _write_plain_line(f"focus: {focus}")
    actions = step_payload.get("actions")
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict):
                _write_plain_line(f"plan: {_summarize_action(action)}")
    manager_events = step_payload.get("manager_events")
    if isinstance(manager_events, list):
        for event in manager_events:
            if isinstance(event, dict):
                _write_plain_line(f"manager: {_summarize_manager_event(event)}")
                for detail in _plain_manager_event_details(event):
                    _write_plain_line(detail)
    results = step_payload.get("results")
    if isinstance(results, list):
        for result in results:
            if isinstance(result, dict):
                _write_plain_line(f"result: {_summarize_result(result)}")
                for detail in _plain_result_details(result):
                    _write_plain_line(detail)


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
        "explorer_uses_managed_auth": config.provider.provider_type.strip().lower()
        == "codex",
        "explorer_compact_trigger_tokens": config.compact_trigger_tokens_for(
            config.llm.explorer_profile
        ),
        "supervise_max_steps": config.supervise.max_steps,
        "supervise_window_enabled": config.supervise.window_enabled,
        "supervise_window_start": config.supervise.window_start,
        "supervise_window_end": config.supervise.window_end,
        "supervise_timezone": config.supervise.timezone,
        "supervise_soft_wrap_minutes": config.supervise.soft_wrap_minutes,
        "supervise_auto_restart_terminal_sessions": config.supervise.auto_restart_terminal_sessions,
        "manager_enabled": config.manager.enabled,
        "manager_profiles": config.manager.profiles,
        "manager_max_candidate_families_in_packet": config.manager.max_candidate_families_in_packet,
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
        return {
            "ok": False,
            "warning": "Trading-Dashboard harness directory was not found.",
        }

    queue_keys = ["QUEUE:sweep_jobs", "QUEUE:deep_replay_jobs", "QUEUE:sim_jobs"]
    deleted: list[dict[str, Any]] = []
    for key in queue_keys:
        completed = subprocess.run(
            [
                "uv",
                "run",
                "cli.py",
                "--env",
                ".env.redis",
                "redis",
                "kv",
                "del",
                "--key",
                key,
            ],
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


def _extract_cloud_profiles(
    payload: dict[str, Any] | list[Any] | None,
) -> list[dict[str, Any]]:
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
        "is_active": bool(profile.get("isActive"))
        if isinstance(profile.get("isActive"), bool)
        else None,
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
        payload["message"] = (
            "Dry run only. Re-run with --yes to delete these saved cloud profiles."
        )
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
            deleted.append(
                {
                    "id": profile_id,
                    "name": str((item.get("profile") or {}).get("name") or ""),
                }
            )
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


def _provider_test_scenarios() -> list[
    tuple[str, list[ChatMessage], Callable[[dict[str, Any]], str | None]]
]:
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
                        "Return exactly this JSON object and nothing else: "
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
                        "Return a JSON object with exactly these top-level fields: "
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
        compact = compact.encode(DISPLAY_ENCODING, errors="replace").decode(
            DISPLAY_ENCODING
        )
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _set_display_context(
    *, repo_root: Path | None = None, run_dir: Path | None = None
) -> None:
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
):
    config = load_config()
    effective_explorer = explorer_profile or config.llm.explorer_profile

    missing: list[str] = []
    if effective_explorer not in config.providers:
        missing.append(f"explorer profile {effective_explorer!r}")
    if missing:
        raise SystemExit(f"Unknown provider profile override(s): {', '.join(missing)}")

    config.llm.explorer_profile = effective_explorer
    return config


def _resolve_supervise_policy(
    config,
    *,
    max_steps: int | None,
    window: str | None,
    no_window: bool,
    timezone_name: str | None,
) -> tuple[int, RunPolicy]:
    cfg = config.supervise
    window_start, window_end = _parse_window(window)
    effective_max_steps = max_steps or cfg.max_steps or config.research.max_steps
    window_enabled = bool(cfg.window_enabled) and not no_window
    effective_window_start = (
        None
        if not window_enabled
        else (window_start if window_start is not None else cfg.window_start)
    )
    effective_window_end = (
        None
        if not window_enabled
        else (window_end if window_end is not None else cfg.window_end)
    )
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
            return (
                f"run_cli {' '.join(_display_value(str(item)) for item in args[:14])}"
            )
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
    if tool == "run_cli" or tool in CLI_OK_TOOLS:
        ok = bool(result.get("ok"))
        status = "ok" if ok else "failed"
        parts = [f"{tool} {status}"]
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
                    parts.append(
                        f"attempt=existing score={attempt.get('composite_score')}"
                    )
        if tool == "evaluate_candidate" and result.get("score") is not None:
            parts.append(f"typed_score={result.get('score')}")
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
        count = (
            len(result.get("items", [])) if isinstance(result.get("items"), list) else 0
        )
        return f"list_dir ok | items={count}"
    if tool == "log_attempt":
        payload = result.get("result")
        if isinstance(payload, dict):
            if payload.get("status") == "existing":
                attempt = payload.get("attempt")
                if isinstance(attempt, dict):
                    return (
                        f"log_attempt existing | score={attempt.get('composite_score')}"
                    )
            return f"log_attempt {payload.get('status')} | score={payload.get('composite_score')}"
    if tool == "yield_guard":
        base = str(result.get("message", ""))
        parts = [f"yield_guard | {_short_text(base, 300)}"]
        score_target = result.get("score_target")
        if isinstance(score_target, str) and score_target.strip():
            parts.append("target: " + _short_text(score_target, 140))
        questions = result.get("questions")
        if isinstance(questions, list) and questions:
            parts.append(
                "q: "
                + " / ".join(_short_text(str(item), 120) for item in questions[:2])
            )
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
    if tool in CLI_OK_TOOLS:
        return "green" if bool(result.get("ok")) else "bold red"
    return "cyan"


def _manager_event_style(event: dict[str, object]) -> str:
    status = str(event.get("status") or "").strip().lower()
    if status == "ok":
        return "green"
    if status == "no_change":
        return "cyan"
    if status == "partial":
        return "bold yellow"
    if status == "failed":
        return "bold red"
    return "white"


def _action_style(action: dict[str, object]) -> str:
    tool = str(action.get("tool", "unknown"))
    if tool == "finish":
        return "magenta"
    if tool in CLI_OK_TOOLS:
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

    manager_events = step_payload.get("manager_events")
    if isinstance(manager_events, list) and manager_events:
        manager_table = Table(box=box.SIMPLE_HEAVY, expand=True)
        manager_table.add_column("Manager", style="bold magenta", width=10)
        manager_table.add_column("Detail", style="white")
        for event in manager_events:
            if isinstance(event, dict):
                style = _manager_event_style(event)
                manager_table.add_row(
                    Text("manager", style=style),
                    Text(_summarize_manager_event(event), style=style),
                )
                rationale = str(event.get("rationale") or "").strip()
                if rationale:
                    manager_table.add_row(
                        Text("", style=style),
                        Text(
                            "rationale: " + _short_text(rationale, 320),
                            style=style,
                        ),
                    )
        body.append(manager_table)

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
                result_table.add_row(
                    Text(label, style=style),
                    Text(_summarize_result(result), style=style),
                )
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


def _render_context_compaction_plain(event: dict[str, object]) -> None:
    step = event.get("step")
    before = event.get("approx_tokens_before")
    after = event.get("approx_tokens_after")
    if step is not None:
        label = f"compaction step {step}: ~{before} tok before, ~{after} tok after"
    else:
        label = f"compaction: ~{before} tok before, ~{after} tok after"
    _write_plain_line(_plain_separator(label, fill="="))


def _render_context_compaction_rich(event: dict[str, object]) -> None:
    step = event.get("step")
    before = event.get("approx_tokens_before")
    after = event.get("approx_tokens_after")
    trig = event.get("compact_trigger_tokens")
    lines = [f"Approx prompt tokens: ~{before} → ~{after}"]
    if trig is not None:
        lines.append(f"Compaction trigger: {trig}")
    console.print(
        Panel(
            Text("\n".join(lines), style="white"),
            title=f"[bold magenta]Context compaction[/bold magenta] (step {step})",
            border_style="magenta",
            box=box.ROUNDED,
        )
    )


def _render_context_compaction(event: dict[str, object]) -> None:
    _safe_render(
        lambda: _render_context_compaction_rich(event),
        lambda: _render_context_compaction_plain(event),
    )


def _emit_run_progress(event: dict[str, object]) -> None:
    kind = event.get("event")
    if kind == "context_compaction":
        _render_context_compaction(event)
        return
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
    as_json: bool,
    plain_progress: bool,
) -> int:
    _set_plain_progress_mode(plain_progress and not as_json)
    _set_trace_console_mode(plain_progress=plain_progress, as_json=as_json)
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
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
    as_json: bool,
    plain_progress: bool,
) -> int:
    _set_plain_progress_mode(plain_progress and not as_json)
    _set_trace_console_mode(plain_progress=plain_progress, as_json=as_json)
    config = _load_runtime_config(
        explorer_profile=explorer_profile,
    )
    _set_display_context(repo_root=config.repo_root, run_dir=None)
    session_max_steps, policy = _resolve_supervise_policy(
        config,
        max_steps=max_steps,
        window=window,
        no_window=no_window,
        timezone_name=timezone_name,
    )
    auto_restart_terminal = bool(config.supervise.auto_restart_terminal_sessions)
    session_results: list[dict[str, Any]] = []
    stop_reason = "window_closed"
    while True:
        within_window, minutes_remaining = _window_state(policy)
        if not within_window:
            stop_reason = "window_closed"
            break
        if session_results and policy.soft_wrap_minutes > 0:
            if (
                minutes_remaining is not None
                and minutes_remaining <= policy.soft_wrap_minutes
            ):
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
    run_metadata_by_run_id = (
        {
            run_dir.name: load_run_metadata(run_dir)
            for run_dir in sorted(
                path
                for path in config.runs_root.iterdir()
                if path.is_dir() and path.name != "derived"
            )
        }
        if config.runs_root.exists()
        else {}
    )
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
    emit(
        f"leaderboard: validating {len(analysis_ranked)} best-per-run leaders at 12mo and 36mo"
    )
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
        emit(
            f"leaderboard: skipped {skipped_validation_rows} validation candidate(s) after recoverable errors"
        )
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
                "validation_leaderboard_json": str(
                    config.validation_leaderboard_json_path
                ),
                "validation_scatter_plot": str(config.validation_scatter_plot_path),
                "validation_delta_plot": str(config.validation_delta_plot_path),
                "similarity_leaders": len(similarity_leaders),
                "similarity_pairs": len(similarity_rendered.get("pairs") or []),
                "similarity_leaderboard_json": str(
                    config.similarity_leaderboard_json_path
                ),
                "similarity_heatmap_plot": str(config.similarity_heatmap_plot_path),
                "similarity_scatter_plot": str(config.similarity_scatter_plot_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_dashboard(
    *, host: str, port: int, limit: int, refresh_on_start: bool, force_rebuild: bool
) -> int:
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
        return Path(resolved).resolve(), next(
            iter(_trading_dashboard_roots(config)), None
        )

    exe_name = (
        "fuzzfolio-drop-renderer.exe" if os.name == "nt" else "fuzzfolio-drop-renderer"
    )
    for workspace_root in _trading_dashboard_roots(config):
        candidate = (
            workspace_root
            / "harness"
            / "fuzzfolio_drop_renderer"
            / "cli"
            / "target"
            / "release"
            / exe_name
        )
        if candidate.exists():
            return candidate.resolve(), workspace_root
    raise FileNotFoundError(
        "Could not resolve fuzzfolio-drop-renderer. Set AUTORESEARCH_DROP_RENDERER or build the renderer under Trading-Dashboard."
    )


def _run_external(
    argv: list[str], *, cwd: Path, timeout_seconds: float | None = None
) -> None:
    try:
        proc = subprocess.run(
            argv,
            cwd=str(cwd),
            text=True,
            capture_output=True,
            encoding="utf-8",
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {timeout_seconds:.0f}s: {' '.join(argv)}\n"
            f"cwd: {cwd}\n"
            f"stdout:\n{(exc.stdout or '').strip()[:1600]}\n\n"
            f"stderr:\n{(exc.stderr or '').strip()[:1600]}"
        ) from exc
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


def _best_attempt_for_run(
    attempts: list[dict[str, Any]], *, lower_is_better: bool = False
) -> dict[str, Any] | None:
    scored = [
        attempt for attempt in attempts if attempt.get("composite_score") is not None
    ]
    if not scored:
        return None
    return sorted(
        scored,
        key=lambda attempt: float(attempt.get("composite_score")),
        reverse=not lower_is_better,
    )[0]


def _matching_run_dirs(
    config, run_ids: list[str] | None = None
) -> list[Path]:
    all_run_dirs = list_run_dirs(config.runs_root)
    if not run_ids:
        return all_run_dirs
    wanted = {token.strip() for token in run_ids if str(token).strip()}
    run_dirs = [run_dir for run_dir in all_run_dirs if run_dir.name in wanted]
    missing = sorted(wanted - {run_dir.name for run_dir in run_dirs})
    if missing:
        raise SystemExit(f"Run directories do not exist: {', '.join(missing)}")
    return run_dirs


def _catalog_rows_for_run_dirs(
    config,
    run_dirs: list[Path],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    run_metadata_by_run_id = {
        run_dir.name: load_run_metadata(run_dir) for run_dir in run_dirs
    }
    total_runs = len(run_dirs)
    if progress_callback is not None:
        progress_callback({"stage": "start", "total_runs": total_runs})
    rows: list[dict[str, Any]] = []
    for index, run_dir in enumerate(run_dirs, start=1):
        run_metadata = run_metadata_by_run_id.get(run_dir.name) or {}
        attempts = load_run_attempts(run_dir)
        for attempt in attempts:
            rows.append(
                extract_attempt_catalog_row(
                    attempt,
                    run_metadata,
                    validation_cache_root=config.validation_cache_root,
                )
            )
        if progress_callback is not None:
            progress_callback(
                {
                    "stage": "progress",
                    "completed_runs": index,
                    "total_runs": total_runs,
                    "run_id": run_dir.name,
                    "attempt_count": len(attempts),
                    "row_count": len(rows),
                }
            )
    rows.sort(
        key=lambda row: (
            row.get("composite_score") is None,
            -(float(row["composite_score"]) if row.get("composite_score") is not None else float("-inf")),
            str(row.get("attempt_id") or ""),
        )
    )
    return rows


def _refresh_global_derived_corpus_state(config) -> dict[str, Any]:
    run_dirs = _matching_run_dirs(config, None)
    rows = _catalog_rows_for_run_dirs(config, run_dirs)
    summary = catalog_summary(rows)
    audit_payload = build_full_backtest_audit(rows)
    write_json(config.attempt_catalog_json_path, rows)
    write_csv(config.attempt_catalog_csv_path, rows)
    write_json(config.attempt_catalog_summary_path, summary)
    write_json(config.full_backtest_audit_json_path, audit_payload)
    return {
        "run_count": len(run_dirs),
        "attempt_count": len(rows),
        "attempt_catalog_json": str(config.attempt_catalog_json_path),
        "attempt_catalog_csv": str(config.attempt_catalog_csv_path),
        "attempt_catalog_summary_json": str(config.attempt_catalog_summary_path),
        "full_backtest_audit_json": str(config.full_backtest_audit_json_path),
        "summary": summary,
        "audit": audit_payload,
    }


def _matched_attempt_items(
    config,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
    require_scored: bool = True,
) -> list[tuple[Path, list[dict[str, Any]], dict[str, Any]]]:
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }
    items: list[tuple[Path, list[dict[str, Any]], dict[str, Any]]] = []
    for run_dir in _matching_run_dirs(config, run_ids):
        attempts = load_run_attempts(run_dir)
        if not attempts:
            continue
        for attempt in attempts:
            attempt_id = str(attempt.get("attempt_id") or "").strip()
            if wanted_attempt_ids and attempt_id not in wanted_attempt_ids:
                continue
            if require_scored and not wanted_attempt_ids and attempt.get("composite_score") is None:
                continue
            items.append((run_dir, attempts, attempt))
    if wanted_attempt_ids:
        matched_attempt_ids = {
            str(attempt.get("attempt_id") or "").strip() for _, _, attempt in items
        }
        missing = sorted(wanted_attempt_ids - matched_attempt_ids)
        if missing:
            raise SystemExit(f"Attempt ids do not exist: {', '.join(missing)}")
    items.sort(
        key=lambda item: (
            item[2].get("composite_score") is None,
            -(
                float(item[2]["composite_score"])
                if item[2].get("composite_score") is not None
                else float("-inf")
            ),
            str(item[2].get("attempt_id") or ""),
        )
    )
    return items


def _full_backtest_priority_key(row: dict[str, Any] | None) -> tuple[bool, float, float, str]:
    row = row or {}
    score_36 = row.get("score_36m")
    composite_score = row.get("composite_score")
    primary = (
        float(score_36)
        if score_36 is not None
        else (
            float(composite_score)
            if composite_score is not None
            else float("-inf")
        )
    )
    secondary = (
        float(composite_score)
        if composite_score is not None
        else float("-inf")
    )
    return (
        primary == float("-inf"),
        -primary,
        -secondary,
        str(row.get("attempt_id") or ""),
    )


def _detect_dev_sim_worker_count(*, timeout_seconds: float = 2.0) -> int | None:
    try:
        with urllib.request.urlopen(
            "http://127.0.0.1:47821/processes", timeout=timeout_seconds
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (
        OSError,
        TimeoutError,
        urllib.error.URLError,
        urllib.error.HTTPError,
        json.JSONDecodeError,
    ):
        return None
    if not isinstance(payload, list):
        return None
    count = 0
    for item in payload:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        name = str(item.get("name") or "").strip().lower()
        command = str(item.get("command") or "").strip().lower()
        if status != "running":
            continue
        if name.startswith("sim worker") or command.endswith("sim-worker"):
            count += 1
    return count if count > 0 else None


def _run_full_backtest_with_retry(
    config,
    attempt: dict[str, Any],
    *,
    job_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    if artifact_dir.exists():
        source_payload = resolve_attempt_scrutiny_source(
            attempt,
            36,
            validation_cache_root=config.validation_cache_root,
        )
        source_name = str(source_payload.get("source") or "")
        result_path_raw = str(source_payload.get("result_path") or "").strip()
        curve_path_raw = str(source_payload.get("curve_path") or "").strip()
        if (
            source_payload.get("available")
            and source_name in {"attempt_scrutiny_cache", "legacy_run_validation_cache"}
            and result_path_raw
            and curve_path_raw
        ):
            source_result_path = Path(result_path_raw)
            source_curve_path = Path(curve_path_raw)
            if source_result_path.exists() and source_curve_path.exists():
                dest_curve = artifact_dir / "full-backtest-36mo-curve.json"
                dest_result = artifact_dir / "full-backtest-36mo-result.json"
                shutil.copy2(source_curve_path, dest_curve)
                shutil.copy2(source_result_path, dest_result)
                return {
                    "curve_path": str(dest_curve),
                    "result_path": str(dest_result),
                    "seed_source": source_name,
                }
    try:
        return _run_full_backtest_for_attempt(
            config, attempt, job_timeout_seconds=job_timeout_seconds
        )
    except Exception as exc:
        message = str(exc)
        profile_path_raw = str(attempt.get("profile_path") or "").strip()
        profile_path = Path(profile_path_raw) if profile_path_raw else None
        should_retry_with_local_profile = (
            (
                "Selected-cell detail has not been computed yet" in message
                or "Profile not found" in message
            )
            and profile_path is not None
            and profile_path.exists()
        )
        if not should_retry_with_local_profile:
            raise
    retry_attempt = dict(attempt)
    retry_attempt["profile_ref"] = ""
    result = _run_full_backtest_for_attempt(
        config, retry_attempt, job_timeout_seconds=job_timeout_seconds
    )
    result["retry_mode"] = "local_profile_reupload"
    return result


def _classify_full_backtest_failure(error_message: str) -> str:
    message = str(error_message or "").lower()
    if "profile not found" in message:
        return "profile_not_found"
    if "selected-cell detail has not been computed yet" in message:
        return "selected_cell_detail_pending"
    if "command timed out" in message:
        return "timeout"
    if "artifact directory does not exist" in message:
        return "missing_artifact_dir"
    if "missing a valid cloud profile ref and local profile file" in message:
        return "missing_profile_material"
    if "sensitivity-basket failed" in message:
        return "sensitivity_basket_failed"
    return "other"


def _build_full_backtest_failure_summary(
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    failed_rows = [row for row in results if str(row.get("status") or "") == "failed"]
    by_reason: dict[str, int] = {}
    examples: dict[str, list[dict[str, Any]]] = {}
    for row in failed_rows:
        reason = _classify_full_backtest_failure(str(row.get("error") or ""))
        by_reason[reason] = by_reason.get(reason, 0) + 1
        bucket = examples.setdefault(reason, [])
        if len(bucket) >= 20:
            continue
        bucket.append(
            {
                "run_id": row.get("run_id"),
                "attempt_id": row.get("attempt_id"),
                "candidate_name": row.get("candidate_name"),
                "duration_seconds": row.get("duration_seconds"),
                "error": row.get("error"),
            }
        )
    recovery_summary = {
        "seeded_materialized": sum(
            1 for row in results if str(row.get("status") or "") == "seeded"
        ),
        "local_profile_reupload": sum(
            1
            for row in results
            if str(row.get("retry_mode") or "") == "local_profile_reupload"
        ),
        "timeout_salvaged": sum(
            1 for row in results if str(row.get("recovery_mode") or "") == "timeout_salvaged"
        ),
    }
    return {
        "failed_count": len(failed_rows),
        "failure_reasons": by_reason,
        "failure_examples": examples,
        "recovery_summary": recovery_summary,
    }


def _trade_rate_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(
        float(value)
        for value in (_safe_float_value(row.get("trades_per_month_36m")) for row in rows)
        if value is not None
    )
    if not values:
        return {
            "count": 0,
            "min": None,
            "median": None,
            "mean": None,
            "max": None,
            "under_1_per_month": 0,
            "at_least_1_per_month": 0,
            "at_least_2_per_month": 0,
            "at_least_4_per_month": 0,
        }
    count = len(values)
    midpoint = count // 2
    median = (
        values[midpoint]
        if count % 2 == 1
        else (values[midpoint - 1] + values[midpoint]) / 2.0
    )
    return {
        "count": count,
        "min": values[0],
        "median": median,
        "mean": sum(values) / count,
        "max": values[-1],
        "under_1_per_month": sum(1 for value in values if value < 1.0),
        "at_least_1_per_month": sum(1 for value in values if value >= 1.0),
        "at_least_2_per_month": sum(1 for value in values if value >= 2.0),
        "at_least_4_per_month": sum(1 for value in values if value >= 4.0),
    }


def _curve_terminal_realized_r(curve_path: Path | None) -> float | None:
    if curve_path is None or not curve_path.exists():
        return None
    payload = _load_json_if_exists(curve_path)
    points = _nested_get(payload, ["curve", "points"])
    if not isinstance(points, list):
        return None
    last_value: float | None = None
    for point in points:
        if not isinstance(point, dict):
            continue
        value = _safe_float_value(point.get("realized_r"))
        if value is None:
            continue
        last_value = value
    return last_value


def _summary_from_values(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "mean": None, "median": None, "max": None, "sum": None}
    ordered = sorted(values)
    count = len(ordered)
    midpoint = count // 2
    median = (
        ordered[midpoint]
        if count % 2 == 1
        else (ordered[midpoint - 1] + ordered[midpoint]) / 2.0
    )
    return {
        "count": count,
        "min": ordered[0],
        "mean": sum(ordered) / count,
        "median": median,
        "max": ordered[-1],
        "sum": sum(ordered),
    }


def _build_selection_basket_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    realized_r_per_month_values: list[float] = []
    realized_r_total_values: list[float] = []
    drawdown_values: list[float] = []
    drawdown_per_month_values: list[float] = []
    trades_per_month_values: list[float] = []
    score_values: list[float] = []

    for row in rows:
        trades_per_month = _safe_float_value(row.get("trades_per_month_36m"))
        if trades_per_month is not None:
            trades_per_month_values.append(trades_per_month)
        score_36 = _safe_float_value(row.get("score_36m"))
        if score_36 is not None:
            score_values.append(score_36)
        drawdown_r = _safe_float_value(row.get("max_drawdown_r_36m"))
        if drawdown_r is not None:
            drawdown_values.append(drawdown_r)

        effective_window = _safe_float_value(row.get("effective_window_months_36m"))
        curve_path_raw = row.get("full_backtest_curve_path_36m") or row.get("scrutiny_curve_path_36m")
        curve_path = Path(str(curve_path_raw)).resolve() if curve_path_raw else None
        realized_r_total = _curve_terminal_realized_r(curve_path)
        if realized_r_total is not None:
            realized_r_total_values.append(realized_r_total)
            if effective_window and effective_window > 0:
                realized_r_per_month_values.append(realized_r_total / effective_window)
        if drawdown_r is not None and effective_window and effective_window > 0:
            drawdown_per_month_values.append(drawdown_r / effective_window)

    return {
        "strategy_count": len(rows),
        "trades_per_month": _summary_from_values(trades_per_month_values),
        "score_36m": _summary_from_values(score_values),
        "realized_r_total_36m": _summary_from_values(realized_r_total_values),
        "realized_r_per_month_36m": _summary_from_values(realized_r_per_month_values),
        "max_drawdown_r_36m": _summary_from_values(drawdown_values),
        "max_drawdown_r_per_month_36m": _summary_from_values(drawdown_per_month_values),
    }


def _coerce_curve_points(curve_path: Path | None) -> list[dict[str, Any]]:
    if curve_path is None or not curve_path.exists():
        return []
    payload = _load_json_if_exists(curve_path)
    points = _nested_get(payload, ["curve", "points"])
    if not isinstance(points, list):
        return []
    normalized: list[dict[str, Any]] = []
    for point in points:
        if not isinstance(point, dict):
            continue
        timestamp_value = point.get("time")
        date_value = point.get("date")
        timestamp: int | None = None
        try:
            if timestamp_value is not None:
                timestamp = int(timestamp_value)
        except (TypeError, ValueError):
            timestamp = None
        if timestamp is None and date_value:
            try:
                timestamp = int(datetime.fromisoformat(str(date_value)).timestamp())
            except ValueError:
                timestamp = None
        if timestamp is None:
            continue
        normalized.append(
            {
                "time": timestamp,
                "date": str(date_value or ""),
                "equity_r": _safe_float_value(point.get("equity_r")) or 0.0,
                "drawdown_r": _safe_float_value(point.get("drawdown_r")) or 0.0,
                "realized_r": _safe_float_value(
                    point.get("cumulative_realized_r")
                )
                if _safe_float_value(point.get("cumulative_realized_r")) is not None
                else (_safe_float_value(point.get("realized_r")) or 0.0),
                "closed_trade_count": int(
                    _safe_float_value(point.get("closed_trade_count")) or 0
                ),
            }
        )
    normalized.sort(key=lambda item: (int(item.get("time") or 0), str(item.get("date") or "")))
    return normalized


def _build_selection_basket_curve(rows: list[dict[str, Any]]) -> dict[str, Any]:
    curve_series: list[list[dict[str, Any]]] = []
    strategy_count = 0
    for row in rows:
        curve_path_raw = row.get("full_backtest_curve_path_36m") or row.get(
            "scrutiny_curve_path_36m"
        )
        curve_path = Path(str(curve_path_raw)).resolve() if curve_path_raw else None
        points = _coerce_curve_points(curve_path)
        if not points:
            continue
        curve_series.append(points)
        strategy_count += 1

    if not curve_series:
        return {
            "strategy_count": 0,
            "point_count": 0,
            "points": [],
            "max_equity_r": None,
            "max_drawdown_r": None,
            "final_equity_r": None,
            "final_drawdown_r": None,
            "final_realized_r": None,
            "final_closed_trade_count": None,
        }

    all_timestamps = sorted({int(point["time"]) for series in curve_series for point in series})
    per_series_indexes = [0 for _ in curve_series]
    per_series_states = [
        {
            "equity_r": 0.0,
            "drawdown_r": 0.0,
            "realized_r": 0.0,
            "closed_trade_count": 0,
        }
        for _ in curve_series
    ]
    basket_points: list[dict[str, Any]] = []
    max_equity_r: float | None = None
    max_drawdown_r: float | None = None
    for timestamp in all_timestamps:
        for index, series in enumerate(curve_series):
            series_index = per_series_indexes[index]
            while series_index < len(series) and int(series[series_index]["time"]) <= timestamp:
                point = series[series_index]
                per_series_states[index] = {
                    "equity_r": float(point.get("equity_r") or 0.0),
                    "drawdown_r": float(point.get("drawdown_r") or 0.0),
                    "realized_r": float(point.get("realized_r") or 0.0),
                    "closed_trade_count": int(point.get("closed_trade_count") or 0),
                }
                series_index += 1
            per_series_indexes[index] = series_index
        equity_r = sum(float(state["equity_r"]) for state in per_series_states)
        drawdown_r = sum(float(state["drawdown_r"]) for state in per_series_states)
        realized_r = sum(float(state["realized_r"]) for state in per_series_states)
        closed_trade_count = sum(
            int(state["closed_trade_count"]) for state in per_series_states
        )
        point_date = datetime.fromtimestamp(timestamp).date().isoformat()
        basket_points.append(
            {
                "time": timestamp,
                "date": point_date,
                "equity_r": round(equity_r, 6),
                "drawdown_r": round(drawdown_r, 6),
                "realized_r": round(realized_r, 6),
                "closed_trade_count": closed_trade_count,
            }
        )
        max_equity_r = equity_r if max_equity_r is None else max(max_equity_r, equity_r)
        max_drawdown_r = (
            drawdown_r if max_drawdown_r is None else max(max_drawdown_r, drawdown_r)
        )

    final_point = basket_points[-1]
    return {
        "strategy_count": strategy_count,
        "point_count": len(basket_points),
        "points": basket_points,
        "max_equity_r": round(max_equity_r, 6) if max_equity_r is not None else None,
        "max_drawdown_r": round(max_drawdown_r, 6) if max_drawdown_r is not None else None,
        "final_equity_r": final_point["equity_r"],
        "final_drawdown_r": final_point["drawdown_r"],
        "final_realized_r": final_point["realized_r"],
        "final_closed_trade_count": final_point["closed_trade_count"],
    }


def _nested_get(payload: dict[str, Any], path: list[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _safe_float_value(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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
        token = (
            str(_nested_get(indicator, ["config", "timeframe"]) or "").strip().upper()
        )
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
    request_payload = (
        _nested_get(
            _load_json_if_exists(artifact_dir / "deep-replay-job.json"), ["request"]
        )
        or {}
    )
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


def _find_attempt_for_profile_ref(
    attempts: list[dict[str, Any]], profile_ref: str
) -> dict[str, Any] | None:
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
    base_profile_path = (
        Path(base_profile_path_raw).resolve() if base_profile_path_raw else None
    )
    base_request_payload = _attempt_request_payload(base_attempt)

    instruments = _normalize_tokens(list(sweep_payload.get("instruments") or []))
    if not instruments and base_profile_path is not None and base_profile_path.exists():
        instruments = _coerce_profile_instruments(base_profile_path)

    timeframe = (
        str(
            base_request_payload.get("timeframe")
            or _nested_get(base_attempt, ["best_summary", "timeframe"])
            or (
                _coerce_profile_timeframe(base_profile_path)
                if base_profile_path is not None and base_profile_path.exists()
                else ""
            )
            or ""
        )
        .strip()
        .upper()
    )

    if not timeframe or not instruments:
        return {}

    return {
        "artifact_dir": Path(str(attempt.get("artifact_dir", ""))).resolve(),
        "profile_path": base_profile_path,
        "profile_ref": base_profile_id,
        "timeframe": timeframe,
        "instruments": instruments,
        "lookback_months": _derive_lookback_months(
            base_request_payload,
            _load_json_if_exists(
                Path(str(base_attempt.get("artifact_dir", ""))).resolve()
                / "sensitivity-response.json"
            ),
        ),
        "recovered_from_sweep": True,
    }


def _derive_lookback_months(
    request_payload: dict[str, Any], sensitivity_payload: dict[str, Any]
) -> int:
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
    sensitivity_payload = _load_json_if_exists(
        artifact_dir / "sensitivity-response.json"
    )

    timeframe = (
        str(
            request_payload.get("timeframe")
            or _nested_get(sensitivity_payload, ["data", "aggregate", "timeframe"])
            or _nested_get(sensitivity_payload, ["data", "timeframe"])
            or _nested_get(attempt, ["best_summary", "timeframe"])
            or (
                _coerce_profile_timeframe(profile_path)
                if profile_path is not None and profile_path.exists()
                else ""
            )
            or ""
        )
        .strip()
        .upper()
    )

    instruments_raw = request_payload.get("instruments")
    instruments = _normalize_tokens(
        instruments_raw if isinstance(instruments_raw, list) else []
    )
    if not instruments and profile_path is not None and profile_path.exists():
        instruments = _coerce_profile_instruments(profile_path)

    if (
        (
            not timeframe
            or not instruments
            or not (profile_path is not None and profile_path.exists())
        )
        and run_dir is not None
        and attempts is not None
    ):
        recovered = _recover_package_inputs_from_sweep(run_dir, attempt, attempts)
        if recovered:
            merged = {
                "artifact_dir": artifact_dir,
                "profile_path": profile_path,
                "timeframe": timeframe,
                "instruments": instruments,
                "lookback_months": _derive_lookback_months(
                    request_payload, sensitivity_payload
                ),
            }
            merged.update({key: value for key, value in recovered.items() if value})
            return merged

    if not timeframe:
        raise RuntimeError(
            f"Could not resolve timeframe for attempt {attempt.get('attempt_id')}"
        )
    if not instruments:
        raise RuntimeError(
            f"Could not resolve instruments for attempt {attempt.get('attempt_id')}"
        )

    return {
        "artifact_dir": artifact_dir,
        "profile_path": profile_path,
        "timeframe": timeframe,
        "instruments": instruments,
        "lookback_months": _derive_lookback_months(
            request_payload, sensitivity_payload
        ),
    }


def _profile_export_missing(text: str) -> bool:
    lowered = text.lower()
    return "not found" in lowered or "404" in lowered or "no document" in lowered


def _cloud_profile_exists(cli: FuzzfolioCli, profile_ref: str) -> bool:
    result = cli.run(["export-profile", "--profile-ref", profile_ref], check=False)
    if result.returncode == 0:
        return True
    combined = "\n".join(
        part for part in [result.stdout, result.stderr] if part
    ).strip()
    if _profile_export_missing(combined):
        return False
    raise CliError(FuzzfolioCli.format_result(result))


def _create_cloud_profile(cli: FuzzfolioCli, profile_path: Path) -> str:
    result = cli.run(["profiles", "create", "--file", str(profile_path), "--pretty"])
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else None
    profile_id = str(_nested_get(payload or {}, ["data", "id"]) or "").strip()
    if not profile_id:
        raise CliError(
            f"profiles create did not return a profile id for {profile_path}"
        )
    return profile_id


def _update_attempt_profile_ref(
    run_dir: Path, attempt_id: str, profile_ref: str
) -> None:
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
        raise RuntimeError(
            f"Package command did not create a bundle under {package_output_root}"
        )
    return sorted(bundle_dirs, key=lambda path: path.stat().st_mtime, reverse=True)[0]


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _normalize_profile_description(text: str | None) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _is_generic_profile_description(text: str | None) -> bool:
    normalized = _normalize_profile_description(text)
    if not normalized:
        return True
    generic_tokens = {
        "portable scoring profile scaffolded from live indicator templates.",
        "portable scoring profile scaffolded from live indicator templates",
    }
    return normalized in generic_tokens


def _extract_profile_document_profile(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("profile"), dict):
        return payload.get("profile")
    profile_document = payload.get("profile_document")
    if isinstance(profile_document, dict) and isinstance(profile_document.get("profile"), dict):
        return profile_document.get("profile")
    return None


def _summarize_phrase(items: list[str], *, limit: int = 4) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    if len(cleaned) <= limit:
        return ", ".join(cleaned[:-1]) + f", and {cleaned[-1]}"
    visible = cleaned[:limit]
    remaining = len(cleaned) - limit
    return ", ".join(visible[:-1]) + f", {visible[-1]}, and {remaining} more"


def _build_profile_drop_description(
    profile_payload: dict[str, Any] | None,
    *,
    package_inputs: dict[str, Any],
    row: dict[str, Any],
    attempt: dict[str, Any],
) -> str:
    profile = _extract_profile_document_profile(profile_payload) or {}
    direction_mode = str(
        profile.get("directionMode")
        or row.get("direction_mode")
        or attempt.get("direction_mode")
        or "both"
    ).strip().lower()
    direction_label = {
        "both": "Both-direction",
        "long": "Long-only",
        "short": "Short-only",
    }.get(direction_mode, "Multi-direction")

    instruments = [
        str(item).strip().upper()
        for item in list(package_inputs.get("instruments") or [])
        if str(item).strip()
    ]
    if len(instruments) == 1:
        instrument_phrase = f"{instruments[0]} profile"
    else:
        instrument_phrase = f"basket profile across {_summarize_phrase(instruments, limit=3)}"

    primary_timeframe = str(package_inputs.get("timeframe") or "").strip().upper() or "mixed"

    indicators = [
        indicator
        for indicator in list(profile.get("indicators") or [])
        if isinstance(indicator, dict)
    ]
    active_indicators = [
        indicator
        for indicator in indicators
        if bool((indicator.get("config") or {}).get("isActive", True))
    ]
    indicator_source = active_indicators or indicators

    indicator_labels: list[str] = []
    indicator_timeframes: list[str] = []
    trend_count = 0
    mean_reversion_count = 0
    neutral_count = 0
    for indicator in indicator_source:
        config = indicator.get("config") or {}
        meta = indicator.get("meta") or {}
        label = str(config.get("label") or meta.get("id") or "indicator").strip()
        timeframe = str(config.get("timeframe") or "").strip().upper()
        if label and label not in indicator_labels:
            indicator_labels.append(label)
        if timeframe and timeframe not in indicator_timeframes:
            indicator_timeframes.append(timeframe)
        trend_flag = config.get("isTrendFollowing")
        if trend_flag is True:
            trend_count += 1
        elif trend_flag is False:
            mean_reversion_count += 1
        else:
            neutral_count += 1

    label_phrase = _summarize_phrase(indicator_labels, limit=4) or "a custom indicator stack"
    sentence_one = (
        f"{direction_label} {instrument_phrase} on {primary_timeframe} using {label_phrase}."
    )

    detail_parts: list[str] = []
    if indicator_timeframes:
        detail_parts.append(
            f"Active signals span {_summarize_phrase(indicator_timeframes, limit=4)} timeframes"
        )
    mix_parts: list[str] = []
    if trend_count:
        mix_parts.append(f"{trend_count} trend-following")
    if mean_reversion_count:
        mix_parts.append(f"{mean_reversion_count} mean-reversion")
    if neutral_count:
        mix_parts.append(f"{neutral_count} neutral")
    if mix_parts:
        mix_phrase = _summarize_phrase(mix_parts, limit=4)
        detail_parts.append(f"mixing {mix_phrase} indicator{'s' if len(mix_parts) != 1 else ''}")

    if detail_parts:
        return sentence_one + " " + "; ".join(detail_parts) + "."
    return sentence_one


def _apply_profile_drop_description_fallback(
    bundle_dir: Path,
    *,
    package_inputs: dict[str, Any],
    row: dict[str, Any],
    attempt: dict[str, Any],
) -> str | None:
    profile_document_path = bundle_dir / "profile-document.json"
    payload = _load_json_if_exists(profile_document_path)
    if not isinstance(payload, dict):
        return None
    profile = _extract_profile_document_profile(payload)
    if not isinstance(profile, dict):
        return None
    if not _is_generic_profile_description(profile.get("description")):
        return None
    description = _build_profile_drop_description(
        payload,
        package_inputs=package_inputs,
        row=row,
        attempt=attempt,
    )
    if not description:
        return None
    profile["description"] = description
    _write_json_file(profile_document_path, payload)
    return description


def _validation_cache_dir(config, run_id: str, lookback_months: int) -> Path:
    return config.validation_cache_root / run_id / f"{int(lookback_months)}mo"


def _validation_manifest_path(config, run_id: str, lookback_months: int) -> Path:
    return _validation_cache_dir(config, run_id, lookback_months) / "manifest.json"


def _profile_drop_manifest_path(run_dir: Path, lookback_months: int) -> Path:
    return run_dir / f"profile-drop-{int(lookback_months)}mo.manifest.json"


def _slug_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    return token or "item"


def _profile_drop_attempt_token(
    row: dict[str, Any] | None,
    attempt: dict[str, Any] | None = None,
) -> str:
    row = row or {}
    attempt = attempt or {}
    rank = (
        row.get("selection_rank")
        or row.get("portfolio_rank")
        or attempt.get("selection_rank")
        or attempt.get("portfolio_rank")
        or ""
    )
    attempt_id = str(row.get("attempt_id") or attempt.get("attempt_id") or "").strip()
    candidate_name = str(
        row.get("candidate_name") or attempt.get("candidate_name") or "candidate"
    ).strip()
    identity = attempt_id or candidate_name or "candidate"
    return _slug_token(f"{rank}-{identity}")


def _attempt_scrutiny_cache_dir(
    attempt: dict[str, Any], lookback_months: int
) -> Path:
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    return scrutiny_cache_dir_for_artifact_dir(artifact_dir, lookback_months)


def _attempt_scrutiny_manifest_path(
    attempt: dict[str, Any], lookback_months: int
) -> Path:
    return _attempt_scrutiny_cache_dir(attempt, lookback_months) / "manifest.json"


def _scrutiny_manifest_payload(
    config,
    *,
    run_dir: Path,
    attempt: dict[str, Any],
    profile_ref: str,
    package_inputs: dict[str, Any],
    lookback_months: int,
) -> dict[str, Any]:
    return {
        "run_id": run_dir.name,
        "attempt_id": str(attempt.get("attempt_id") or ""),
        "candidate_name": str(attempt.get("candidate_name") or ""),
        "profile_ref": profile_ref,
        "timeframe": str(package_inputs["timeframe"]),
        "instruments": list(package_inputs["instruments"]),
        "lookback_months": int(lookback_months),
        "quality_score_preset": str(config.research.quality_score_preset),
    }


def _load_validation_score(artifact_dir: Path) -> dict[str, Any]:
    sensitivity_payload = _load_json_if_exists(
        artifact_dir / "sensitivity-response.json"
    )
    aggregate = _nested_get(sensitivity_payload, ["data", "aggregate"])
    if not isinstance(aggregate, dict):
        aggregate = _nested_get(sensitivity_payload, ["data"])
    compare_payload = {"best": aggregate or {}}
    score = build_attempt_score(
        compare_payload, sensitivity_payload if sensitivity_payload else None
    )
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


def _try_seed_attempt_scrutiny_cache(
    config,
    *,
    run_dir: Path,
    attempt: dict[str, Any],
    cache_dir: Path,
    manifest_payload: dict[str, Any],
) -> str | None:
    lookback_months = int(manifest_payload["lookback_months"])
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()

    legacy_dir = legacy_validation_cache_dir(
        config.validation_cache_root, run_dir.name, lookback_months
    )
    legacy_manifest_path = legacy_dir / "manifest.json"
    if (
        legacy_manifest_path.exists()
        and (legacy_dir / "sensitivity-response.json").exists()
        and (legacy_dir / "best-cell-path-detail.json").exists()
    ):
        legacy_manifest = load_json_if_exists(legacy_manifest_path)
        if (
            isinstance(legacy_manifest, dict)
            and str(legacy_manifest.get("attempt_id") or "")
            == str(manifest_payload.get("attempt_id") or "")
            and int(legacy_manifest.get("lookback_months") or 0) == lookback_months
            and str(legacy_manifest.get("timeframe") or "").strip().upper()
            == str(manifest_payload.get("timeframe") or "").strip().upper()
            and normalize_tokens(list(legacy_manifest.get("instruments") or []))
            == normalize_tokens(list(manifest_payload.get("instruments") or []))
        ):
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            shutil.copytree(legacy_dir, cache_dir)
            _write_json_file(cache_dir / "manifest.json", manifest_payload)
            return "legacy_run_validation_cache"

    if lookback_months == 36:
        full_result_path = artifact_dir / "full-backtest-36mo-result.json"
        full_curve_path = artifact_dir / "full-backtest-36mo-curve.json"
        if full_result_path.exists() and full_curve_path.exists():
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(full_result_path, cache_dir / "sensitivity-response.json")
            shutil.copy2(full_curve_path, cache_dir / "best-cell-path-detail.json")
            job_path = artifact_dir / "deep-replay-job.json"
            if job_path.exists():
                shutil.copy2(job_path, cache_dir / "deep-replay-job.json")
            _write_json_file(cache_dir / "manifest.json", manifest_payload)
            return "full_backtest"

    return None


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
    return covariance / (left_var**0.5 * right_var**0.5)


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
        active_dates = {
            date for date, value in curve_series.items() if abs(float(value)) > 1e-9
        }
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

    prepared.sort(
        key=lambda item: float(item.get("score_36m", float("-inf"))), reverse=True
    )
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
            right_instruments = set(
                str(item) for item in right.get("instruments_36m") or []
            )
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
                    "same_timeframe": str(left.get("timeframe_36m") or "")
                    == str(right.get("timeframe_36m") or ""),
                    "overlap_days": len(common_dates),
                    "similarity_score": similarity_score,
                }
            )

    adjacency: dict[str, list[dict[str, Any]]] = {
        str(item["run_id"]): [] for item in prepared
    }
    for pair in pair_records:
        adjacency[pair["left_run_id"]].append(pair)
        adjacency[pair["right_run_id"]].append(pair)

    leaders: list[dict[str, Any]] = []
    for row in prepared:
        related = adjacency.get(str(row["run_id"]), [])
        max_pair = max(
            related,
            key=lambda item: float(item.get("similarity_score", 0.0)),
            default=None,
        )
        avg_sameness = (
            sum(float(item.get("similarity_score", 0.0)) for item in related)
            / len(related)
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
                "max_sameness": float(max_pair.get("similarity_score", 0.0))
                if max_pair
                else 0.0,
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

    pair_records.sort(
        key=lambda item: float(item.get("similarity_score", 0.0)), reverse=True
    )
    return {
        "leaders": leaders,
        "pairs": pair_records,
        "matrix_labels": matrix_labels,
        "matrix_values": matrix_values,
    }


def _ensure_attempt_scrutiny_artifacts(
    *,
    config,
    cli: FuzzfolioCli,
    run_dir: Path,
    attempts: list[dict[str, Any]],
    attempt: dict[str, Any],
    lookback_months: int,
    force_rebuild: bool = False,
    emit: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    package_inputs = _build_package_inputs(attempt, run_dir=run_dir, attempts=attempts)
    profile_path = package_inputs.get("profile_path")
    profile_ref = str(
        package_inputs.get("profile_ref") or attempt.get("profile_ref") or ""
    ).strip()
    recreated_profile = False

    if profile_ref and not _cloud_profile_exists(cli, profile_ref):
        profile_ref = ""
    if not profile_ref:
        if not isinstance(profile_path, Path) or not profile_path.exists():
            raise RuntimeError(
                f"Attempt is missing a valid cloud profile ref and local profile file: {profile_path}"
            )
        profile_ref = _create_cloud_profile(cli, profile_path)
        _update_attempt_profile_ref(
            run_dir, str(attempt.get("attempt_id") or ""), profile_ref
        )
        recreated_profile = True

    cache_dir = _attempt_scrutiny_cache_dir(attempt, lookback_months)
    manifest_path = _attempt_scrutiny_manifest_path(attempt, lookback_months)
    manifest_payload = _scrutiny_manifest_payload(
        config,
        run_dir=run_dir,
        attempt=attempt,
        profile_ref=profile_ref,
        package_inputs=package_inputs,
        lookback_months=lookback_months,
    )
    sensitivity_path = cache_dir / "sensitivity-response.json"
    if (not force_rebuild) and sensitivity_path.exists() and manifest_path.exists():
        existing_manifest = _load_json_if_exists(manifest_path)
        if existing_manifest == manifest_payload:
            payload = _load_validation_score(cache_dir)
            payload["artifact_dir"] = str(cache_dir)
            payload["profile_ref"] = profile_ref
            payload["recreated_profile"] = recreated_profile
            payload["cache_hit"] = True
            payload["seed_source"] = None
            return payload

    seed_source = None
    if not force_rebuild:
        seed_source = _try_seed_attempt_scrutiny_cache(
            config,
            run_dir=run_dir,
            attempt=attempt,
            cache_dir=cache_dir,
            manifest_payload=manifest_payload,
        )
        if seed_source is not None:
            payload = _load_validation_score(cache_dir)
            payload["artifact_dir"] = str(cache_dir)
            payload["profile_ref"] = profile_ref
            payload["recreated_profile"] = recreated_profile
            payload["cache_hit"] = True
            payload["seed_source"] = seed_source
            return payload

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if emit:
        emit(
            f"  scrutiny {lookback_months}mo: "
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
    payload["seed_source"] = seed_source
    return payload


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
    return _ensure_attempt_scrutiny_artifacts(
        config=config,
        cli=cli,
        run_dir=run_dir,
        attempts=attempts,
        attempt=best_attempt,
        lookback_months=lookback_months,
        force_rebuild=force_rebuild,
        emit=emit,
    )


def _build_validation_rows(
    *,
    config,
    cli: FuzzfolioCli,
    ranked_attempts: list[dict[str, Any]],
    run_metadata_by_run_id: dict[str, dict[str, Any]],
    force_rebuild: bool = False,
    emit: Callable[[str], None] | None = None,
) -> list[dict[str, Any]]:
    prepared_attempts: list[
        tuple[int, str, Path, list[dict[str, Any]], dict[str, Any], dict[str, Any]]
    ] = []
    for index, attempt in enumerate(ranked_attempts):
        run_id = str(attempt.get("run_id") or "").strip()
        if not run_id:
            continue
        run_dir = config.runs_root / run_id
        attempts = load_run_attempts(run_dir)
        if not attempts:
            continue
        best_attempt = _best_attempt_for_run(
            attempts, lower_is_better=config.research.plot_lower_is_better
        )
        if best_attempt is None or str(best_attempt.get("attempt_id") or "") != str(
            attempt.get("attempt_id") or ""
        ):
            best_attempt = attempt
        prepared_attempts.append(
            (index, run_id, run_dir, attempts, best_attempt, attempt)
        )

    if not prepared_attempts:
        return []

    emit_lock = threading.Lock()
    max_workers = min(
        max(1, int(config.research.validation_max_concurrency)), len(prepared_attempts)
    )

    def emit_serial(message: str) -> None:
        if emit is None:
            return
        with emit_lock:
            emit(message)

    def build_row(
        item: tuple[
            int, str, Path, list[dict[str, Any]], dict[str, Any], dict[str, Any]
        ],
    ) -> tuple[int, dict[str, Any] | None]:
        index, run_id, run_dir, attempts, best_attempt, attempt = item
        cli_for_task = FuzzfolioCli(cli.config)
        emit_serial(f"validate {run_id} {best_attempt.get('attempt_id')}")
        row = {
            "run_id": run_id,
            "attempt_id": str(best_attempt.get("attempt_id") or ""),
            "candidate_name": best_attempt.get("candidate_name"),
            "leaderboard_label": attempt.get("leaderboard_label"),
            "explorer_model": (run_metadata_by_run_id.get(run_id) or {}).get(
                "explorer_model"
            ),
            "explorer_profile": (run_metadata_by_run_id.get(run_id) or {}).get(
                "explorer_profile"
            ),
        }
        try:
            validation_12 = _ensure_validation_artifacts(
                config=config,
                cli=cli_for_task,
                run_dir=run_dir,
                attempts=attempts,
                best_attempt=best_attempt,
                lookback_months=12,
                force_rebuild=force_rebuild,
                emit=emit_serial,
            )
            validation_36 = _ensure_validation_artifacts(
                config=config,
                cli=cli_for_task,
                run_dir=run_dir,
                attempts=attempts,
                best_attempt=best_attempt,
                lookback_months=36,
                force_rebuild=force_rebuild,
                emit=emit_serial,
            )
        except Exception as exc:
            if emit is not None:
                detail = (
                    str(exc).splitlines()[0].strip()
                    if str(exc).strip()
                    else exc.__class__.__name__
                )
                emit_serial(
                    f"validate skip {run_id} {best_attempt.get('attempt_id')}: {detail}"
                )
            return index, None
        row.update(
            {
                "score_12m": validation_12.get("score"),
                "score_basis_12m": validation_12.get("score_basis"),
                "trades_per_month_12m": validation_12.get("trades_per_month"),
                "trade_count_12m": validation_12.get("trade_count"),
                "effective_window_months_12m": validation_12.get(
                    "effective_window_months"
                ),
                "max_drawdown_r_12m": validation_12.get("max_drawdown_r"),
                "artifact_dir_12m": validation_12.get("artifact_dir"),
                "score_36m": validation_36.get("score"),
                "score_basis_36m": validation_36.get("score_basis"),
                "trades_per_month_36m": validation_36.get("trades_per_month"),
                "trade_count_36m": validation_36.get("trade_count"),
                "effective_window_months_36m": validation_36.get(
                    "effective_window_months"
                ),
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
            row["score_retention_ratio"] = (
                (score_36 / score_12) if score_12 not in {0.0, -0.0} else None
            )
        return index, row

    rows_by_index: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(build_row, item) for item in prepared_attempts]
        for future in as_completed(futures):
            index, row = future.result()
            if row is not None:
                rows_by_index[index] = row

    return [
        rows_by_index[index]
        for index, *_ in prepared_attempts
        if index in rows_by_index
    ]


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
    use_progress = (
        (not as_json)
        and (not PLAIN_PROGRESS_MODE)
        and bool(getattr(console, "is_terminal", False))
    )
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
                emit(f"sync {index}/{total_runs} {run_dir.name}")
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
                package_inputs = _build_package_inputs(
                    best_attempt, run_dir=run_dir, attempts=attempts
                )
                profile_path = package_inputs.get("profile_path")
                profile_ref = str(
                    package_inputs.get("profile_ref")
                    or best_attempt.get("profile_ref")
                    or ""
                ).strip()
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
                    _update_attempt_profile_ref(
                        run_dir, str(best_attempt.get("attempt_id") or ""), profile_ref
                    )
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
                        "quality_score_preset": str(
                            config.research.quality_score_preset
                        ),
                    }
                    horizon_manifest_path = _profile_drop_manifest_path(
                        run_dir, horizon_months
                    )
                    png_path = run_dir / f"profile-drop-{horizon_months}mo.png"
                    if (
                        not force_rebuild
                        and png_path.exists()
                        and horizon_manifest_path.exists()
                        and _load_json_if_exists(horizon_manifest_path)
                        == horizon_manifest_payload
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

                rendered_horizons = [
                    months
                    for months in requested_horizons
                    if months not in skipped_horizons
                ]
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


def cmd_calculate_full_backtests(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    limit: int | None,
    max_workers: int | None,
    use_dev_sim_worker_count: bool,
    require_scrutiny_36: bool,
    force_rebuild: bool,
    job_timeout_seconds: int | None,
    as_json: bool,
) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    catalog_rows = _catalog_rows_for_run_dirs(config, run_dirs)
    catalog_by_attempt_id = {
        str(row.get("attempt_id") or ""): row for row in catalog_rows
    }
    matched_items = _matched_attempt_items(
        config,
        run_ids=run_ids,
        attempt_ids=attempt_ids,
        require_scored=False,
    )
    if not matched_items:
        print(
            json.dumps(
                {"status": "no_attempts", "considered": 0}, ensure_ascii=True, indent=2
            )
        )
        return 0

    def needs_calculation(attempt: dict[str, Any]) -> bool:
        artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
        if not artifact_dir.exists():
            return False
        if force_rebuild:
            return True
        curve_path = artifact_dir / "full-backtest-36mo-curve.json"
        return not curve_path.exists()

    filter_rejections = {
        "already_has_full_backtest": 0,
        "missing_scrutiny_36m": 0,
    }
    eligible_items: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
    for run_dir, _attempts, attempt in matched_items:
        row = catalog_by_attempt_id.get(str(attempt.get("attempt_id") or "")) or {}
        if not needs_calculation(attempt):
            filter_rejections["already_has_full_backtest"] += 1
            continue
        if require_scrutiny_36 and not bool(row.get("has_scrutiny_36m")):
            filter_rejections["missing_scrutiny_36m"] += 1
            continue
        eligible_items.append((run_dir, attempt, row))

    eligible_items.sort(key=lambda item: _full_backtest_priority_key(item[2]))
    to_calculate = list(eligible_items)
    if limit is not None and int(limit) >= 0:
        to_calculate = to_calculate[: int(limit)]
    total = len(to_calculate)
    skipped = len(matched_items) - total

    worker_source = "config.validation_max_concurrency"
    detected_sim_workers = None
    resolved_max_workers = int(max_workers) if max_workers is not None else None
    if use_dev_sim_worker_count:
        detected_sim_workers = _detect_dev_sim_worker_count()
        if detected_sim_workers is not None:
            worker_source = "dev_sim_workers"
            if resolved_max_workers is None:
                resolved_max_workers = detected_sim_workers
        else:
            worker_source = "config.validation_max_concurrency_fallback"
    if resolved_max_workers is None:
        resolved_max_workers = int(config.research.validation_max_concurrency)
    resolved_max_workers = max(1, int(resolved_max_workers))
    if total > 0:
        resolved_max_workers = min(resolved_max_workers, total)

    results: list[dict[str, Any]] = []
    calculated = 0
    seeded_materialized = 0
    failed = 0
    derived_refresh = None

    use_progress = (
        (not as_json)
        and (not PLAIN_PROGRESS_MODE)
        and bool(getattr(console, "is_terminal", False))
    )
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
        task_id = progress.add_task("calculate full backtests", total=total or 1)
        if total == 0:
            progress.update(
                task_id,
                description=(
                    f"0/0 [green]ok={calculated}[/green] "
                    f"[yellow]skip={skipped}[/yellow] [red]fail={failed}[/red]"
                ),
            )
        else:
            pending_items = list(to_calculate)
            in_flight: dict[Any, tuple[Path, dict[str, Any], dict[str, Any], float]] = {}

            def refresh_progress(active_run_id: str | None = None) -> None:
                queued_count = len(pending_items)
                running_count = len(in_flight)
                detail = f" {active_run_id}" if active_run_id else ""
                progress.update(
                    task_id,
                    description=(
                        f"{calculated + failed}/{total} "
                        f"[green]ok={calculated}[/green] "
                        f"[yellow]seeded={seeded_materialized}[/yellow] "
                        f"[cyan]run={running_count}[/cyan] "
                        f"[magenta]queue={queued_count}[/magenta] "
                        f"[bright_yellow]skip={skipped}[/bright_yellow] "
                        f"[red]fail={failed}[/red]{detail}"
                    ),
                )

            with ThreadPoolExecutor(max_workers=resolved_max_workers) as executor:
                while pending_items or in_flight:
                    while pending_items and len(in_flight) < resolved_max_workers:
                        run_dir, attempt, row = pending_items.pop(0)
                        attempt_id = str(attempt.get("attempt_id") or "")
                        emit(
                            f"queue {calculated + failed + len(in_flight) + 1}/{total} "
                            f"{run_dir.name} {attempt_id}"
                        )
                        future = executor.submit(
                            _run_full_backtest_with_retry,
                            config,
                            attempt,
                            job_timeout_seconds=job_timeout_seconds,
                        )
                        in_flight[future] = (run_dir, attempt, row, pytime.time())
                        refresh_progress(run_dir.name)
                    done, _ = wait(
                        set(in_flight.keys()),
                        return_when=FIRST_COMPLETED,
                    )
                    for future in done:
                        run_dir, attempt, row, started_at = in_flight.pop(future)
                        attempt_id = str(attempt.get("attempt_id") or "")
                        result_entry: dict[str, Any] = {
                            "run_id": run_dir.name,
                            "attempt_id": attempt_id,
                            "candidate_name": row.get("candidate_name"),
                            "score_36m": row.get("score_36m"),
                            "composite_score": row.get("composite_score"),
                            "status": "pending",
                            "duration_seconds": round(pytime.time() - started_at, 3),
                        }
                        try:
                            paths = future.result()
                            seed_source = paths.get("seed_source")
                            retry_mode = paths.get("retry_mode")
                            recovery_mode = paths.get("recovery_mode")
                            result_entry["status"] = (
                                "seeded" if seed_source else "calculated"
                            )
                            result_entry["curve_path"] = paths.get("curve_path")
                            result_entry["result_path"] = paths.get("result_path")
                            result_entry["seed_source"] = seed_source
                            if retry_mode:
                                result_entry["retry_mode"] = retry_mode
                            if recovery_mode:
                                result_entry["recovery_mode"] = recovery_mode
                            calculated += 1
                            if seed_source:
                                seeded_materialized += 1
                            emit(
                                f"  done: {run_dir.name} {attempt_id} "
                                f"({result_entry['duration_seconds']}s)"
                                + (
                                    f" seed={seed_source}"
                                    if seed_source
                                    else ""
                                )
                                + (
                                    f" retry={retry_mode}"
                                    if retry_mode
                                    else ""
                                )
                                + (
                                    f" recovery={recovery_mode}"
                                    if recovery_mode
                                    else ""
                                )
                            )
                        except Exception as exc:
                            failed += 1
                            result_entry["status"] = "failed"
                            result_entry["error"] = str(exc)
                            emit(
                                f"  failed: {run_dir.name} {attempt_id} "
                                f"({result_entry['duration_seconds']}s) {exc}"
                            )
                        results.append(result_entry)
                        progress.advance(task_id, 1)
                        refresh_progress(run_dir.name)

    derived_refresh = _refresh_global_derived_corpus_state(config)
    failure_summary = _build_full_backtest_failure_summary(results)
    write_json(config.full_backtest_failures_json_path, failure_summary)

    payload = {
        "runs_considered": len({run_dir.name for run_dir, *_ in matched_items}),
        "matched_attempts": len(matched_items),
        "eligible_attempts": len(eligible_items),
        "skipped": skipped,
        "filter_rejections": filter_rejections,
        "filters": {
            "run_ids": run_ids,
            "attempt_ids": attempt_ids,
            "limit": limit,
            "require_scrutiny_36": require_scrutiny_36,
            "force_rebuild": force_rebuild,
            "job_timeout_seconds": job_timeout_seconds,
        },
        "max_workers_used": resolved_max_workers,
        "max_workers_source": worker_source,
        "detected_dev_sim_workers": detected_sim_workers,
        "calculated": calculated,
        "seeded_materialized": seeded_materialized,
        "failed": failed,
        "failure_summary": failure_summary,
        "failure_summary_json": str(config.full_backtest_failures_json_path),
        "derived_refresh": derived_refresh,
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if failed == 0 else 1


def cmd_build_attempt_catalog(*, run_ids: list[str] | None, as_json: bool) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    rows = _catalog_rows_for_run_dirs(config, run_dirs)
    summary = catalog_summary(rows)
    write_json(config.attempt_catalog_json_path, rows)
    write_csv(config.attempt_catalog_csv_path, rows)
    write_json(config.attempt_catalog_summary_path, summary)
    payload = {
        "run_count": len(run_dirs),
        "attempt_catalog_json": str(config.attempt_catalog_json_path),
        "attempt_catalog_csv": str(config.attempt_catalog_csv_path),
        "attempt_catalog_summary_json": str(config.attempt_catalog_summary_path),
        "summary": summary,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


def cmd_audit_full_backtests(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    as_json: bool,
) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    rows = _catalog_rows_for_run_dirs(config, run_dirs)
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }
    if wanted_attempt_ids:
        rows = [
            row for row in rows if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    audit_payload = build_full_backtest_audit(rows)
    output_path = (
        config.full_backtest_audit_json_path
        if not run_ids and not wanted_attempt_ids
        else None
    )
    if output_path is not None:
        write_json(output_path, audit_payload)
    payload = {
        "full_backtest_audit_json": str(output_path) if output_path is not None else None,
        "run_count": len(run_dirs),
        "attempt_count": len(rows),
        **audit_payload,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


def cmd_plot_corpus_score_vs_trades(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    require_full_backtest_36: bool,
    x_axis_max: float | None,
    as_json: bool,
) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    rows = _catalog_rows_for_run_dirs(config, run_dirs)
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }
    if wanted_attempt_ids:
        rows = [
            row for row in rows if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    plotted_rows = render_attempt_tradeoff_scatter_artifacts(
        rows,
        config.corpus_tradeoff_plot_path,
        config.corpus_tradeoff_json_path,
        require_full_backtest_36=require_full_backtest_36,
        x_axis_max=(
            None if x_axis_max is not None and float(x_axis_max) < 0.0 else x_axis_max
        ),
    )
    payload = {
        "plot_path": str(config.corpus_tradeoff_plot_path),
        "json_path": str(config.corpus_tradeoff_json_path),
        "run_count": len(run_dirs),
        "candidate_rows_plotted": len(plotted_rows),
        "valid_full_backtest_rows_plotted": sum(
            1 for row in plotted_rows if bool(row.get("is_valid_full_backtest_36m"))
        ),
        "require_full_backtest_36": require_full_backtest_36,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


def _render_profile_drop_for_attempt(
    *,
    config,
    cli: FuzzfolioCli,
    renderer_executable: Path,
    working_dir: Path,
    run_dir: Path,
    attempts: list[dict[str, Any]],
    row: dict[str, Any],
    attempt: dict[str, Any],
    output_root: Path,
    lookback_months: int,
    force_rebuild: bool,
    timeout_seconds: int,
    emit: Callable[[str], None] | None,
) -> dict[str, Any]:
    package_inputs = _build_package_inputs(attempt, run_dir=run_dir, attempts=attempts)
    profile_path = package_inputs.get("profile_path")
    profile_ref = str(
        package_inputs.get("profile_ref") or attempt.get("profile_ref") or ""
    ).strip()
    recreated_profile = False

    if profile_ref and not _cloud_profile_exists(cli, profile_ref):
        profile_ref = ""
    if not profile_ref:
        if not isinstance(profile_path, Path) or not profile_path.exists():
            raise RuntimeError(
                f"Attempt is missing a valid cloud profile ref and local profile file: {profile_path}"
            )
        profile_ref = _create_cloud_profile(cli, profile_path)
        _update_attempt_profile_ref(
            run_dir, str(attempt.get("attempt_id") or ""), profile_ref
        )
        recreated_profile = True

    attempt_token = _profile_drop_attempt_token(row, attempt)
    attempt_root = output_root / attempt_token
    package_output_root = attempt_root / "bundle"
    png_path = attempt_root / f"profile-drop-{int(lookback_months)}mo.png"
    manifest_path = attempt_root / f"profile-drop-{int(lookback_months)}mo.manifest.json"
    manifest_payload = {
        "version": 1,
        "run_id": run_dir.name,
        "attempt_id": str(row.get("attempt_id") or attempt.get("attempt_id") or ""),
        "candidate_name": str(
            row.get("candidate_name") or attempt.get("candidate_name") or ""
        ),
        "profile_ref": profile_ref,
        "timeframe": str(package_inputs["timeframe"]),
        "instruments": list(package_inputs["instruments"]),
        "lookback_months": int(lookback_months),
        "quality_score_preset": str(config.research.quality_score_preset),
    }
    if (
        (not force_rebuild)
        and png_path.exists()
        and manifest_path.exists()
        and _load_json_if_exists(manifest_path) == manifest_payload
    ):
        return {
            "status": "cached",
            "png_path": str(png_path),
            "manifest_path": str(manifest_path),
            "profile_ref": profile_ref,
            "recreated_profile": recreated_profile,
        }

    if attempt_root.exists():
        shutil.rmtree(attempt_root)
    package_output_root.mkdir(parents=True, exist_ok=True)
    if emit:
        emit(
            f"  package {attempt.get('attempt_id')} timeframe={package_inputs['timeframe']} "
            f"instruments={','.join(package_inputs['instruments'])} lookback={lookback_months}mo"
        )
    package_args = [
        "package",
        "--profile-ref",
        profile_ref,
        "--timeframe",
        str(package_inputs["timeframe"]),
        "--lookback-months",
        str(int(lookback_months)),
        "--output-root",
        str(package_output_root),
        "--label",
        f"shortlist-{attempt_token}",
        "--skip-catalogs",
        "--skip-render-capture",
        "--allow-timeframe-mismatch",
        "--quality-score-preset",
        str(config.research.quality_score_preset),
    ]
    for instrument in package_inputs["instruments"]:
        package_args.extend(["--instrument", str(instrument)])
    cli.run(package_args, cwd=working_dir, timeout_seconds=float(timeout_seconds))

    bundle_dir = _discover_bundle_dir(package_output_root)
    description_override = _apply_profile_drop_description_fallback(
        bundle_dir,
        package_inputs=package_inputs,
        row=row,
        attempt=attempt,
    )
    if emit and description_override:
        emit(f"  profile summary fallback applied for {attempt.get('attempt_id')}")
    renderer_argv = [str(renderer_executable)]
    if config.fuzzfolio.workspace_root is not None:
        renderer_argv.extend(["--workspace-root", str(config.fuzzfolio.workspace_root)])
    renderer_argv.extend(["render", "--bundle", str(bundle_dir), "--out", str(png_path)])
    _run_external(renderer_argv, cwd=working_dir, timeout_seconds=float(timeout_seconds))
    _write_json_file(manifest_path, manifest_payload)
    if not png_path.exists():
        raise RuntimeError(
            f"Profile-drop render reported success but PNG is missing: {png_path}"
        )
    if not manifest_path.exists():
        raise RuntimeError(
            f"Profile-drop render reported success but manifest is missing: {manifest_path}"
        )
    return {
        "status": "rendered",
        "png_path": str(png_path),
        "manifest_path": str(manifest_path),
        "profile_ref": profile_ref,
        "recreated_profile": recreated_profile,
    }


def _should_retry_profile_drop_error(message: str) -> bool:
    normalized = str(message or "").lower()
    retry_tokens = [
        "http request failed",
        "connection was forcibly closed",
        "connection error",
        "sendrequest",
        "timed out",
        "timeout",
    ]
    return any(token in normalized for token in retry_tokens)


def _render_profile_drop_rows(
    *,
    config,
    rows: list[dict[str, Any]],
    output_root: Path,
    lookback_months: int,
    timeout_seconds: int,
    force_rebuild: bool,
    profile_drop_workers: int,
    as_json: bool,
    progress_label: str,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()
    renderer_executable, workspace_root = _resolve_drop_renderer_executable(config)
    working_dir = workspace_root or config.repo_root

    expected_drop_dirs = {
        _profile_drop_attempt_token(row)
        for row in rows
    }
    output_root.mkdir(parents=True, exist_ok=True)
    for child in output_root.iterdir():
        if not child.is_dir():
            continue
        if child.name not in expected_drop_dirs:
            shutil.rmtree(child)

    matched_items = _matched_attempt_items(
        config,
        attempt_ids=[
            str(row.get("attempt_id") or "").strip()
            for row in rows
            if str(row.get("attempt_id") or "").strip()
        ],
        require_scored=False,
    )
    item_by_attempt_id = {
        str(attempt.get("attempt_id") or ""): (run_dir, attempts, attempt)
        for run_dir, attempts, attempt in matched_items
    }

    ordered_results: list[dict[str, Any]] = []
    work_items: list[tuple[int, dict[str, Any], Path, list[dict[str, Any]], dict[str, Any]]] = []
    for index, row in enumerate(rows):
        attempt_id = str(row.get("attempt_id") or "").strip()
        matched = item_by_attempt_id.get(attempt_id)
        if matched is None:
            ordered_results.append(
                {
                    "_row_index": index,
                    "attempt_id": row.get("attempt_id"),
                    "run_id": row.get("run_id"),
                    "candidate_name": row.get("candidate_name"),
                    "status": "failed",
                    "error": f"Attempt ledger record missing for attempt {attempt_id}",
                }
            )
            continue
        run_dir, attempts, attempt = matched
        work_items.append((index, row, run_dir, attempts, attempt))

    use_progress = (
        (not as_json)
        and (not PLAIN_PROGRESS_MODE)
        and bool(getattr(console, "is_terminal", False))
    )
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

    cached_count = 0
    rendered_count = 0
    failed_count = sum(1 for row in ordered_results if row.get("status") == "failed")

    def _progress_description() -> str:
        return (
            f"{progress_label} "
            f"[green]rendered={rendered_count}[/green] "
            f"[cyan]cached={cached_count}[/cyan] "
            f"[red]failed={failed_count}[/red]"
        )

    def render_one(
        row: dict[str, Any],
        run_dir: Path,
        attempts: list[dict[str, Any]],
        attempt: dict[str, Any],
    ) -> dict[str, Any]:
        attempts_remaining = 3
        last_error: Exception | None = None
        while attempts_remaining > 0:
            try:
                worker_cli = FuzzfolioCli(config.fuzzfolio)
                result = _render_profile_drop_for_attempt(
                    config=config,
                    cli=worker_cli,
                    renderer_executable=renderer_executable,
                    working_dir=working_dir,
                    run_dir=run_dir,
                    attempts=attempts,
                    row=row,
                    attempt=attempt,
                    output_root=output_root,
                    lookback_months=int(lookback_months),
                    force_rebuild=force_rebuild,
                    timeout_seconds=int(timeout_seconds),
                    emit=None,
                )
                return {
                    "attempt_id": row.get("attempt_id"),
                    "run_id": row.get("run_id"),
                    "candidate_name": row.get("candidate_name"),
                    **result,
                }
            except Exception as exc:
                last_error = exc
                attempts_remaining -= 1
                if attempts_remaining <= 0 or not _should_retry_profile_drop_error(str(exc)):
                    break
                pytime.sleep(2.0)
        raise RuntimeError(str(last_error) if last_error is not None else "Unknown render error")

    worker_count = max(1, int(profile_drop_workers))
    with progress:
        task_id = progress.add_task(
            _progress_description(),
            total=max(1, len(work_items)),
        )
        if worker_count == 1:
            for index, row, run_dir, attempts, attempt in work_items:
                try:
                    result = render_one(row, run_dir, attempts, attempt)
                except Exception as exc:
                    result = {
                        "attempt_id": row.get("attempt_id"),
                        "run_id": row.get("run_id"),
                        "candidate_name": row.get("candidate_name"),
                        "status": "failed",
                        "error": str(exc),
                    }
                status = str(result.get("status") or "")
                if status == "cached":
                    cached_count += 1
                elif status == "failed":
                    failed_count += 1
                else:
                    rendered_count += 1
                ordered_results.append({"_row_index": index, **result})
                progress.advance(task_id, 1)
                progress.update(task_id, description=_progress_description())
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(render_one, row, run_dir, attempts, attempt): (index, row)
                    for index, row, run_dir, attempts, attempt in work_items
                }
                for future in as_completed(future_map):
                    index, row = future_map[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {
                            "attempt_id": row.get("attempt_id"),
                            "run_id": row.get("run_id"),
                            "candidate_name": row.get("candidate_name"),
                            "status": "failed",
                            "error": str(exc),
                        }
                    status = str(result.get("status") or "")
                    if status == "cached":
                        cached_count += 1
                    elif status == "failed":
                        failed_count += 1
                    else:
                        rendered_count += 1
                    ordered_results.append({"_row_index": index, **result})
                    progress.advance(task_id, 1)
                    progress.update(task_id, description=_progress_description())

    ordered_results.sort(key=lambda row: int(row.get("_row_index") or 0))
    for row in ordered_results:
        row.pop("_row_index", None)
        status = str(row.get("status") or "")
        if status not in {"rendered", "cached"}:
            continue
        png_path = Path(str(row.get("png_path") or "")) if row.get("png_path") else None
        manifest_path = (
            Path(str(row.get("manifest_path") or "")) if row.get("manifest_path") else None
        )
        missing_parts: list[str] = []
        if png_path is None or not png_path.exists():
            missing_parts.append("png")
        if manifest_path is None or not manifest_path.exists():
            missing_parts.append("manifest")
        if missing_parts:
            row["status"] = "failed"
            row["error"] = (
                "Profile-drop artifacts missing after render: " + ", ".join(missing_parts)
            )
    return ordered_results


def _portfolio_chart_paths(report_root: Path) -> tuple[Path, Path]:
    charts_root = report_root / "charts"
    profile_drop_root = report_root / "profile-drops"
    charts_root.mkdir(parents=True, exist_ok=True)
    profile_drop_root.mkdir(parents=True, exist_ok=True)
    return charts_root, profile_drop_root


def _sanitize_report_token(value: str, *, fallback: str = "slice") -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-._")
    return token[:80] if token else fallback


def _shortlist_report_root(
    config: AppConfig,
    *,
    run_ids: Sequence[str] | None,
    attempt_ids: Sequence[str] | None,
) -> tuple[Path, bool]:
    filtered_run_ids = [str(value).strip() for value in (run_ids or []) if str(value).strip()]
    filtered_attempt_ids = [
        str(value).strip() for value in (attempt_ids or []) if str(value).strip()
    ]
    if not filtered_run_ids and not filtered_attempt_ids:
        return config.derived_root / "shortlist-report", True
    label_parts: list[str] = []
    if filtered_run_ids:
        if len(filtered_run_ids) == 1:
            label_parts.append(f"run-{_sanitize_report_token(filtered_run_ids[0], fallback='run')}")
        else:
            label_parts.append(f"runs-{len(filtered_run_ids)}")
    if filtered_attempt_ids:
        if len(filtered_attempt_ids) == 1:
            label_parts.append(
                f"attempt-{_sanitize_report_token(filtered_attempt_ids[0], fallback='attempt')}"
            )
        else:
            label_parts.append(f"attempts-{len(filtered_attempt_ids)}")
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
    label = "-".join(label_parts) if label_parts else "filtered"
    return config.derived_root / "shortlist-report-slices" / f"{timestamp}-{label}", False


def _phase_emit(message: str, *, as_json: bool) -> None:
    if as_json:
        return
    _write_plain_line(message)


def _catalog_phase_callback(label: str, *, as_json: bool) -> Callable[[dict[str, Any]], None]:
    def callback(event: dict[str, Any]) -> None:
        if as_json:
            return
        stage = str(event.get("stage") or "")
        if stage == "start":
            total_runs = int(event.get("total_runs") or 0)
            _write_plain_line(f"[{label}] loading corpus rows from {total_runs} run directories")
            return
        if stage == "progress":
            completed = int(event.get("completed_runs") or 0)
            total_runs = int(event.get("total_runs") or 0)
            run_id = str(event.get("run_id") or "")
            row_count = int(event.get("row_count") or 0)
            if completed == 1 or completed == total_runs or completed % 10 == 0:
                _write_plain_line(
                    f"[{label}] catalog {completed}/{total_runs} runs, {row_count} rows loaded, latest={run_id}"
                )

    return callback


def _similarity_phase_callback(
    label: str, *, as_json: bool
) -> Callable[[dict[str, Any]], None]:
    def callback(event: dict[str, Any]) -> None:
        if as_json:
            return
        stage = str(event.get("stage") or "")
        if stage == "prepare_start":
            total = int(event.get("total") or 0)
            _write_plain_line(f"[{label}] loading {total} curve payloads for similarity")
            return
        if stage == "prepare_progress":
            completed = int(event.get("completed") or 0)
            total = int(event.get("total") or 0)
            prepared = int(event.get("prepared_count") or 0)
            _write_plain_line(
                f"[{label}] similarity curves {completed}/{total} scanned, {prepared} usable"
            )
            return
        if stage == "pairs_start":
            total = int(event.get("total") or 0)
            prepared = int(event.get("prepared_count") or 0)
            _write_plain_line(
                f"[{label}] computing similarity pairs for {prepared} candidates ({total} pair checks)"
            )
            return
        if stage == "pairs_progress":
            completed = int(event.get("completed") or 0)
            total = int(event.get("total") or 0)
            _write_plain_line(f"[{label}] similarity pairs {completed}/{total}")

    return callback


def cmd_build_shortlist_report(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    candidate_limit: int,
    shortlist_size: int,
    min_score_36: float,
    min_retention_ratio: float,
    min_trades_per_month: float,
    max_drawdown_r: float,
    drawdown_penalty: float,
    trade_rate_bonus_weight: float,
    trade_rate_bonus_target: float,
    novelty_penalty: float,
    max_per_run: int,
    max_per_strategy_key: int,
    max_sameness_to_board: float,
    require_full_backtest_36: bool,
    generate_profile_drops: bool,
    profile_drop_lookback_months: int,
    chart_trades_x_max: float,
    profile_drop_timeout_seconds: int,
    profile_drop_workers: int,
    force_rebuild_profile_drops: bool,
    as_json: bool,
) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    full_catalog_rows = _catalog_rows_for_run_dirs(
        config,
        run_dirs,
        progress_callback=_catalog_phase_callback("shortlist", as_json=as_json),
    )
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }
    rows = list(full_catalog_rows)
    if wanted_attempt_ids:
        rows = [
            row for row in rows if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    rows.sort(key=_full_backtest_priority_key)
    if candidate_limit >= 0:
        rows = rows[:candidate_limit]

    filter_rejections = {
        "missing_score_36m": 0,
        "score_below_min_score_36": 0,
        "missing_trades_per_month_36m": 0,
        "trades_below_min_trades_per_month": 0,
        "missing_retention_ratio_36m_vs_12m": 0,
        "retention_below_min_retention_ratio": 0,
        "missing_drawdown_36m": 0,
        "drawdown_above_max_drawdown_r": 0,
        "missing_full_backtest_36m": 0,
        "invalid_full_backtest_36m": 0,
    }
    candidate_rows: list[dict[str, Any]] = []
    max_drawdown_cap = None if float(max_drawdown_r) < 0.0 else float(max_drawdown_r)
    for row in rows:
        score_36 = _safe_float_value(row.get("score_36m"))
        if score_36 is None:
            filter_rejections["missing_score_36m"] += 1
            continue
        if score_36 < float(min_score_36):
            filter_rejections["score_below_min_score_36"] += 1
            continue
        trades_per_month_36 = _safe_float_value(row.get("trades_per_month_36m"))
        if float(min_trades_per_month) > 0.0:
            if trades_per_month_36 is None:
                filter_rejections["missing_trades_per_month_36m"] += 1
                continue
            if trades_per_month_36 < float(min_trades_per_month):
                filter_rejections["trades_below_min_trades_per_month"] += 1
                continue
        retention_ratio = _safe_float_value(row.get("score_retention_ratio_36m_vs_12m"))
        if float(min_retention_ratio) > 0.0:
            if retention_ratio is None:
                filter_rejections["missing_retention_ratio_36m_vs_12m"] += 1
                continue
            if retention_ratio < float(min_retention_ratio):
                filter_rejections["retention_below_min_retention_ratio"] += 1
                continue
        drawdown_36 = _safe_float_value(row.get("max_drawdown_r_36m"))
        if max_drawdown_cap is not None:
            if drawdown_36 is None:
                filter_rejections["missing_drawdown_36m"] += 1
                continue
            if drawdown_36 > max_drawdown_cap:
                filter_rejections["drawdown_above_max_drawdown_r"] += 1
                continue
        if require_full_backtest_36 and not bool(row.get("has_full_backtest_36m")):
            filter_rejections["missing_full_backtest_36m"] += 1
            continue
        if (
            require_full_backtest_36
            and str(row.get("full_backtest_validation_status_36m") or "") != "valid"
        ):
            filter_rejections["invalid_full_backtest_36m"] += 1
            continue
        candidate_rows.append(row)

    _phase_emit(
        f"[shortlist] {len(candidate_rows)} qualified candidates after filtering",
        as_json=as_json,
    )
    similarity_payload = build_candidate_similarity_payload(
        candidate_rows,
        progress_callback=_similarity_phase_callback("shortlist", as_json=as_json),
    )
    shortlist_board = select_promotion_board(
        candidate_rows,
        similarity_payload,
        board_size=shortlist_size,
        novelty_penalty=novelty_penalty,
        drawdown_penalty=drawdown_penalty,
        trade_rate_bonus_weight=trade_rate_bonus_weight,
        trade_rate_bonus_target=trade_rate_bonus_target,
        max_drawdown_r=max_drawdown_cap,
        max_sameness_to_board=max_sameness_to_board,
        max_per_run=(None if max_per_run < 0 else max_per_run),
        max_per_strategy_key=(None if max_per_strategy_key < 0 else max_per_strategy_key),
    )
    shortlist_rows = list(shortlist_board.get("selected") or [])
    _phase_emit(
        f"[shortlist] selected {len(shortlist_rows)} candidates, building shortlist-only similarity payload",
        as_json=as_json,
    )
    shortlist_similarity_payload = subset_similarity_payload(
        similarity_payload,
        shortlist_rows,
    )

    report_root, is_canonical_report = _shortlist_report_root(
        config,
        run_ids=run_ids,
        attempt_ids=attempt_ids,
    )
    report_root.mkdir(parents=True, exist_ok=True)
    charts_root, profile_drop_root = _portfolio_chart_paths(report_root)

    trades_x_cap = None if float(chart_trades_x_max) < 0.0 else float(chart_trades_x_max)
    render_attempt_tradeoff_scatter_artifacts(
        candidate_rows,
        charts_root / "corpus-score-vs-trades-36mo.png",
        charts_root / "corpus-score-vs-trades-36mo.json",
        require_full_backtest_36=require_full_backtest_36,
        x_axis_max=trades_x_cap,
        title_prefix="Corpus",
    )
    render_attempt_tradeoff_scatter_artifacts(
        shortlist_rows,
        charts_root / "shortlist-score-vs-trades-36mo.png",
        charts_root / "shortlist-score-vs-trades-36mo.json",
        require_full_backtest_36=require_full_backtest_36,
        x_axis_max=trades_x_cap,
        title_prefix="Shortlist",
    )
    render_attempt_tradeoff_overlay_artifacts(
        candidate_rows,
        shortlist_rows,
        charts_root / "shortlist-overlay-score-vs-trades-36mo.png",
        charts_root / "shortlist-overlay-score-vs-trades-36mo.json",
        x_axis_max=trades_x_cap,
        title_prefix="Shortlist Overlay",
    )
    render_attempt_drawdown_scatter_artifacts(
        candidate_rows,
        charts_root / "corpus-score-vs-drawdown-36mo.png",
        charts_root / "corpus-score-vs-drawdown-36mo.json",
        require_full_backtest_36=require_full_backtest_36,
        title_prefix="Corpus",
    )
    render_attempt_drawdown_scatter_artifacts(
        shortlist_rows,
        charts_root / "shortlist-score-vs-drawdown-36mo.png",
        charts_root / "shortlist-score-vs-drawdown-36mo.json",
        require_full_backtest_36=require_full_backtest_36,
        title_prefix="Shortlist",
    )
    render_similarity_scatter_artifacts(
        similarity_payload,
        charts_root / "corpus-score-vs-sameness-36mo.png",
    )
    render_similarity_heatmap_artifacts(
        shortlist_similarity_payload,
        charts_root / "shortlist-similarity-heatmap.png",
        charts_root / "shortlist-similarity-heatmap.json",
    )
    render_similarity_scatter_artifacts(
        shortlist_similarity_payload,
        charts_root / "shortlist-score-vs-sameness-36mo.png",
    )

    shortlist_payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "scope": {
            "is_canonical": is_canonical_report,
            "is_filtered": bool(run_ids or attempt_ids),
            "report_root": str(report_root),
        },
        "filters": {
            "run_ids": run_ids,
            "attempt_ids": attempt_ids,
            "candidate_limit": candidate_limit,
            "shortlist_size": shortlist_size,
            "min_score_36": min_score_36,
            "min_retention_ratio": min_retention_ratio,
            "min_trades_per_month": min_trades_per_month,
            "max_drawdown_r": max_drawdown_cap,
            "drawdown_penalty": drawdown_penalty,
            "trade_rate_bonus_weight": trade_rate_bonus_weight,
            "trade_rate_bonus_target": trade_rate_bonus_target,
            "novelty_penalty": novelty_penalty,
            "max_per_run": max_per_run,
            "max_per_strategy_key": max_per_strategy_key,
            "max_sameness_to_board": max_sameness_to_board,
            "require_full_backtest_36": require_full_backtest_36,
            "profile_drop_lookback_months": profile_drop_lookback_months,
            "profile_drop_workers": int(profile_drop_workers),
            "chart_trades_x_max": trades_x_cap,
        },
        "candidate_count": len(candidate_rows),
        "selected_count": len(shortlist_rows),
        "alternate_count": len(shortlist_board.get("alternates") or []),
        "candidate_trade_rate_summary": _trade_rate_summary(candidate_rows),
        "selected_trade_rate_summary": _trade_rate_summary(shortlist_rows),
        "selected_basket_summary": _build_selection_basket_summary(shortlist_rows),
        "selected_basket_curve_36m": _build_selection_basket_curve(shortlist_rows),
        "filter_rejections": filter_rejections,
        "selected_by_run": shortlist_board.get("selected_by_run") or {},
        "selected_by_strategy_key": shortlist_board.get("selected_by_strategy_key") or {},
        "selected": shortlist_rows,
        "alternates": shortlist_board.get("alternates") or [],
        "top_similarity_pairs": list((similarity_payload.get("pairs") or [])[:400]),
        "charts": {
            "corpus_score_vs_trades": str(charts_root / "corpus-score-vs-trades-36mo.png"),
            "shortlist_score_vs_trades": str(charts_root / "shortlist-score-vs-trades-36mo.png"),
            "shortlist_overlay_score_vs_trades": str(
                charts_root / "shortlist-overlay-score-vs-trades-36mo.png"
            ),
            "corpus_score_vs_drawdown": str(charts_root / "corpus-score-vs-drawdown-36mo.png"),
            "shortlist_score_vs_drawdown": str(charts_root / "shortlist-score-vs-drawdown-36mo.png"),
            "corpus_score_vs_sameness": str(charts_root / "corpus-score-vs-sameness-36mo.png"),
            "shortlist_score_vs_sameness": str(charts_root / "shortlist-score-vs-sameness-36mo.png"),
            "shortlist_similarity_heatmap": str(charts_root / "shortlist-similarity-heatmap.png"),
        },
        "profile_drop_phase": "pending"
        if generate_profile_drops and shortlist_rows
        else "skipped",
        "profile_drops": [],
    }
    write_json(report_root / "shortlist-report.json", shortlist_payload)
    write_csv(
        report_root / "shortlist-report.csv",
        [{"section": "selected", **row} for row in shortlist_rows]
        + [{"section": "alternate", **row} for row in (shortlist_board.get("alternates") or [])],
    )
    profile_drop_results: list[dict[str, Any]] = []
    if generate_profile_drops and shortlist_rows:
        profile_drop_results = _render_profile_drop_rows(
            config=config,
            rows=shortlist_rows,
            output_root=profile_drop_root,
            lookback_months=int(profile_drop_lookback_months),
            timeout_seconds=int(profile_drop_timeout_seconds),
            force_rebuild=force_rebuild_profile_drops,
            profile_drop_workers=int(profile_drop_workers),
            as_json=as_json,
            progress_label="shortlist profile drops",
        )
        shortlist_payload["generated_at"] = datetime.now().astimezone().isoformat()
        shortlist_payload["profile_drop_phase"] = "complete"
        shortlist_payload["profile_drops"] = profile_drop_results
        write_json(report_root / "shortlist-report.json", shortlist_payload)
    print(
        json.dumps(
            {
                "report_root": str(report_root),
                "is_canonical_report": is_canonical_report,
                "shortlist_json": str(report_root / "shortlist-report.json"),
                "shortlist_csv": str(report_root / "shortlist-report.csv"),
                "candidate_count": len(candidate_rows),
                "selected_count": len(shortlist_rows),
                "alternate_count": len(shortlist_board.get("alternates") or []),
                "profile_drop_rendered": sum(
                    1 for row in profile_drop_results if row.get("status") in {"rendered", "cached"}
                ),
                "profile_drop_failed": sum(
                    1 for row in profile_drop_results if row.get("status") == "failed"
                ),
                "charts": shortlist_payload["charts"],
                "selected": shortlist_rows,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_build_portfolio(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    portfolio_config: str | None,
    catch_up_full_backtests: bool | None,
    catch_up_force_rebuild: bool | None,
    catch_up_require_scrutiny_36: bool | None,
    generate_profile_drops: bool | None,
    export_bundle: bool | None,
    profile_drop_workers: int | None,
    as_json: bool,
) -> int:
    config = load_config()
    spec_path = (
        Path(portfolio_config).resolve()
        if portfolio_config
        else (config.repo_root / "portfolio.config.json")
    )
    portfolio_spec, used_defaults = load_portfolio_spec(spec_path)

    if catch_up_full_backtests is not None:
        portfolio_spec["catch_up_full_backtests"] = bool(catch_up_full_backtests)
    if catch_up_force_rebuild is not None:
        portfolio_spec["catch_up_force_rebuild"] = bool(catch_up_force_rebuild)
    if catch_up_require_scrutiny_36 is not None:
        portfolio_spec["catch_up_require_scrutiny_36"] = bool(catch_up_require_scrutiny_36)
    if generate_profile_drops is not None:
        portfolio_spec["generate_profile_drops"] = bool(generate_profile_drops)
    if export_bundle is not None:
        portfolio_spec["export_bundle"] = bool(export_bundle)
    if profile_drop_workers is not None:
        portfolio_spec["profile_drop_workers"] = max(1, int(profile_drop_workers))

    catch_up_summary: dict[str, Any] | None = None
    if bool(portfolio_spec.get("catch_up_full_backtests")):
        if as_json:
            with io.StringIO() as capture, redirect_stdout(capture):
                exit_code = cmd_calculate_full_backtests(
                    run_ids=run_ids,
                    attempt_ids=attempt_ids,
                    limit=None,
                    max_workers=None,
                    use_dev_sim_worker_count=True,
                    require_scrutiny_36=bool(
                        portfolio_spec.get("catch_up_require_scrutiny_36")
                    ),
                    force_rebuild=bool(portfolio_spec.get("catch_up_force_rebuild")),
                    job_timeout_seconds=(
                        int(portfolio_spec.get("full_backtest_job_timeout_seconds"))
                        if portfolio_spec.get("full_backtest_job_timeout_seconds")
                        is not None
                        else None
                    ),
                    as_json=True,
                )
        else:
            exit_code = cmd_calculate_full_backtests(
                run_ids=run_ids,
                attempt_ids=attempt_ids,
                limit=None,
                max_workers=None,
                use_dev_sim_worker_count=True,
                require_scrutiny_36=bool(portfolio_spec.get("catch_up_require_scrutiny_36")),
                force_rebuild=bool(portfolio_spec.get("catch_up_force_rebuild")),
                job_timeout_seconds=(
                    int(portfolio_spec.get("full_backtest_job_timeout_seconds"))
                    if portfolio_spec.get("full_backtest_job_timeout_seconds") is not None
                    else None
                ),
                as_json=False,
            )
        catch_up_summary = {
            "attempt_catalog_summary": load_json_if_exists(config.attempt_catalog_summary_path),
            "full_backtest_failures": load_json_if_exists(
                config.full_backtest_failures_json_path
            ),
        }
        if exit_code != 0:
            raise SystemExit("Full-backtest catch-up failed during build-portfolio.")

    run_dirs = _matching_run_dirs(config, run_ids)
    full_catalog_rows = _catalog_rows_for_run_dirs(
        config,
        run_dirs,
        progress_callback=_catalog_phase_callback("portfolio", as_json=as_json),
    )
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }
    rows = list(full_catalog_rows)
    if wanted_attempt_ids:
        rows = [
            row for row in rows if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    rows.sort(key=_full_backtest_priority_key)

    raw_sleeve_specs = list(portfolio_spec.get("sleeves") or [])
    sleeve_filters: list[dict[str, Any]] = []
    for sleeve_spec in raw_sleeve_specs:
        sleeve_name = str(sleeve_spec.get("name") or "sleeve").strip()
        candidate_rows, filter_rejections, max_drawdown_cap = filter_selection_candidate_rows(
            rows,
            candidate_limit=int(sleeve_spec.get("candidate_limit", -1)),
            min_score_36=float(sleeve_spec.get("min_score_36", 40.0)),
            min_retention_ratio=float(sleeve_spec.get("min_retention_ratio", 0.0)),
            min_trades_per_month=float(sleeve_spec.get("min_trades_per_month", 0.0)),
            max_drawdown_r=float(sleeve_spec.get("max_drawdown_r", -1.0)),
            require_full_backtest_36=bool(sleeve_spec.get("require_full_backtest_36", True)),
        )
        sleeve_filters.append(
            {
                "name": sleeve_name,
                "spec": dict(sleeve_spec),
                "candidate_rows": candidate_rows,
                "filter_rejections": filter_rejections,
                "max_drawdown_cap": max_drawdown_cap,
            }
        )

    union_candidate_rows = merge_portfolio_sleeves(
        [
            {
                "name": item["name"],
                "candidate_rows": item["candidate_rows"],
                "selected_rows": [],
            }
            for item in sleeve_filters
        ]
    ).get("candidate_rows") or []

    candidate_similarity_payload = build_candidate_similarity_payload(
        list(union_candidate_rows),
        progress_callback=_similarity_phase_callback(
            "portfolio-union", as_json=as_json
        ),
    )

    sleeve_results = []
    for sleeve in sleeve_filters:
        sleeve_name = str(sleeve.get("name") or "sleeve").strip()
        _phase_emit(f"[portfolio] building sleeve '{sleeve_name}'", as_json=as_json)
        sleeve_similarity_payload = subset_similarity_payload(
            candidate_similarity_payload,
            list(sleeve.get("candidate_rows") or []),
        )
        board = select_promotion_board(
            list(sleeve.get("candidate_rows") or []),
            sleeve_similarity_payload,
            board_size=int(sleeve["spec"].get("shortlist_size", 12)),
            novelty_penalty=float(sleeve["spec"].get("novelty_penalty", 18.0)),
            drawdown_penalty=float(sleeve["spec"].get("drawdown_penalty", 0.65)),
            trade_rate_bonus_weight=float(
                sleeve["spec"].get("trade_rate_bonus_weight", 0.0)
            ),
            trade_rate_bonus_target=float(
                sleeve["spec"].get("trade_rate_bonus_target", 8.0)
            ),
            max_drawdown_r=sleeve.get("max_drawdown_cap"),
            max_sameness_to_board=(
                None
                if float(sleeve["spec"].get("max_sameness_to_board", 0.78)) < 0.0
                else float(sleeve["spec"].get("max_sameness_to_board", 0.78))
            ),
            max_per_run=(
                None
                if int(sleeve["spec"].get("max_per_run", 1)) < 0
                else int(sleeve["spec"]["max_per_run"])
            ),
            max_per_strategy_key=(
                None
                if int(sleeve["spec"].get("max_per_strategy_key", 1)) < 0
                else int(sleeve["spec"]["max_per_strategy_key"])
            ),
        )
        selected_rows = [dict(row) for row in (board.get("selected") or [])]
        for rank, row in enumerate(selected_rows, start=1):
            row["sleeve_name"] = sleeve_name
            row["sleeve_selection_rank"] = rank
        sleeve_result = {
            "name": sleeve_name,
            "spec": dict(sleeve["spec"]),
            "candidate_rows": list(sleeve.get("candidate_rows") or []),
            "filter_rejections": sleeve.get("filter_rejections") or {},
            "similarity_payload": sleeve_similarity_payload,
            "board": board,
            "selected_rows": selected_rows,
        }
        _phase_emit(
            f"[portfolio] sleeve '{sleeve_name}' selected {len(selected_rows)} from {len(sleeve_result.get('candidate_rows') or [])} qualified",
            as_json=as_json,
        )
        sleeve_results.append(sleeve_result)

    merged = merge_portfolio_sleeves(sleeve_results)
    portfolio_rows = list(merged.get("selected_rows") or [])
    portfolio_candidate_rows = list(merged.get("candidate_rows") or [])
    _phase_emit(
        f"[portfolio] merged {len(portfolio_rows)} final selections from {len(portfolio_candidate_rows)} union candidates",
        as_json=as_json,
    )
    portfolio_similarity_payload = subset_similarity_payload(
        candidate_similarity_payload,
        portfolio_rows,
    )

    portfolio_name = str(portfolio_spec.get("portfolio_name") or "default-portfolio").strip()
    report_root = config.derived_root / "portfolio-report" / _slug_token(portfolio_name)
    report_root.mkdir(parents=True, exist_ok=True)
    charts_root, profile_drop_root = _portfolio_chart_paths(report_root)

    trades_x_cap = (
        None
        if float(portfolio_spec.get("chart_trades_x_max", 300.0)) < 0.0
        else float(portfolio_spec.get("chart_trades_x_max", 300.0))
    )
    render_attempt_tradeoff_scatter_artifacts(
        portfolio_candidate_rows,
        charts_root / "portfolio-candidate-score-vs-trades-36mo.png",
        charts_root / "portfolio-candidate-score-vs-trades-36mo.json",
        require_full_backtest_36=True,
        x_axis_max=trades_x_cap,
        title_prefix="Portfolio Candidate Union",
    )
    render_attempt_tradeoff_scatter_artifacts(
        portfolio_rows,
        charts_root / "portfolio-score-vs-trades-36mo.png",
        charts_root / "portfolio-score-vs-trades-36mo.json",
        require_full_backtest_36=True,
        x_axis_max=trades_x_cap,
        title_prefix="Portfolio",
    )
    render_attempt_tradeoff_overlay_artifacts(
        portfolio_candidate_rows,
        portfolio_rows,
        charts_root / "portfolio-overlay-score-vs-trades-36mo.png",
        charts_root / "portfolio-overlay-score-vs-trades-36mo.json",
        x_axis_max=trades_x_cap,
        title_prefix="Portfolio Overlay",
    )
    render_attempt_drawdown_scatter_artifacts(
        portfolio_candidate_rows,
        charts_root / "portfolio-candidate-score-vs-drawdown-36mo.png",
        charts_root / "portfolio-candidate-score-vs-drawdown-36mo.json",
        require_full_backtest_36=True,
        title_prefix="Portfolio Candidate Union",
    )
    render_attempt_drawdown_scatter_artifacts(
        portfolio_rows,
        charts_root / "portfolio-score-vs-drawdown-36mo.png",
        charts_root / "portfolio-score-vs-drawdown-36mo.json",
        require_full_backtest_36=True,
        title_prefix="Portfolio",
    )
    render_similarity_scatter_artifacts(
        candidate_similarity_payload,
        charts_root / "portfolio-candidate-score-vs-sameness-36mo.png",
    )
    render_similarity_scatter_artifacts(
        portfolio_similarity_payload,
        charts_root / "portfolio-score-vs-sameness-36mo.png",
    )
    render_similarity_heatmap_artifacts(
        portfolio_similarity_payload,
        charts_root / "portfolio-similarity-heatmap.png",
        charts_root / "portfolio-similarity-heatmap.json",
    )

    sleeves_payload: list[dict[str, Any]] = []
    for sleeve in sleeve_results:
        board = sleeve.get("board") or {}
        sleeves_payload.append(
            {
                "name": sleeve.get("name"),
                "spec": sleeve.get("spec") or {},
                "candidate_count": len(sleeve.get("candidate_rows") or []),
                "selected_count": len(sleeve.get("selected_rows") or []),
                "alternate_count": len(board.get("alternates") or []),
                "filter_rejections": sleeve.get("filter_rejections") or {},
                "selected_trade_rate_summary": _trade_rate_summary(
                    list(sleeve.get("selected_rows") or [])
                ),
                "candidate_trade_rate_summary": _trade_rate_summary(
                    list(sleeve.get("candidate_rows") or [])
                ),
                "selected": list(sleeve.get("selected_rows") or []),
                "alternates": list(board.get("alternates") or []),
            }
        )

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "portfolio_name": portfolio_name,
        "portfolio_config_path": str(spec_path),
        "portfolio_config_defaulted": used_defaults,
        "portfolio_spec": portfolio_spec,
        "catch_up_summary": catch_up_summary,
        "run_ids": run_ids,
        "attempt_ids": attempt_ids,
        "export_bundle": None,
        "candidate_union_count": len(portfolio_candidate_rows),
        "selected_union_count": len(portfolio_rows),
        "selected_overlap_count": int(merged.get("selected_overlap_count") or 0),
        "candidate_trade_rate_summary": _trade_rate_summary(portfolio_candidate_rows),
        "selected_trade_rate_summary": _trade_rate_summary(portfolio_rows),
        "selected_basket_summary": _build_selection_basket_summary(portfolio_rows),
        "selected_basket_curve_36m": _build_selection_basket_curve(portfolio_rows),
        "sleeves": sleeves_payload,
        "selected": portfolio_rows,
        "charts": {
            "portfolio_candidate_score_vs_trades": str(
                charts_root / "portfolio-candidate-score-vs-trades-36mo.png"
            ),
            "portfolio_score_vs_trades": str(
                charts_root / "portfolio-score-vs-trades-36mo.png"
            ),
            "portfolio_overlay_score_vs_trades": str(
                charts_root / "portfolio-overlay-score-vs-trades-36mo.png"
            ),
            "portfolio_candidate_score_vs_drawdown": str(
                charts_root / "portfolio-candidate-score-vs-drawdown-36mo.png"
            ),
            "portfolio_score_vs_drawdown": str(
                charts_root / "portfolio-score-vs-drawdown-36mo.png"
            ),
            "portfolio_candidate_score_vs_sameness": str(
                charts_root / "portfolio-candidate-score-vs-sameness-36mo.png"
            ),
            "portfolio_score_vs_sameness": str(
                charts_root / "portfolio-score-vs-sameness-36mo.png"
            ),
            "portfolio_similarity_heatmap": str(
                charts_root / "portfolio-similarity-heatmap.png"
            ),
        },
        "profile_drop_phase": "pending"
        if bool(portfolio_spec.get("generate_profile_drops")) and portfolio_rows
        else "skipped",
        "profile_drops": [],
    }
    write_json(report_root / "portfolio-report.json", payload)
    write_csv(
        report_root / "portfolio-report.csv",
        [{"section": "selected", **row} for row in portfolio_rows],
    )
    profile_drop_results: list[dict[str, Any]] = []
    if bool(portfolio_spec.get("generate_profile_drops")) and portfolio_rows:
        profile_drop_results = _render_profile_drop_rows(
            config=config,
            rows=portfolio_rows,
            output_root=profile_drop_root,
            lookback_months=int(portfolio_spec.get("profile_drop_lookback_months", 36)),
            timeout_seconds=int(portfolio_spec.get("profile_drop_timeout_seconds", 1800)),
            force_rebuild=False,
            profile_drop_workers=int(portfolio_spec.get("profile_drop_workers", 4)),
            as_json=as_json,
            progress_label="portfolio profile drops",
        )
        payload["generated_at"] = datetime.now().astimezone().isoformat()
        payload["profile_drop_phase"] = "complete"
        payload["profile_drops"] = profile_drop_results
        write_json(report_root / "portfolio-report.json", payload)
    export_bundle_summary = None
    if bool(portfolio_spec.get("export_bundle")):
        export_bundle_summary = _export_portfolio_bundle(
            config=config,
            payload=payload,
            report_root=report_root,
            report_path=report_root / "portfolio-report.json",
        )
        payload["export_bundle"] = export_bundle_summary
        write_json(report_root / "portfolio-report.json", payload)
    print(
        json.dumps(
            {
                "report_root": str(report_root),
                "portfolio_json": str(report_root / "portfolio-report.json"),
                "portfolio_csv": str(report_root / "portfolio-report.csv"),
                "portfolio_name": portfolio_name,
                "candidate_union_count": len(portfolio_candidate_rows),
                "selected_union_count": len(portfolio_rows),
                "selected_overlap_count": int(merged.get("selected_overlap_count") or 0),
                "profile_drop_rendered": sum(
                    1 for row in profile_drop_results if row.get("status") in {"rendered", "cached"}
                ),
                "profile_drop_failed": sum(
                    1 for row in profile_drop_results if row.get("status") == "failed"
                ),
                "export_bundle": export_bundle_summary,
                "charts": payload["charts"],
                "selected": portfolio_rows,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_hydrate_scrutiny_cache(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    lookback_months: list[int] | None,
    limit: int | None,
    force_rebuild: bool,
    as_json: bool,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()
    horizons = sorted({int(value) for value in (lookback_months or [12, 36]) if int(value) > 0})
    items = _matched_attempt_items(
        config,
        run_ids=run_ids,
        attempt_ids=attempt_ids,
        require_scored=True,
    )
    if limit is not None and limit >= 0:
        items = items[:limit]

    total = len(items)
    results: list[dict[str, Any]] = []
    rebuilt = 0
    cache_hits = 0
    seeded = 0
    failed = 0
    derived_refresh = None

    use_progress = (
        (not as_json)
        and (not PLAIN_PROGRESS_MODE)
        and bool(getattr(console, "is_terminal", False))
    )
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
        task_id = progress.add_task("hydrate scrutiny cache", total=total or 1)
        for index, (run_dir, attempts, attempt) in enumerate(items, start=1):
            progress.update(
                task_id,
                description=(
                    f"{index}/{total} "
                    f"[green]rebuilt={rebuilt}[/green] "
                    f"[cyan]hits={cache_hits}[/cyan] "
                    f"[yellow]seeded={seeded}[/yellow] "
                    f"[red]fail={failed}[/red] "
                    f"{run_dir.name}"
                ),
            )
            emit(f"scrutiny {index}/{total} {run_dir.name} {attempt.get('attempt_id')}")
            attempt_result: dict[str, Any] = {
                "run_id": run_dir.name,
                "attempt_id": str(attempt.get("attempt_id") or ""),
                "candidate_name": attempt.get("candidate_name"),
                "horizons": [],
            }
            try:
                for horizon in horizons:
                    payload = _ensure_attempt_scrutiny_artifacts(
                        config=config,
                        cli=cli,
                        run_dir=run_dir,
                        attempts=attempts,
                        attempt=attempt,
                        lookback_months=horizon,
                        force_rebuild=force_rebuild,
                        emit=emit,
                    )
                    cache_hit = bool(payload.get("cache_hit"))
                    seed_source = payload.get("seed_source")
                    if cache_hit:
                        cache_hits += 1
                    else:
                        rebuilt += 1
                    if seed_source:
                        seeded += 1
                    attempt_result["horizons"].append(
                        {
                            "lookback_months": horizon,
                            "artifact_dir": payload.get("artifact_dir"),
                            "score": payload.get("score"),
                            "score_basis": payload.get("score_basis"),
                            "cache_hit": cache_hit,
                            "seed_source": seed_source,
                        }
                    )
                attempt_result["status"] = "ok"
            except Exception as exc:
                failed += 1
                attempt_result["status"] = "failed"
                attempt_result["error"] = str(exc)
                emit(f"  failed: {exc}")
            results.append(attempt_result)
            progress.advance(task_id, 1)

    derived_refresh = _refresh_global_derived_corpus_state(config)

    payload = {
        "attempts_considered": len(items),
        "horizons": horizons,
        "rebuilt": rebuilt,
        "cache_hits": cache_hits,
        "seeded": seeded,
        "failed": failed,
        "derived_refresh": derived_refresh,
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if failed == 0 else 1


def cmd_build_promotion_board(
    *,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    candidate_limit: int,
    board_size: int,
    min_score_36: float,
    min_retention_ratio: float,
    min_trades_per_month: float,
    novelty_penalty: float,
    max_per_run: int,
    max_per_strategy_key: int,
    max_sameness_to_board: float,
    require_full_backtest_36: bool,
    hydrate_missing: bool,
    force_rebuild: bool,
    as_json: bool,
) -> int:
    config = load_config()
    run_dirs = _matching_run_dirs(config, run_ids)
    full_catalog_rows = _catalog_rows_for_run_dirs(config, run_dirs)
    initial_rows = list(full_catalog_rows)
    wanted_attempt_ids = {
        token.strip() for token in (attempt_ids or []) if str(token).strip()
    }

    def promotion_sort_key(row: dict[str, Any]) -> tuple[bool, float, float, str]:
        score_36 = row.get("score_36m")
        composite_score = row.get("composite_score")
        primary = (
            float(score_36)
            if score_36 is not None
            else (
                float(composite_score)
                if composite_score is not None
                else float("-inf")
            )
        )
        secondary = (
            float(composite_score)
            if composite_score is not None
            else float("-inf")
        )
        return (
            primary == float("-inf"),
            -primary,
            -secondary,
            str(row.get("attempt_id") or ""),
        )

    if wanted_attempt_ids:
        initial_rows = [
            row
            for row in initial_rows
            if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    initial_rows.sort(key=promotion_sort_key)
    if candidate_limit >= 0:
        initial_rows = initial_rows[:candidate_limit]

    if hydrate_missing and initial_rows:
        cli = FuzzfolioCli(config.fuzzfolio)
        cli.ensure_login()
        hydrate_attempt_ids = [str(row.get("attempt_id") or "") for row in initial_rows]
        horizons = [36]
        if min_retention_ratio > 0.0:
            horizons = [12, 36]
        hydrate_items = _matched_attempt_items(
            config,
            run_ids=run_ids,
            attempt_ids=hydrate_attempt_ids,
            require_scored=True,
        )
        for run_dir, attempts, attempt in hydrate_items:
            for horizon in horizons:
                _ensure_attempt_scrutiny_artifacts(
                    config=config,
                    cli=cli,
                    run_dir=run_dir,
                    attempts=attempts,
                    attempt=attempt,
                    lookback_months=horizon,
                    force_rebuild=force_rebuild,
                    emit=None,
                )

    full_catalog_rows = _catalog_rows_for_run_dirs(config, run_dirs)
    rows = list(full_catalog_rows)
    if wanted_attempt_ids:
        rows = [
            row for row in rows if str(row.get("attempt_id") or "") in wanted_attempt_ids
        ]
    rows.sort(key=promotion_sort_key)
    if candidate_limit >= 0:
        rows = rows[:candidate_limit]

    filtered_rows = []
    filter_rejections = {
        "missing_score_36m": 0,
        "score_below_min_score_36": 0,
        "missing_trades_per_month_36m": 0,
        "trades_below_min_trades_per_month": 0,
        "missing_retention_ratio_36m_vs_12m": 0,
        "retention_below_min_retention_ratio": 0,
        "missing_full_backtest_36m": 0,
        "invalid_full_backtest_36m": 0,
    }
    for row in rows:
        score_36 = row.get("score_36m")
        trades_per_month_36 = row.get("trades_per_month_36m")
        retention_ratio = row.get("score_retention_ratio_36m_vs_12m")
        if score_36 is None:
            filter_rejections["missing_score_36m"] += 1
            continue
        if float(score_36) < float(min_score_36):
            filter_rejections["score_below_min_score_36"] += 1
            continue
        if min_trades_per_month > 0.0:
            if trades_per_month_36 is None:
                filter_rejections["missing_trades_per_month_36m"] += 1
                continue
            if float(trades_per_month_36) < float(min_trades_per_month):
                filter_rejections["trades_below_min_trades_per_month"] += 1
                continue
        if min_retention_ratio > 0.0:
            if retention_ratio is None:
                filter_rejections["missing_retention_ratio_36m_vs_12m"] += 1
                continue
            if float(retention_ratio) < float(min_retention_ratio):
                filter_rejections["retention_below_min_retention_ratio"] += 1
                continue
        if require_full_backtest_36 and not bool(row.get("has_full_backtest_36m")):
            filter_rejections["missing_full_backtest_36m"] += 1
            continue
        if (
            require_full_backtest_36
            and str(row.get("full_backtest_validation_status_36m") or "") != "valid"
        ):
            filter_rejections["invalid_full_backtest_36m"] += 1
            continue
        filtered_rows.append(row)

    similarity_payload = build_candidate_similarity_payload(filtered_rows)
    board = select_promotion_board(
        filtered_rows,
        similarity_payload,
        board_size=board_size,
        novelty_penalty=novelty_penalty,
        max_sameness_to_board=max_sameness_to_board,
        max_per_run=(None if max_per_run < 0 else max_per_run),
        max_per_strategy_key=(
            None if max_per_strategy_key < 0 else max_per_strategy_key
        ),
    )
    full_catalog_summary = catalog_summary(full_catalog_rows)
    provisional_reasons = full_backtest_provisional_reasons(
        full_catalog_summary,
        require_full_backtest_36=require_full_backtest_36,
        selected_rows=list(board.get("selected") or []),
    )
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "status": "provisional" if provisional_reasons else "ready_for_review",
        "provisional_reasons": provisional_reasons,
        "filters": {
            "run_ids": run_ids,
            "attempt_ids": attempt_ids,
            "candidate_limit": candidate_limit,
            "board_size": board_size,
            "min_score_36": min_score_36,
            "min_retention_ratio": min_retention_ratio,
            "min_trades_per_month": min_trades_per_month,
            "novelty_penalty": novelty_penalty,
            "max_per_run": max_per_run,
            "max_per_strategy_key": max_per_strategy_key,
            "max_sameness_to_board": max_sameness_to_board,
            "require_full_backtest_36": require_full_backtest_36,
            "hydrate_missing": hydrate_missing,
            "force_rebuild": force_rebuild,
        },
        "coverage": {
            "attempt_count": full_catalog_summary.get("attempt_count"),
            "attempts_with_scrutiny_36m": full_catalog_summary.get(
                "attempts_with_scrutiny_36m"
            ),
            "attempts_with_full_backtest_36m": full_catalog_summary.get(
                "attempts_with_full_backtest_36m"
            ),
            "attempts_with_valid_full_backtest_36m": full_catalog_summary.get(
                "attempts_with_valid_full_backtest_36m"
            ),
            "attempts_with_invalid_full_backtest_36m": full_catalog_summary.get(
                "attempts_with_invalid_full_backtest_36m"
            ),
            "full_backtest_36m_vs_scrutiny_coverage_ratio": full_catalog_summary.get(
                "full_backtest_36m_vs_scrutiny_coverage_ratio"
            ),
            "valid_full_backtest_36m_vs_scrutiny_coverage_ratio": full_catalog_summary.get(
                "valid_full_backtest_36m_vs_scrutiny_coverage_ratio"
            ),
        },
        "filter_rejections": filter_rejections,
        "candidate_count": len(filtered_rows),
        "similarity_pair_count": len(similarity_payload.get("pairs") or []),
        "selected_by_run": board.get("selected_by_run") or {},
        "selected_by_strategy_key": board.get("selected_by_strategy_key") or {},
        "selected": board.get("selected") or [],
        "alternates": board.get("alternates") or [],
        "top_similarity_pairs": list((similarity_payload.get("pairs") or [])[:200]),
    }
    derived_refresh = _refresh_global_derived_corpus_state(config)

    write_json(config.promotion_board_json_path, payload)
    csv_rows = [
        {"section": "selected", **row} for row in (payload["selected"])
    ] + [{"section": "alternate", **row} for row in (payload["alternates"])]
    write_csv(config.promotion_board_csv_path, csv_rows)
    print(
        json.dumps(
            {
                "promotion_board_json": str(config.promotion_board_json_path),
                "promotion_board_csv": str(config.promotion_board_csv_path),
                "status": payload["status"],
                "derived_refresh": derived_refresh,
                "provisional_reasons": payload["provisional_reasons"],
                "candidate_count": payload["candidate_count"],
                "selected_count": len(payload["selected"]),
                "alternate_count": len(payload["alternates"]),
                "similarity_pair_count": payload["similarity_pair_count"],
                "selected_by_run": payload["selected_by_run"],
                "selected_by_strategy_key": payload["selected_by_strategy_key"],
                "selected": payload["selected"],
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


def _nuke_deep_cache_artifacts(
    *,
    runs_root: Path,
    derived_root: Path,
    summary_timestamp: str | None = None,
) -> dict[str, Any]:
    runs_root.mkdir(parents=True, exist_ok=True)
    blocked: list[dict[str, str]] = []

    def _delete_file(path: Path) -> bool:
        try:
            path.unlink()
            return True
        except OSError as exc:
            blocked.append({"path": str(path), "error": str(exc)})
            return False

    def _delete_tree(path: Path) -> bool:
        try:
            shutil.rmtree(path)
            return True
        except OSError as exc:
            blocked.append({"path": str(path), "error": str(exc)})
            return False

    full_backtest_curve_files = list(runs_root.glob("**/full-backtest-36mo-curve.json"))
    full_backtest_result_files = list(
        runs_root.glob("**/full-backtest-36mo-result.json")
    )
    scrutiny_cache_dirs = [
        path for path in runs_root.glob("**/scrutiny-cache") if path.is_dir()
    ]
    run_profile_drop_pngs = [
        path
        for path in runs_root.glob("**/profile-drop-*.png")
        if derived_root not in path.parents
    ]
    run_profile_drop_manifests = [
        path
        for path in runs_root.glob("**/profile-drop-*.manifest.json")
        if derived_root not in path.parents
    ]

    deleted_curve_files = sum(1 for path in full_backtest_curve_files if _delete_file(path))
    deleted_result_files = sum(
        1 for path in full_backtest_result_files if _delete_file(path)
    )
    deleted_scrutiny_cache_dirs = sum(
        1 for path in scrutiny_cache_dirs if _delete_tree(path)
    )
    deleted_run_profile_drop_pngs = sum(
        1 for path in run_profile_drop_pngs if _delete_file(path)
    )
    deleted_run_profile_drop_manifests = sum(
        1 for path in run_profile_drop_manifests if _delete_file(path)
    )

    derived_entries_before_reset = 0
    deleted_derived_root = False
    if derived_root.exists():
        derived_entries_before_reset = len(list(derived_root.iterdir()))
        deleted_derived_root = _delete_tree(derived_root)
    derived_root.mkdir(parents=True, exist_ok=True)

    timestamp_token = summary_timestamp or datetime.now().astimezone().strftime(
        "%Y%m%dT%H%M%S"
    )
    summary_path = derived_root / f"deep-cache-reset-{timestamp_token}.json"
    summary = {
        "runs_root": str(runs_root),
        "derived_root": str(derived_root),
        "summary_path": str(summary_path),
        "deleted_full_backtest_curve_files": deleted_curve_files,
        "deleted_full_backtest_result_files": deleted_result_files,
        "deleted_scrutiny_cache_dirs": deleted_scrutiny_cache_dirs,
        "deleted_run_profile_drop_pngs": deleted_run_profile_drop_pngs,
        "deleted_run_profile_drop_manifests": deleted_run_profile_drop_manifests,
        "derived_entries_before_reset": derived_entries_before_reset,
        "deleted_derived_root": deleted_derived_root,
        "blocked_entries": blocked,
    }
    write_json(summary_path, summary)
    return summary


def cmd_nuke_deep_caches(*, as_json: bool) -> int:
    config = load_config()
    summary = _nuke_deep_cache_artifacts(
        runs_root=config.runs_root,
        derived_root=config.derived_root,
    )
    if as_json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0
    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"Full-backtest curves deleted: {summary['deleted_full_backtest_curve_files']}",
                    f"Full-backtest results deleted: {summary['deleted_full_backtest_result_files']}",
                    f"Scrutiny-cache dirs deleted: {summary['deleted_scrutiny_cache_dirs']}",
                    f"Run profile-drop PNGs deleted: {summary['deleted_run_profile_drop_pngs']}",
                    f"Run profile-drop manifests deleted: {summary['deleted_run_profile_drop_manifests']}",
                    f"Derived entries reset: {summary['derived_entries_before_reset']}",
                    f"Summary: {summary['summary_path']}",
                    "Next: uv run autoresearch build-portfolio",
                ]
            ),
            title="Deep Cache Reset",
            border_style="yellow",
        )
    )
    return 0


def _latest_portfolio_report_path(config) -> Path | None:
    root = config.derived_root / "portfolio-report"
    if not root.exists():
        return None
    candidates = sorted(
        root.glob("*/portfolio-report.json"),
        key=lambda path: path.stat().st_mtime_ns if path.exists() else 0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _portfolio_bundle_root(config, portfolio_name: str) -> Path:
    return config.derived_root / "portfolio-exports" / _slug_token(portfolio_name)


def _copy_if_exists(source: Path | None, destination: Path) -> bool:
    if source is None or not source.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return True


def _human_bundle_item_token(
    candidate_name: str,
    attempt_id: str,
    used_tokens: set[str],
) -> str:
    base_token = _sanitize_report_token(candidate_name, fallback=attempt_id or "attempt")
    token = base_token
    suffix = 2
    while token in used_tokens:
        token = f"{base_token}-{suffix}"
        suffix += 1
    used_tokens.add(token)
    return token


def _export_portfolio_bundle(
    *,
    config,
    payload: dict[str, Any],
    report_root: Path,
    report_path: Path,
) -> dict[str, Any]:
    portfolio_name = str(payload.get("portfolio_name") or "default-portfolio").strip()
    export_root = _portfolio_bundle_root(config, portfolio_name)
    export_root.mkdir(parents=True, exist_ok=True)
    timestamp_token = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
    bundle_root = export_root / timestamp_token
    bundle_root.mkdir(parents=True, exist_ok=True)

    selected_rows = list(payload.get("selected") or [])
    profile_drop_lookup = {
        str(item.get("attempt_id") or "").strip(): dict(item)
        for item in list(payload.get("profile_drops") or [])
        if str(item.get("attempt_id") or "").strip()
    }

    exported_profiles = 0
    missing_profiles: list[str] = []
    exported_drop_pngs = 0
    missing_drop_attempts: list[str] = []
    manifest_rows: list[dict[str, Any]] = []
    used_item_tokens: set[str] = set()

    for rank, row in enumerate(selected_rows, start=1):
        attempt_id = str(row.get("attempt_id") or "").strip()
        candidate_name = str(row.get("candidate_name") or "").strip()
        item_token = _human_bundle_item_token(
            candidate_name=candidate_name,
            attempt_id=attempt_id,
            used_tokens=used_item_tokens,
        )
        item_root = bundle_root / item_token
        item_root.mkdir(parents=True, exist_ok=True)
        profile_path_raw = str(row.get("profile_path") or "").strip()
        profile_path = Path(profile_path_raw).resolve() if profile_path_raw else None
        profile_export_path = item_root / f"{item_token}.json"
        has_profile = _copy_if_exists(profile_path, profile_export_path)
        if has_profile:
            exported_profiles += 1
        else:
            missing_profiles.append(attempt_id)

        drop_item = profile_drop_lookup.get(attempt_id) or {}
        png_path_raw = str(drop_item.get("png_path") or "").strip()
        png_path = Path(png_path_raw).resolve() if png_path_raw else None
        png_export_path = item_root / f"{item_token}.png"
        has_drop_png = False
        if _copy_if_exists(png_path, png_export_path):
            exported_drop_pngs += 1
            has_drop_png = True
        if not has_drop_png:
            missing_drop_attempts.append(attempt_id)

        manifest_rows.append(
            {
                "selection_rank": rank,
                "attempt_id": attempt_id,
                "run_id": str(row.get("run_id") or ""),
                "candidate_name": candidate_name,
                "profile_ref": str(drop_item.get("profile_ref") or row.get("profile_ref") or ""),
                "export_dir": str(item_root),
                "profile_export_path": str(profile_export_path) if has_profile else None,
                "drop_png_export_path": str(png_export_path) if has_drop_png else None,
            }
        )

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "portfolio_name": portfolio_name,
        "bundle_root": str(bundle_root),
        "report_path": str(report_path),
        "selected_count": len(selected_rows),
        "exported_profiles": exported_profiles,
        "missing_profiles": missing_profiles,
        "exported_drop_pngs": exported_drop_pngs,
        "missing_drop_attempts": missing_drop_attempts,
        "selected_rows": manifest_rows,
    }
    return summary


def cmd_export_portfolio_bundle(*, portfolio_report: str | None, as_json: bool) -> int:
    config = load_config()
    report_path = (
        Path(portfolio_report).resolve() if portfolio_report else _latest_portfolio_report_path(config)
    )
    if report_path is None or not report_path.exists():
        raise SystemExit("No portfolio-report.json found to export.")
    payload = load_json_if_exists(report_path)
    if not isinstance(payload, dict) or not payload:
        raise SystemExit(f"Portfolio report is missing or invalid: {report_path}")
    summary = _export_portfolio_bundle(
        config=config,
        payload=payload,
        report_root=report_path.parent,
        report_path=report_path,
    )
    if as_json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0
    console.print(
        Panel.fit(
            "\n".join(
                [
                    f"Portfolio: {summary['portfolio_name']}",
                    f"Bundle: {summary['bundle_root']}",
                    f"Selected: {summary['selected_count']}",
                    f"Profiles exported: {summary['exported_profiles']}",
                    f"Drop PNGs exported: {summary['exported_drop_pngs']}",
                ]
            ),
            title="Portfolio Bundle Export",
            border_style="green",
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
    snapshot = (
        load_sensitivity_snapshot(artifact_dir.resolve())
        if snapshot_path.exists()
        else None
    )
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
        print(
            json.dumps(
                {"runs_updated": 0, "updated": 0, "skipped": 0, "attempts": 0},
                ensure_ascii=True,
                indent=2,
            )
        )
        return 0

    for run_dir in sorted(
        path
        for path in config.runs_root.iterdir()
        if path.is_dir() and path.name != "derived"
    ):
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
    if args.command == "calculate-full-backtests":
        return cmd_calculate_full_backtests(
            run_ids=args.run_ids,
            attempt_ids=args.attempt_id,
            limit=args.limit,
            max_workers=args.max_workers,
            use_dev_sim_worker_count=not bool(args.no_use_dev_sim_worker_count),
            require_scrutiny_36=bool(args.require_scrutiny_36),
            force_rebuild=bool(args.force_rebuild),
            job_timeout_seconds=None,
            as_json=bool(args.json),
        )
    if args.command == "build-attempt-catalog":
        return cmd_build_attempt_catalog(
            run_ids=args.run_id,
            as_json=bool(args.json),
        )
    if args.command == "audit-full-backtests":
        return cmd_audit_full_backtests(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            as_json=bool(args.json),
        )
    if args.command == "plot-corpus-score-vs-trades":
        return cmd_plot_corpus_score_vs_trades(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            require_full_backtest_36=bool(args.require_full_backtest_36),
            x_axis_max=float(args.x_axis_max),
            as_json=bool(args.json),
        )
    if args.command == "hydrate-scrutiny-cache":
        return cmd_hydrate_scrutiny_cache(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            lookback_months=args.lookback_months,
            limit=args.limit,
            force_rebuild=bool(args.force_rebuild),
            as_json=bool(args.json),
        )
    if args.command == "build-promotion-board":
        return cmd_build_promotion_board(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            candidate_limit=int(args.candidate_limit),
            board_size=int(args.board_size),
            min_score_36=float(args.min_score_36),
            min_retention_ratio=float(args.min_retention_ratio),
            min_trades_per_month=float(args.min_trades_per_month),
            novelty_penalty=float(args.novelty_penalty),
            max_per_run=int(args.max_per_run),
            max_per_strategy_key=int(args.max_per_strategy_key),
            max_sameness_to_board=float(args.max_sameness_to_board),
            require_full_backtest_36=bool(args.require_full_backtest_36),
            hydrate_missing=bool(args.hydrate_missing),
            force_rebuild=bool(args.force_rebuild),
            as_json=bool(args.json),
        )
    if args.command == "build-shortlist-report":
        return cmd_build_shortlist_report(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            candidate_limit=int(args.candidate_limit),
            shortlist_size=int(args.shortlist_size),
            min_score_36=float(args.min_score_36),
            min_retention_ratio=float(args.min_retention_ratio),
            min_trades_per_month=float(args.min_trades_per_month),
            max_drawdown_r=float(args.max_drawdown_r),
            drawdown_penalty=float(args.drawdown_penalty),
            trade_rate_bonus_weight=float(args.trade_rate_bonus_weight),
            trade_rate_bonus_target=float(args.trade_rate_bonus_target),
            novelty_penalty=float(args.novelty_penalty),
            max_per_run=int(args.max_per_run),
            max_per_strategy_key=int(args.max_per_strategy_key),
            max_sameness_to_board=float(args.max_sameness_to_board),
            require_full_backtest_36=bool(args.require_full_backtest_36),
            generate_profile_drops=bool(args.generate_profile_drops),
            profile_drop_lookback_months=int(args.profile_drop_lookback_months),
            chart_trades_x_max=float(args.chart_trades_x_max),
            profile_drop_timeout_seconds=int(args.profile_drop_timeout_seconds),
            profile_drop_workers=int(args.profile_drop_workers),
            force_rebuild_profile_drops=bool(args.force_rebuild_profile_drops),
            as_json=bool(args.json),
        )
    if args.command == "build-portfolio":
        return cmd_build_portfolio(
            run_ids=args.run_id,
            attempt_ids=args.attempt_id,
            portfolio_config=args.portfolio_config,
            catch_up_full_backtests=args.catch_up_full_backtests,
            catch_up_force_rebuild=args.catch_up_force_rebuild,
            catch_up_require_scrutiny_36=args.catch_up_require_scrutiny_36,
            generate_profile_drops=args.generate_profile_drops,
            export_bundle=args.export_bundle,
            profile_drop_workers=args.profile_drop_workers,
            as_json=bool(args.json),
        )
    if args.command == "export-portfolio-bundle":
        return cmd_export_portfolio_bundle(
            portfolio_report=args.portfolio_report,
            as_json=bool(args.json),
        )
    if args.command == "nuke-deep-caches":
        return cmd_nuke_deep_caches(as_json=bool(args.json))
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
