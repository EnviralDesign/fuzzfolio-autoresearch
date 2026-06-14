from __future__ import annotations

import atexit
import copy
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from .calendar_robustness import compute_calendar_robustness, evaluate_calendar_gate
from .config import AppConfig, load_config
from .execution_costs import execution_cost_cli_args
from .fuzzfolio import CliError, FuzzfolioCli
from .ledger import (
    append_attempt,
    attempts_path_for_run_dir,
    load_attempts,
    make_attempt_record,
    write_attempts,
    write_run_metadata,
)
from .playhand_health import build_play_hand_evidence, build_play_hand_health
from .plotting import render_progress_artifacts
from .scoring import build_attempt_score, load_sensitivity_snapshot


console = Console(safe_box=True)

DEFAULT_INSTRUMENT_POOL = (
    "EURUSD",
    "GBPUSD",
    "AUDUSD",
    "USDCAD",
    "NZDUSD",
    "USDJPY",
    "USDCHF",
    "XAUUSD",
)

SWEEP_PERMUTATION_HARD_LIMIT = 256
PLAY_HAND_SWEEP_PERMUTATION_LIMIT = 1024
SWEEP_BUDGET_PRESETS: dict[str, int] = {
    "low": 256,
    "medium": 640,
    "high": 1024,
}
INSTRUMENT_SCOUT_DEFAULT_SIZE = 5
INSTRUMENT_SCOUT_DEFAULT_MAX_SELECTED = 3
INSTRUMENT_SCOUT_DEFAULT_WORKERS = 4
INSTRUMENT_SCOUT_MAX_WORKERS = 8
INSTRUMENT_SCOUT_MIN_SCORE = 45.0
INSTRUMENT_SCOUT_SCORE_TOLERANCE = 12.0
INSTRUMENT_SCOUT_MAX_SIMILARITY = 0.72
INSTRUMENT_SCOUT_MIN_RESOLVED_TRADES = 3
EVOLUTIONARY_AXIS_MAX_VALUE_COUNT = 9
EVOLUTIONARY_BUDGET_PRESETS: dict[str, tuple[int, int]] = {
    "low": (32, 8),
    "medium": (40, 16),
    "high": (64, 16),
}
PLAY_HAND_REWARD_STEP_R = 0.5
PLAY_HAND_REWARD_COLUMN_LIMIT = 25
PLAY_HAND_HARD_MAX_REWARD_R = PLAY_HAND_REWARD_STEP_R * PLAY_HAND_REWARD_COLUMN_LIMIT
PLAY_HAND_DEFAULT_MAX_REWARD_R = 4.0
PLAY_HAND_RUNNER = "play_hand_v1"
PLAY_HAND_FINAL_SCRUTINY_MIN_SCORE = 0.0
PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON = "final_36mo_scrutiny_failed"
PLAY_HAND_CALENDAR_GATE_FAILED_REASON = "calendar_gate_failed"
PLAY_HAND_CALENDAR_GATE_ENV = "AUTORESEARCH_CALENDAR_GATE"
PLAY_HAND_CALENDAR_GATE_MODES = ("off", "report", "enforce")
PLAY_HAND_CALENDAR_GATE_DEFAULT_MODE = "report"
PLAY_HAND_SCREEN_ANCHOR_ENV = "AUTORESEARCH_SCREEN_ANCHOR_MODE"
PLAY_HAND_SCREEN_ANCHOR_MODES = ("now", "random")
PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MODE = "now"
PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MAX_OFFSET_MONTHS = 24
# The market-data lake holds a rolling ~36-month window. Keep the anchored
# screen window plus indicator warmup comfortably inside it so historical
# evaluations never run out of bars at the old edge.
PLAY_HAND_SCREEN_ANCHOR_OFFSET_BUDGET_MONTHS = 30
AVERAGE_DAYS_PER_MONTH = 30.4375
PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS = 2400
PLAY_HAND_DEFAULT_SWEEP_TIMEOUT_SECONDS = 7200
PLAY_HAND_EARLY_EXIT_ENV = "AUTORESEARCH_EARLY_EXIT_MODE"
PLAY_HAND_EARLY_EXIT_MODES = ("off", "report")
PLAY_HAND_EARLY_EXIT_DEFAULT_MODE = "off"
PLAY_HAND_EARLY_EXIT_VERSION = "early_exit_policy_v1"
PLAY_HAND_COARSE_HALVING_VERSION = "coarse_halving_v1"
PLAY_HAND_COARSE_HALVING_MODES = ("off", "enforce")
PLAY_HAND_COARSE_HALVING_DEFAULT_MODE = "off"
PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET = 128
PLAY_HAND_FAMILY_POLICY_EXECUTION_VERSION = "family_policy_execution_v1"
PLAY_HAND_FAMILY_POLICY_MODES = ("off", "report", "enforce")
PLAY_HAND_FAMILY_POLICY_DEFAULT_MODE = "off"
PLAY_HAND_FAMILY_POLICY_ACTIVE_POLICIES = ("template_locked", "template_guarded")
STAGE_ACCEPTANCE_DROP_TOLERANCE = 5.0
COARSE_HALVING_EXPAND_SCORE = 55.0
COARSE_HALVING_MIN_NEAR_INCUMBENT_SCORE = 50.0
COARSE_HALVING_NEAR_INCUMBENT_TOLERANCE = 5.0
PLAY_HAND_SEED_PLAN_PATH = Path("recipe-priors") / "play-hand-seed-plan.json"
SEED_TEMPLATE_CONFIG_KEYS = (
    "timeframe",
    "lookbackBars",
    "ranges",
    "talibConfig",
    "weight",
    "isTrendFollowing",
    "normalizationMode",
    "useFormingBar",
    "scale",
)


def _play_hand_eval_cli_timeout_seconds(job_timeout_seconds: int) -> int:
    # sensitivity-basket waits for deep replay and then may materialize selected
    # cell detail. Keep the subprocess watchdog outside the CLI's own envelope.
    return max(900, (int(job_timeout_seconds) * 2) + 300)


def _play_hand_sweep_cli_timeout_seconds(sweep_timeout_seconds: int) -> int:
    return max(1800, int(sweep_timeout_seconds) + 300)


def play_hand_reward_matrix(max_reward_r: float | None) -> dict[str, Any] | None:
    requested = PLAY_HAND_DEFAULT_MAX_REWARD_R if max_reward_r is None else float(max_reward_r)
    if not math.isfinite(requested) or requested <= 0:
        raise ValueError("--max-reward-r must be a positive finite number")
    columns = int(math.floor((requested / PLAY_HAND_REWARD_STEP_R) + 1e-9))
    if columns < 1:
        raise ValueError(
            f"--max-reward-r must be at least {PLAY_HAND_REWARD_STEP_R:g} "
            "with the current play-hand reward matrix step"
        )
    columns = min(columns, PLAY_HAND_REWARD_COLUMN_LIMIT)
    effective_max = round(PLAY_HAND_REWARD_STEP_R * columns, 6)
    return {
        "requested_max_reward_r": requested,
        "reward_step_r": PLAY_HAND_REWARD_STEP_R,
        "reward_columns": columns,
        "effective_max_reward_r": effective_max,
        "default_max_reward_r": PLAY_HAND_DEFAULT_MAX_REWARD_R,
        "hard_max_reward_r": PLAY_HAND_HARD_MAX_REWARD_R,
        "is_default_cap": max_reward_r is None,
        "is_active_cap": effective_max < PLAY_HAND_HARD_MAX_REWARD_R,
    }


def _reward_matrix_cli_args(reward_matrix: dict[str, Any] | None) -> list[str]:
    if not reward_matrix:
        return []
    return [
        "--reward-step-r",
        f"{float(reward_matrix['reward_step_r']):g}",
        "--reward-columns",
        str(int(reward_matrix["reward_columns"])),
    ]


_SWEEP_REWARD_MATRIX_SUPPORT: bool | None = None


def _sweep_reward_matrix_supported(cli: FuzzfolioCli) -> bool:
    global _SWEEP_REWARD_MATRIX_SUPPORT
    if _SWEEP_REWARD_MATRIX_SUPPORT is not None:
        return _SWEEP_REWARD_MATRIX_SUPPORT
    try:
        result = cli.run(
            ["sweep", "run", "--help"],
            check=False,
            timeout_seconds=10,
        )
    except Exception:
        _SWEEP_REWARD_MATRIX_SUPPORT = False
        return False
    text = f"{result.stdout}\n{result.stderr}"
    _SWEEP_REWARD_MATRIX_SUPPORT = (
        result.returncode == 0
        and "--reward-step-r" in text
        and "--reward-columns" in text
    )
    return _SWEEP_REWARD_MATRIX_SUPPORT


def resolve_sweep_budget(
    *,
    sweep_budget: str | None = None,
    max_sweep_permutations: int | None = None,
    evolutionary_budget: str | None = None,
) -> dict[str, Any]:
    if max_sweep_permutations is not None:
        value = max(1, int(max_sweep_permutations))
        return {
            "label": f"custom:{value}",
            "tier": None,
            "value": value,
            "source": "max_sweep_permutations",
        }
    tier = str(sweep_budget or evolutionary_budget or "high").strip().lower()
    if tier not in SWEEP_BUDGET_PRESETS:
        tier = "high"
    return {
        "label": tier,
        "tier": tier,
        "value": SWEEP_BUDGET_PRESETS[tier],
        "source": (
            "sweep_budget"
            if sweep_budget
            else "evolutionary_budget"
            if evolutionary_budget
            else "default"
        ),
    }


def _evolutionary_shape_for_budget(evaluation_budget: int) -> tuple[int, int]:
    budget = max(1, int(evaluation_budget))
    min_population = min(16, budget)
    max_population = min(64, budget)
    best_population = max_population
    best_generations = 1
    best_score: tuple[int, int, int] | None = None
    for population_size in range(min_population, max_population + 1):
        max_generations = max(1, (budget + population_size - 1) // population_size)
        planned = population_size * max_generations
        score = (planned - budget, -population_size, max_generations)
        if best_score is None or score < best_score:
            best_score = score
            best_population = population_size
            best_generations = max_generations
    return best_population, best_generations


def evolutionary_budget_settings(sweep_budget: str | int, evaluation_budget: int | None = None) -> dict[str, int]:
    tier = str(sweep_budget or "").strip().lower()
    if tier in EVOLUTIONARY_BUDGET_PRESETS:
        population_size, max_generations = EVOLUTIONARY_BUDGET_PRESETS[tier]
    else:
        if evaluation_budget is not None:
            budget_value = evaluation_budget
        else:
            try:
                budget_value = int(sweep_budget)
            except (TypeError, ValueError):
                budget_value = SWEEP_BUDGET_PRESETS["high"]
        population_size, max_generations = _evolutionary_shape_for_budget(budget_value)
    return {
        "population_size": population_size,
        "max_generations": max_generations,
        "evaluation_budget": population_size * max_generations,
    }


CANDLESTICK_PATTERN_BUNDLES: dict[str, list[str]] = {
    "broad_default": [
        "CDLENGULFING",
        "CDLHAMMER",
        "CDLDOJI",
        "CDLSHOOTINGSTAR",
        "CDLMORNINGSTAR",
        "CDLEVENINGSTAR",
    ],
    "major_reversal": [
        "CDLENGULFING",
        "CDLHAMMER",
        "CDLINVERTEDHAMMER",
        "CDLSHOOTINGSTAR",
        "CDLMORNINGSTAR",
        "CDLEVENINGSTAR",
        "CDLPIERCING",
        "CDLDARKCLOUDCOVER",
    ],
    "doji_reversal": [
        "CDLDOJI",
        "CDLDOJISTAR",
        "CDLDRAGONFLYDOJI",
        "CDLGRAVESTONEDOJI",
        "CDLMORNINGDOJISTAR",
        "CDLEVENINGDOJISTAR",
    ],
    "three_bar_reversal": [
        "CDL3BLACKCROWS",
        "CDL3WHITESOLDIERS",
        "CDL3INSIDE",
        "CDL3OUTSIDE",
        "CDL3LINESTRIKE",
    ],
    "body_pressure": [
        "CDLBELTHOLD",
        "CDLCLOSINGMARUBOZU",
        "CDLLONGLINE",
        "CDLMARUBOZU",
        "CDLSHORTLINE",
        "CDLSPINNINGTOP",
    ],
}

ROLE_TIMEFRAME_POOLS: dict[str, tuple[str, ...]] = {
    "trigger": ("M1", "M5", "M15"),
    "setup": ("M5", "M15", "M30", "H1"),
    "context": ("M30", "H1", "H4", "D1"),
    "filter": ("M30", "H1", "H4", "D1"),
}

TIMEFRAME_MINUTES: dict[str, int] = {
    "M1": 1,
    "M5": 5,
    "M15": 15,
    "M30": 30,
    "H1": 60,
    "H4": 240,
    "D1": 1440,
}


@dataclass
class PlayHandContext:
    config: AppConfig
    cli: FuzzfolioCli
    run_id: str
    run_dir: Path
    profiles_dir: Path
    evals_dir: Path
    attempts_path: Path
    events_path: Path
    summary_path: Path
    dry_run: bool = False
    job_timeout_seconds: int = PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS
    sweep_timeout_seconds: int = PLAY_HAND_DEFAULT_SWEEP_TIMEOUT_SECONDS
    registered_profile_refs: list[str] = field(default_factory=list)
    io_lock: Any = field(default_factory=threading.RLock, repr=False)


@dataclass(frozen=True)
class PlayHandStage:
    index: int
    total: int
    label: str

    @property
    def prefix(self) -> str:
        return f"[{self.index}/{self.total}]"

    def event_payload(self) -> dict[str, Any]:
        return {
            "stage_index": self.index,
            "stage_total": self.total,
            "stage_label": self.label,
        }


@dataclass(frozen=True)
class SeedIndicator:
    id: str
    signal_role: str | None = None
    signal_persistence: str | None = None
    preferred_timeframe_role: str | None = None

    def as_metadata(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "signalRole": self.signal_role,
            "signalPersistence": self.signal_persistence,
            "preferredTimeframeRole": self.preferred_timeframe_role,
        }


@dataclass(frozen=True)
class SweepAxisCandidate:
    axis: str
    key: str
    values: list[Any]
    current_value: Any
    kind: str
    role: str
    priority: float
    min_count: int
    max_count: int


@dataclass(frozen=True)
class SweepAxisPlan:
    axes: list[str]
    original_axes: list[str]
    original_permutations: int
    selected_permutations: int
    max_permutations: int
    search_mode: str
    axis_plans: list[dict[str, Any]]
    anchored_axes: list[dict[str, Any]]
    dropped_axes: list[dict[str, Any]]

    def event_payload(self) -> dict[str, Any]:
        return {
            "planner": "procedural_sweep_axis_planner_v1",
            "original_axes": self.original_axes,
            "selected_axes": self.axes,
            "anchored_axes": self.anchored_axes,
            "dropped_axes": self.dropped_axes,
            "axis_plans": self.axis_plans,
            "original_permutations": self.original_permutations,
            "selected_permutations": self.selected_permutations,
            "max_permutations": self.max_permutations,
            "search_mode": self.search_mode,
        }


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_label(value: str, *, max_len: int = 72) -> str:
    text = re.sub(r"[^\w\-]+", "_", str(value or "item").strip()) or "item"
    return text[:max_len]


def _clean_tokens(values: list[str] | tuple[str, ...] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        token = str(item).strip().upper()
        if not token or token in seen:
            continue
        cleaned.append(token)
        seen.add(token)
    return cleaned


def _normalize_role(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _seed_indicator_role(indicator: SeedIndicator) -> str:
    role = _normalize_role(indicator.signal_role)
    if role in {"trigger", "setup", "context", "filter"}:
        return role
    preferred = _normalize_role(indicator.preferred_timeframe_role)
    if preferred == "higher-context":
        return "context"
    if preferred == "mid-setup":
        return "setup"
    if preferred == "entry":
        return "trigger"
    return "state"


def _profile_indicator_role(item: dict[str, Any]) -> str:
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    role = _normalize_role(meta.get("signalRole") or meta.get("signal_role"))
    if role in {"trigger", "setup", "context", "filter"}:
        return role
    preferred = _normalize_role(
        meta.get("preferredTimeframeRole") or meta.get("preferred_timeframe_role")
    )
    if preferred == "higher-context":
        return "context"
    if preferred == "mid-setup":
        return "setup"
    if preferred == "entry":
        return "trigger"
    return "state"


def _role_timeframe_pool(role: str) -> tuple[str, ...]:
    return ROLE_TIMEFRAME_POOLS.get(role, ("M5", "M15", "M30"))


def deal_instruments(
    *,
    instrument: list[str] | None,
    instrument_pool: list[str] | None,
    rng: random.Random,
) -> dict[str, Any]:
    pinned = _clean_tokens(instrument)
    if pinned:
        return {
            "source": "pinned",
            "primary_instrument": pinned[0],
            "instruments": pinned,
            "instrument_pool": pinned,
        }

    pool = _clean_tokens(instrument_pool) or list(DEFAULT_INSTRUMENT_POOL)
    shuffled = list(pool)
    rng.shuffle(shuffled)
    primary = shuffled[0]
    return {
        "source": "dealt",
        "primary_instrument": primary,
        "instruments": [primary],
        "instrument_pool": pool,
    }


def deal_indicator_count(
    *,
    available_count: int,
    min_indicators: int,
    max_indicators: int,
    rng: random.Random,
) -> int:
    available = max(1, int(available_count))
    upper = max(1, min(int(max_indicators or 1), available))
    lower = max(1, int(min_indicators or 1))
    lower = min(lower, upper)
    if lower == upper:
        return upper
    return rng.randint(lower, upper)


def deal_role_balanced_indicators(
    indicators: list[SeedIndicator],
    *,
    target_count: int,
) -> list[SeedIndicator]:
    selected: list[SeedIndicator] = []

    def add_first(*roles: str) -> None:
        for candidate in indicators:
            if len(selected) >= target_count:
                return
            if candidate in selected:
                continue
            if _seed_indicator_role(candidate) in set(roles):
                selected.append(candidate)
                return

    if target_count <= 0:
        return []
    if target_count == 1:
        add_first("trigger")
    elif target_count == 2:
        add_first("setup", "context", "filter")
        add_first("trigger")
    else:
        add_first("context", "filter")
        add_first("setup")
        add_first("trigger")
        if target_count >= 4:
            add_first("trigger")

    for candidate in indicators:
        if len(selected) >= target_count:
            break
        if candidate not in selected:
            selected.append(candidate)
    return selected[:target_count]


def _seed_plan_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _seed_plan_upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _seed_plan_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _seed_plan_hard_negative_pair_keys(seed_plan: dict[str, Any] | None) -> set[tuple[str, str]]:
    if not isinstance(seed_plan, dict):
        return set()
    keys: set[tuple[str, str]] = set()
    for row in seed_plan.get("negative_pairs") or []:
        if not isinstance(row, dict):
            continue
        if (
            _seed_plan_float(row.get("negative_weight")) < 1.5
            or str(row.get("negative_reason") or "") != "positive_discovery_collapsed"
        ):
            continue
        first_id = _seed_plan_upper(row.get("first_indicator_id"))
        second_id = _seed_plan_upper(row.get("second_indicator_id"))
        if first_id and second_id:
            keys.add(tuple(sorted((first_id, second_id))))
    return keys


def _seed_plan_template_instrument_policy(seed_plan: dict[str, Any] | None) -> str:
    if not isinstance(seed_plan, dict):
        return "off"
    policy = seed_plan.get("sampling_policy")
    if not isinstance(policy, dict):
        return "seed_pool"
    value = str(policy.get("template_instrument_policy") or "seed_pool").strip().lower()
    return value if value in {"off", "seed_pool"} else "seed_pool"


def _seed_pair_template_instruments(pair: dict[str, Any] | None) -> list[str]:
    if not isinstance(pair, dict):
        return []
    template = _seed_plan_dict(pair.get("recommended_profile_template"))
    instruments = template.get("instruments")
    if not isinstance(instruments, (list, tuple)):
        return []
    return _clean_tokens([str(item) for item in instruments])


def _weighted_seed_plan_choice(
    items: list[Any],
    *,
    rng: random.Random,
    weight_fn: Any,
) -> Any | None:
    weighted: list[tuple[Any, float]] = []
    total = 0.0
    for item in items:
        weight = max(0.0, _seed_plan_float(weight_fn(item)))
        if weight <= 0.0:
            continue
        weighted.append((item, weight))
        total += weight
    if not weighted:
        return None
    threshold = rng.random() * total
    running = 0.0
    for item, weight in weighted:
        running += weight
        if running >= threshold:
            return item
    return weighted[-1][0]


def _seed_plan_recipe_weight(recipe_payload: dict[str, Any]) -> float:
    explicit_weight = _seed_plan_float(recipe_payload.get("recipe_sampling_weight"))
    if explicit_weight > 0.0:
        return explicit_weight
    pair_menu = recipe_payload.get("pair_menu")
    if isinstance(pair_menu, list) and pair_menu:
        return sum(
            max(
                _seed_plan_float(row.get("pair_sampling_weight")),
                _seed_plan_float(row.get("pair_sampling_score")) * 0.20,
            )
            for row in pair_menu
            if isinstance(row, dict)
        )
    slot_menus = recipe_payload.get("slot_menus")
    if not isinstance(slot_menus, dict):
        return 0.0
    total = 0.0
    for rows in slot_menus.values():
        if not isinstance(rows, list):
            continue
        total += sum(
            _seed_plan_float(row.get("sampling_weight"))
            for row in rows[:8]
            if isinstance(row, dict)
        )
    return total


def _seed_plan_guided_recipe_source_mix(policy: dict[str, Any]) -> dict[str, float]:
    raw_mix = policy.get("guided_recipe_source_mix")
    if not isinstance(raw_mix, dict):
        return {}
    mix: dict[str, float] = {}
    for source, weight in raw_mix.items():
        source_name = str(source or "").strip()
        source_weight = _seed_plan_float(weight)
        if source_name and source_weight > 0.0:
            mix[source_name] = source_weight
    return mix


def _seed_pair_family_policy(pair: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(pair, dict):
        return {}
    policy = _seed_plan_dict(pair)
    family_policy = str(policy.get("playhand_family_policy") or "").strip()
    if not family_policy:
        return {}
    return {
        "family_id": policy.get("playhand_family_id") or policy.get("probe_id"),
        "family_policy": family_policy,
        "family_policy_source": policy.get("playhand_family_policy_source"),
        "family_cap_share": policy.get("playhand_family_cap_share"),
        "recommended_max_indicators": policy.get("playhand_recommended_max_indicators"),
        "role_balanced_fill_limit": policy.get("playhand_role_balanced_fill_limit"),
        "mutation_pressure": policy.get("playhand_mutation_pressure"),
        "sampling_weight_multiplier": policy.get("playhand_sampling_weight_multiplier"),
        "exact_branch_required": policy.get("playhand_exact_branch_required"),
        "exact_rescue_rate": policy.get("playhand_exact_rescue_rate"),
        "mutated_win_rate": policy.get("playhand_mutated_win_rate"),
        "avg_mutation_delta": policy.get("playhand_avg_mutation_delta"),
        "promotion_rate": policy.get("playhand_promotion_rate"),
        "observation_count": policy.get("playhand_observation_count"),
    }


def _load_play_hand_seed_plan(config: AppConfig) -> tuple[dict[str, Any] | None, Path | None]:
    path = config.derived_root / PLAY_HAND_SEED_PLAN_PATH
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, path
    if not isinstance(payload, dict) or not isinstance(payload.get("recipes"), dict):
        return None, path
    return payload, path


def _seed_plan_slots_in_order(slot_menus: dict[str, Any]) -> list[str]:
    preferred = [
        "context",
        "setup",
        "trigger",
        "guard",
        "filter",
        "context_or_setup_cluster",
        "trigger_or_response_cluster",
    ]
    names = [name for name in preferred if name in slot_menus]
    names.extend(sorted(name for name in slot_menus if name not in set(names)))
    return names


def _fallback_indicator_deal(
    indicators: list[SeedIndicator],
    *,
    target_count: int,
    source: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "source": source,
        "reason": reason,
        "indicators": deal_role_balanced_indicators(indicators, target_count=target_count),
        "recipe": None,
        "recipe_source": None,
        "pair": None,
        "selected_slots": [],
    }


def _merge_seed_indicator_candidates(
    primary: list[SeedIndicator],
    extra: list[SeedIndicator],
) -> list[SeedIndicator]:
    merged: list[SeedIndicator] = []
    seen: set[str] = set()
    for indicator in [*primary, *extra]:
        indicator_id = str(indicator.id or "").strip().upper()
        if not indicator_id or indicator_id in seen:
            continue
        seen.add(indicator_id)
        merged.append(indicator)
    return merged


def deal_seed_plan_indicators(
    indicators: list[SeedIndicator],
    *,
    target_count: int,
    seed_plan: dict[str, Any] | None,
    rng: random.Random,
    seed_plan_candidates: list[SeedIndicator] | None = None,
) -> dict[str, Any]:
    fallback_indicators = list(indicators)
    guided_indicators = _merge_seed_indicator_candidates(
        indicators,
        seed_plan_candidates or [],
    )
    if target_count <= 0:
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="empty_target",
        )
    if not isinstance(seed_plan, dict):
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="missing_seed_plan",
        )
    by_id = {indicator.id.upper(): indicator for indicator in guided_indicators}
    if not by_id:
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="empty_seed_indicator_pool",
        )
    policy = seed_plan.get("sampling_policy") if isinstance(seed_plan.get("sampling_policy"), dict) else {}
    guided_fraction = min(1.0, max(0.0, _seed_plan_float(policy.get("guided_prior_fraction"), 0.8)))
    if rng.random() > guided_fraction:
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced_policy_exploration",
            reason="policy_exploration",
        )
    recipes = seed_plan.get("recipes")
    if not isinstance(recipes, dict):
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="missing_recipes",
        )
    recipe_items = [
        (str(name), payload)
        for name, payload in recipes.items()
        if isinstance(payload, dict) and _seed_plan_recipe_weight(payload) > 0.0
    ]
    recipe_source_mix = _seed_plan_guided_recipe_source_mix(policy)
    recipe_source_bucket: str | None = None
    recipe_source_bucket_matched = False
    recipe_source_bucket_fallback = False
    recipe_candidates = recipe_items
    if recipe_source_mix:
        selected_source = _weighted_seed_plan_choice(
            list(recipe_source_mix.items()),
            rng=rng,
            weight_fn=lambda item: item[1],
        )
        if selected_source is not None:
            recipe_source_bucket = str(selected_source[0])
            source_candidates = [
                item
                for item in recipe_items
                if str(item[1].get("source") or "").strip() == recipe_source_bucket
            ]
            if source_candidates:
                recipe_candidates = source_candidates
                recipe_source_bucket_matched = True
            else:
                recipe_source_bucket_fallback = True
    selected_recipe = _weighted_seed_plan_choice(
        recipe_candidates,
        rng=rng,
        weight_fn=lambda item: _seed_plan_recipe_weight(item[1]),
    )
    if selected_recipe is None:
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="no_weighted_recipe",
        )
    recipe_name, recipe_payload = selected_recipe
    selected_ids: list[str] = []
    selected_slots: list[dict[str, Any]] = []
    selected_pair: dict[str, Any] | None = None
    selected_family_policy: dict[str, Any] = {}
    policy_target_count = target_count
    role_balanced_fill_limit: int | None = None
    negative_pair_keys = _seed_plan_hard_negative_pair_keys(seed_plan)

    def add_indicator(
        indicator_id: Any,
        *,
        slot: str,
        evidence: dict[str, Any] | None = None,
        allow_negative_pair: bool = False,
    ) -> bool:
        clean_id = _seed_plan_upper(indicator_id)
        if not clean_id or clean_id in selected_ids or clean_id not in by_id:
            return False
        if not allow_negative_pair:
            for selected_id in selected_ids:
                if tuple(sorted((clean_id, selected_id))) in negative_pair_keys:
                    return False
        selected_ids.append(clean_id)
        selected_slots.append(
            {
                "slot": slot,
                "indicator_id": clean_id,
                "sampling_weight": max(
                    _seed_plan_float(_seed_plan_dict(evidence).get("sampling_weight")),
                    _seed_plan_float(_seed_plan_dict(evidence).get("pair_sampling_weight")),
                ),
                "sampling_lane": _seed_plan_dict(evidence).get("sampling_lane"),
                "source": _seed_plan_dict(evidence).get("source"),
            }
        )
        return True

    pair_menu = recipe_payload.get("pair_menu")
    if target_count >= 2 and isinstance(pair_menu, list) and pair_menu:
        pair = _weighted_seed_plan_choice(
            [row for row in pair_menu if isinstance(row, dict)],
            rng=rng,
            weight_fn=lambda row: max(
                _seed_plan_float(row.get("pair_sampling_weight")),
                _seed_plan_float(row.get("pair_sampling_score")) * 0.20,
            ),
        )
        if isinstance(pair, dict):
            candidate_family_policy = _seed_pair_family_policy(pair)
            recommended_max = int(
                _seed_plan_float(candidate_family_policy.get("recommended_max_indicators"))
            )
            first_id = pair.get("anchor_id")
            second_id = pair.get("trigger_id")
            added_first = add_indicator(
                first_id,
                slot="pair_first",
                evidence=pair,
                allow_negative_pair=True,
            )
            added_second = add_indicator(
                second_id,
                slot="pair_second",
                evidence=pair,
                allow_negative_pair=True,
            )
            if added_first or added_second:
                selected_family_policy = candidate_family_policy
                if recommended_max >= 2:
                    policy_target_count = min(policy_target_count, recommended_max)
                fill_limit_value = selected_family_policy.get("role_balanced_fill_limit")
                if fill_limit_value not in {None, ""}:
                    role_balanced_fill_limit = max(0, int(_seed_plan_float(fill_limit_value)))
                selected_pair = {
                    "probe_id": pair.get("probe_id"),
                    "first_indicator_id": _seed_plan_upper(first_id),
                    "second_indicator_id": _seed_plan_upper(second_id),
                    "probe_timeframe": pair.get("probe_timeframe"),
                    "pair_sampling_score": pair.get("pair_sampling_score"),
                    "composite_score": pair.get("composite_score"),
                    "retention_bucket": pair.get("retention_bucket"),
                    "recommended_profile_template": pair.get("recommended_profile_template"),
                    "source": pair.get("source"),
                    "playhand_family_policy": selected_family_policy.get("family_policy"),
                    "playhand_family_id": selected_family_policy.get("family_id"),
                    "playhand_family_policy_source": selected_family_policy.get("family_policy_source"),
                    "playhand_recommended_max_indicators": selected_family_policy.get("recommended_max_indicators"),
                    "playhand_role_balanced_fill_limit": selected_family_policy.get("role_balanced_fill_limit"),
                    "playhand_mutation_pressure": selected_family_policy.get("mutation_pressure"),
                    "playhand_sampling_weight_multiplier": selected_family_policy.get("sampling_weight_multiplier"),
                    "playhand_exact_branch_required": selected_family_policy.get("exact_branch_required"),
                    "playhand_exact_rescue_rate": selected_family_policy.get("exact_rescue_rate"),
                    "playhand_mutated_win_rate": selected_family_policy.get("mutated_win_rate"),
                    "playhand_avg_mutation_delta": selected_family_policy.get("avg_mutation_delta"),
                    "playhand_promotion_rate": selected_family_policy.get("promotion_rate"),
                    "playhand_observation_count": selected_family_policy.get("observation_count"),
                }

    slot_menus = recipe_payload.get("slot_menus")
    if isinstance(slot_menus, dict):
        for slot_name in _seed_plan_slots_in_order(slot_menus):
            if len(selected_ids) >= policy_target_count:
                break
            rows = [row for row in list(slot_menus.get(slot_name) or []) if isinstance(row, dict)]
            rows = [row for row in rows if _seed_plan_upper(row.get("indicator_id")) not in selected_ids]
            candidate = _weighted_seed_plan_choice(
                rows,
                rng=rng,
                weight_fn=lambda row: _seed_plan_float(row.get("sampling_weight")),
            )
            if isinstance(candidate, dict):
                add_indicator(candidate.get("indicator_id"), slot=slot_name, evidence=candidate)

    if len(selected_ids) < target_count:
        role_balanced_added = 0
        remaining = [
            indicator
            for indicator in guided_indicators
            if indicator.id.upper() not in selected_ids
        ]
        for indicator in deal_role_balanced_indicators(
            remaining,
            target_count=len(remaining),
        ):
            if len(selected_ids) >= policy_target_count:
                break
            if (
                role_balanced_fill_limit is not None
                and role_balanced_added >= role_balanced_fill_limit
            ):
                break
            if add_indicator(
                indicator.id,
                slot="role_balanced_fill",
                evidence={"source": "role_balanced_fill"},
                allow_negative_pair=False,
            ):
                role_balanced_added += 1

    selected = [by_id[indicator_id] for indicator_id in selected_ids if indicator_id in by_id]
    if not selected:
        return _fallback_indicator_deal(
            fallback_indicators,
            target_count=target_count,
            source="role_balanced",
            reason="seed_plan_selected_no_available_indicators",
        )
    return {
        "source": "play_hand_seed_plan",
        "reason": "guided_recipe_weighted_selection",
        "indicators": selected[:target_count],
        "recipe": recipe_name,
        "recipe_source": recipe_payload.get("source"),
        "recipe_confidence": recipe_payload.get("recipe_confidence"),
        "guided_recipe_source_mix_expected": recipe_source_mix,
        "guided_recipe_source_bucket": recipe_source_bucket,
        "guided_recipe_source_bucket_matched": recipe_source_bucket_matched,
        "guided_recipe_source_bucket_fallback": recipe_source_bucket_fallback,
        "pair": selected_pair,
        "family_policy": selected_family_policy,
        "policy_target_count": policy_target_count,
        "selected_slots": selected_slots,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _append_event(ctx: PlayHandContext, phase: str, status: str, **payload: Any) -> None:
    with ctx.io_lock:
        stage = payload.pop("stage", None)
        stage_payload = stage.event_payload() if isinstance(stage, PlayHandStage) else {}
        event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": ctx.run_id,
            "phase": phase,
            "status": status,
            **stage_payload,
            **payload,
        }
        ctx.events_path.parent.mkdir(parents=True, exist_ok=True)
        with ctx.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True) + "\n")

        score = payload.get("score")
        detail = f" score={score:.4f}" if isinstance(score, (int, float)) else ""
        prefix = f"{stage.prefix} " if isinstance(stage, PlayHandStage) else ""
        console.print(f"{prefix}[cyan]{phase}[/] [bold]{status}[/]{detail}")


def _play_hand_role_for_phase(phase: str) -> str:
    token = str(phase or "").strip().lower()
    if token == "baseline_3mo":
        return "baseline"
    if token == "exact_template_screen_3mo":
        return "exact_template_screen"
    if token == "lookback_timing_top_3mo":
        return "lookback_top"
    if token in {"coarse_top_3mo", "coarse_probe_top_3mo", "coarse_expand_top_3mo"}:
        return "coarse_top"
    if token == "focused_top_3mo":
        return "focused_top"
    if token.startswith("instrument_scout_"):
        return "instrument_scout"
    if token == "final_36mo":
        return "final"
    return token or "evaluation"


def _play_hand_default_decision(role: str) -> str:
    if role == "final":
        return "canonical"
    if role == "instrument_scout":
        return "scout_pending"
    return "intermediate"


def _play_hand_instrument_for_phase(phase: str) -> str | None:
    match = re.match(r"^instrument_scout_([^_]+)_\d+mo$", str(phase or ""), re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def _attempt_decision_updates_from_scout(
    scout_result: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not isinstance(scout_result, dict):
        return result
    for item in [scout_result.get("primary")]:
        if not isinstance(item, dict):
            continue
        attempt_id = str(item.get("attempt_id") or "").strip()
        if not attempt_id:
            continue
        result[attempt_id] = {
            "attempt_decision": "accepted",
            "attempt_decision_reasons": ["primary_anchor"],
            "play_hand_instrument": str(item.get("instrument") or "").strip().upper() or None,
        }
    for bucket, decision in (("accepted", "accepted"), ("rejected", "rejected")):
        for item in list(scout_result.get(bucket) or []):
            if not isinstance(item, dict):
                continue
            attempt_id = str(item.get("attempt_id") or "").strip()
            if not attempt_id:
                continue
            reasons = list(item.get("decision_reasons") or item.get("decision_reason") or [])
            if isinstance(item.get("decision_reason"), str):
                reasons = [str(item.get("decision_reason"))]
            result[attempt_id] = {
                "attempt_decision": decision,
                "attempt_decision_reasons": [str(reason) for reason in reasons if str(reason).strip()],
                "play_hand_instrument": str(item.get("instrument") or "").strip().upper() or None,
            }
    return result


def _finalize_play_hand_attempt_metadata(
    ctx: PlayHandContext,
    *,
    final_attempt_id: str | None,
    scout_result: dict[str, Any] | None,
    selected_instruments: list[str],
    reward_matrix: dict[str, Any] | None = None,
    final_scrutiny_passed: bool = True,
    final_scrutiny_score: float | None = None,
    tombstone_reason: str | None = None,
    tombstone_reasons: list[str] | None = None,
    run_promoted: bool | None = None,
    calendar_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    promoted = bool(final_scrutiny_passed) if run_promoted is None else bool(run_promoted)
    canonical_attempt_id = final_attempt_id if promoted else None
    tombstoned = not promoted
    tombstone_reason = str(tombstone_reason or "").strip() or None
    tombstone_reasons = [
        str(reason).strip()
        for reason in list(tombstone_reasons or ([tombstone_reason] if tombstone_reason else []))
        if str(reason).strip()
    ]
    attempts = load_attempts(ctx.attempts_path)
    if not attempts:
        return {
            "attempt_count": 0,
            "updated_count": 0,
            "canonical_attempt_id": canonical_attempt_id,
            "run_tombstoned": tombstoned,
            "tombstone_reason": tombstone_reason,
            "final_scrutiny_passed": bool(final_scrutiny_passed),
            "final_scrutiny_score": final_scrutiny_score,
            "calendar_gate": dict(calendar_gate) if isinstance(calendar_gate, dict) else None,
        }
    scout_updates = _attempt_decision_updates_from_scout(scout_result)
    selected = _clean_tokens(selected_instruments)
    matrix = dict(reward_matrix) if isinstance(reward_matrix, dict) else None
    updated = 0
    for attempt in attempts:
        candidate_name = str(attempt.get("candidate_name") or "")
        role = str(attempt.get("attempt_role") or attempt.get("play_hand_role") or "").strip()
        if not role:
            role = _play_hand_role_for_phase(candidate_name)
        decision = str(attempt.get("attempt_decision") or "").strip()
        if not decision:
            decision = _play_hand_default_decision(role)
        attempt_id = str(attempt.get("attempt_id") or "").strip()
        if attempt_id in scout_updates:
            decision = str(scout_updates[attempt_id].get("attempt_decision") or decision)
        is_final_attempt = bool(final_attempt_id and attempt_id == final_attempt_id)
        is_canonical = bool(canonical_attempt_id and attempt_id == canonical_attempt_id)
        if is_final_attempt:
            role = "final"
            decision = "canonical" if promoted else "tombstoned"
        prior = dict(attempt)
        if is_final_attempt and isinstance(calendar_gate, dict):
            attempt["calendar_gate"] = dict(calendar_gate)
        attempt["runner"] = PLAY_HAND_RUNNER
        attempt["attempt_role"] = role
        attempt["play_hand_role"] = role
        attempt["play_hand_stage"] = str(attempt.get("play_hand_stage") or role)
        attempt["attempt_decision"] = decision
        attempt["strategy_family_id"] = ctx.run_id
        attempt["canonical_attempt_id"] = canonical_attempt_id
        attempt["is_canonical_attempt"] = is_canonical
        attempt["is_canonical_playhand_attempt"] = is_canonical
        attempt["run_status"] = "tombstoned" if tombstoned else "promoted"
        attempt["final_scrutiny_passed"] = bool(final_scrutiny_passed)
        attempt["final_scrutiny_score"] = final_scrutiny_score
        if tombstoned:
            attempt["run_tombstoned"] = True
            attempt["is_tombstoned"] = True
            attempt["run_tombstone_reason"] = tombstone_reason
            attempt["run_tombstone_reasons"] = tombstone_reasons
            if is_final_attempt:
                attempt["attempt_tombstoned"] = True
                attempt["is_tombstoned_attempt"] = True
                attempt["tombstone_reason"] = tombstone_reason
                attempt["tombstone_reasons"] = tombstone_reasons
        else:
            for key in (
                "run_tombstoned",
                "is_tombstoned",
                "run_tombstone_reason",
                "run_tombstone_reasons",
                "attempt_tombstoned",
                "is_tombstoned_attempt",
                "tombstone_reason",
                "tombstone_reasons",
            ):
                attempt.pop(key, None)
        if selected:
            attempt["play_hand_selected_instruments"] = selected
        if matrix:
            attempt["max_reward_r"] = matrix.get("requested_max_reward_r")
            attempt["reward_matrix"] = matrix
            attempt["reward_step_r"] = matrix.get("reward_step_r")
            attempt["reward_columns"] = matrix.get("reward_columns")
            attempt["effective_max_reward_r"] = matrix.get("effective_max_reward_r")
        instrument = str(attempt.get("play_hand_instrument") or "").strip().upper()
        if not instrument:
            instrument = _play_hand_instrument_for_phase(candidate_name) or ""
        update = scout_updates.get(attempt_id) or {}
        if update.get("play_hand_instrument"):
            instrument = str(update.get("play_hand_instrument") or "").strip().upper()
        if instrument:
            attempt["play_hand_instrument"] = instrument
        reasons = list(attempt.get("attempt_decision_reasons") or [])
        if update.get("attempt_decision_reasons"):
            reasons = list(update.get("attempt_decision_reasons") or [])
        if is_canonical:
            reasons = ["final_scrutiny_attempt"]
        elif is_final_attempt and tombstoned:
            reasons = tombstone_reasons or [PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON]
        attempt["attempt_decision_reasons"] = [
            str(reason) for reason in reasons if str(reason).strip()
        ]
        if attempt != prior:
            updated += 1
    write_attempts(ctx.attempts_path, attempts)
    return {
        "attempt_count": len(attempts),
        "updated_count": updated,
        "canonical_attempt_id": canonical_attempt_id,
        "final_attempt_id": final_attempt_id or None,
        "run_tombstoned": tombstoned,
        "tombstone_reason": tombstone_reason,
        "tombstone_reasons": tombstone_reasons,
        "final_scrutiny_passed": bool(final_scrutiny_passed),
        "final_scrutiny_score": final_scrutiny_score,
        "calendar_gate": dict(calendar_gate) if isinstance(calendar_gate, dict) else None,
    }


def _render_dealt_hand(
    *,
    indicators: list[str],
    min_indicators: int,
    max_indicators: int,
    instrument_deal: dict[str, Any],
    timeframe: str,
    sweep_budget_label: str,
    sweep_budget_value: int,
    screen_months: int,
    scrutiny_months: int,
    coarse_mode: str,
    reward_matrix: dict[str, Any] | None = None,
) -> None:
    table = Table(title="Play-hand dealt", show_header=False, show_lines=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Instrument", f"{instrument_deal['primary_instrument']} ({instrument_deal['source']})")
    table.add_row("Timeframe", timeframe)
    table.add_row("Indicators", f"{len(indicators)} dealt from {min_indicators}-{max_indicators}: " + ", ".join(indicators))
    table.add_row("Screen", f"{screen_months}mo")
    table.add_row("Scrutiny", f"{scrutiny_months}mo")
    table.add_row("Coarse mode", coarse_mode)
    table.add_row("Sweep budget", f"{sweep_budget_label} ({sweep_budget_value})")
    if reward_matrix:
        table.add_row(
            "Reward cap",
            f"{float(reward_matrix['effective_max_reward_r']):g}R "
            f"({int(reward_matrix['reward_columns'])} x "
            f"{float(reward_matrix['reward_step_r']):g}R)",
        )
    console.print(table)


def _render_sweep_plan(
    *,
    stage: PlayHandStage | None,
    phase: str,
    axes: list[str],
    dropped_axes: list[str],
    permutation_count: int,
    original_permutations: int,
    max_permutations: int,
    mode: str,
    evaluation_budget: int | None = None,
    reward_matrix: dict[str, Any] | None = None,
) -> None:
    title = f"{stage.prefix} {phase} sweep" if stage is not None else f"{phase} sweep"
    table = Table(title=title, show_header=False, show_lines=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Mode", mode)
    if evaluation_budget is None:
        table.add_row(
            "Permutations",
            f"{permutation_count}"
            + (f" / {original_permutations} original" if original_permutations != permutation_count else ""),
        )
        table.add_row("Cap", str(max_permutations))
        table.add_row("Large sweep", "yes" if permutation_count > SWEEP_PERMUTATION_HARD_LIMIT else "no")
    else:
        table.add_row("Eval budget", str(evaluation_budget))
        table.add_row(
            "Search space",
            f"{permutation_count}"
            + (f" / {original_permutations} original" if original_permutations != permutation_count else ""),
        )
        table.add_row("Planner cap", "not applied to evolutionary search space")
        table.add_row("CLI large-sweep bypass", "yes" if permutation_count > SWEEP_PERMUTATION_HARD_LIMIT else "no")
    if reward_matrix:
        table.add_row(
            "Reward cap",
            f"{float(reward_matrix['effective_max_reward_r']):g}R",
        )
    axis_bits = []
    for axis in axes:
        key, values = _parse_axis_values(axis)
        axis_bits.append(f"{key} ({len(values)})")
    table.add_row("Axes", "; ".join(axis_bits) if axis_bits else "none")
    if dropped_axes:
        dropped_bits = []
        for axis in dropped_axes:
            key, values = _parse_axis_values(axis)
            dropped_bits.append(f"{key} ({len(values)})")
        table.add_row("Constrained", "; ".join(dropped_bits))
    console.print(table)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_profile(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload.get("profile")
    if isinstance(profile, dict):
        return profile
    return payload


def _indicator_ids_from_profile(profile_payload: dict[str, Any]) -> list[str]:
    profile = _extract_profile(profile_payload)
    ids: list[str] = []
    for item in profile.get("indicators") or []:
        if not isinstance(item, dict):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        indicator_id = str(meta.get("id") or "").strip()
        if indicator_id:
            ids.append(indicator_id)
    return ids


def _trigger_indicator_indexes(profile_payload: dict[str, Any]) -> list[int]:
    profile = _extract_profile(profile_payload)
    indexes: list[int] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        if _profile_indicator_role(item) == "trigger":
            indexes.append(index)
    return indexes


def _lookback_values_for_timeframe(timeframe: str | None) -> list[int]:
    token = str(timeframe or "").strip().upper()
    if token in {"M1", "M5", "M15"}:
        return [1, 2, 3, 4, 5]
    if token in {"M30", "H1", "H2", "H3"}:
        return [1, 2, 3]
    if token.startswith("H") or token.startswith("D") or token.startswith("W"):
        return [1, 2]
    return [1, 2, 3]


def build_lookback_axes(profile_payload: dict[str, Any]) -> list[str]:
    profile = _extract_profile(profile_payload)
    staged_axes: list[tuple[int, str]] = []
    role_priority = {"trigger": 0, "setup": 1, "context": 2, "filter": 2}
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        config = item.get("config") if isinstance(item.get("config"), dict) else {}
        if config.get("isActive") is False:
            continue
        values = _lookback_values_for_timeframe(str(config.get("timeframe") or ""))
        staged_axes.append(
            (
                role_priority.get(_profile_indicator_role(item), 3),
                f"indicator[{index}].config.lookbackBars="
                + ",".join(str(value) for value in values),
            )
        )
    return [axis for _, axis in sorted(staged_axes, key=lambda item: item[0])]


def build_timeframe_axes(profile_payload: dict[str, Any]) -> list[str]:
    profile = _extract_profile(profile_payload)
    staged_axes: list[tuple[int, str]] = []
    role_priority = {"trigger": 0, "setup": 1, "context": 2, "filter": 2}
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        config = item.get("config") if isinstance(item.get("config"), dict) else {}
        if config.get("isActive") is False:
            continue
        role = _profile_indicator_role(item)
        values = _role_timeframe_pool(role)
        current = str(config.get("timeframe") or "").strip().upper()
        if current and current not in values:
            values = (*values, current)
        if len(values) < 2:
            continue
        staged_axes.append(
            (
                role_priority.get(role, 3),
                f"indicator[{index}].config.timeframe=" + ",".join(values),
            )
        )
    return [axis for _, axis in sorted(staged_axes, key=lambda item: item[0])]


def build_timing_axes(profile_payload: dict[str, Any]) -> list[str]:
    return [*build_timeframe_axes(profile_payload), *build_lookback_axes(profile_payload)]


def _talib_config(item: dict[str, Any]) -> list[dict[str, Any]]:
    config = item.get("config") if isinstance(item.get("config"), dict) else {}
    values = config.get("talibConfig")
    return values if isinstance(values, list) else []


def _set_talib_value(item: dict[str, Any], name: str, value: Any) -> None:
    config = item.setdefault("config", {})
    if not isinstance(config, dict):
        return
    talib = config.setdefault("talibConfig", [])
    if not isinstance(talib, list):
        config["talibConfig"] = [{"name": name, "value": value}]
        return
    for parameter in talib:
        if isinstance(parameter, dict) and parameter.get("name") == name:
            parameter["value"] = value
            return
    talib.append({"name": name, "value": value})


def _get_talib_value(item: dict[str, Any], name: str) -> Any:
    for parameter in _talib_config(item):
        if isinstance(parameter, dict) and parameter.get("name") == name:
            return parameter.get("value")
    return None


def apply_play_hand_profile_defaults(
    profile_payload: dict[str, Any],
    *,
    rng: random.Random,
) -> list[dict[str, Any]]:
    profile = _extract_profile(profile_payload)
    changes: list[dict[str, Any]] = []
    bundle_names = sorted(CANDLESTICK_PATTERN_BUNDLES)
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        indicator_id = str(meta.get("id") or "").strip().upper()
        if indicator_id != "CANDLESTICK_PATTERNS":
            continue
        patterns = _get_talib_value(item, "patterns")
        if isinstance(patterns, list) and patterns:
            continue
        bundle_name = rng.choice(bundle_names)
        bundle = list(CANDLESTICK_PATTERN_BUNDLES[bundle_name])
        _set_talib_value(item, "patterns", bundle)
        if not _get_talib_value(item, "aggregation"):
            _set_talib_value(item, "aggregation", "any")
        changes.append(
            {
                "indicator_index": index,
                "indicator_id": indicator_id,
                "default": "candlestick_pattern_bundle",
                "bundle": bundle_name,
                "patterns": bundle,
            }
        )
    return changes


def apply_seed_indicator_metadata(
    profile_payload: dict[str, Any],
    seed_indicators: list[SeedIndicator],
) -> list[dict[str, Any]]:
    profile = _extract_profile(profile_payload)
    by_id = {indicator.id.upper(): indicator for indicator in seed_indicators}
    changes: list[dict[str, Any]] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        meta = item.setdefault("meta", {})
        if not isinstance(meta, dict):
            item["meta"] = {}
            meta = item["meta"]
        indicator_id = str(meta.get("id") or "").strip().upper()
        seed_indicator = by_id.get(indicator_id)
        if seed_indicator is None:
            continue
        updates = {
            "signalRole": seed_indicator.signal_role,
            "signalPersistence": seed_indicator.signal_persistence,
            "preferredTimeframeRole": seed_indicator.preferred_timeframe_role,
        }
        applied: dict[str, Any] = {}
        for key, value in updates.items():
            if value and not meta.get(key):
                meta[key] = value
                applied[key] = value
        if applied:
            changes.append(
                {
                    "indicator_index": index,
                    "indicator_id": indicator_id,
                    "metadata": applied,
                }
            )
    return changes


def apply_role_timeframe_defaults(
    profile_payload: dict[str, Any],
    *,
    rng: random.Random,
) -> list[dict[str, Any]]:
    profile = _extract_profile(profile_payload)
    changes: list[dict[str, Any]] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        config = item.setdefault("config", {})
        if not isinstance(config, dict):
            item["config"] = {}
            config = item["config"]
        if config.get("isActive") is False:
            continue
        role = _profile_indicator_role(item)
        pool = _role_timeframe_pool(role)
        timeframe = rng.choice(pool)
        previous = str(config.get("timeframe") or "").strip().upper() or None
        config["timeframe"] = timeframe
        changes.append(
            {
                "indicator_index": index,
                "indicator_id": str(
                    (item.get("meta") if isinstance(item.get("meta"), dict) else {}).get("id")
                    or ""
                ).strip(),
                "role": role,
                "previous_timeframe": previous,
                "timeframe": timeframe,
                "pool": list(pool),
            }
        )
    return changes


def apply_seed_pair_template_defaults(
    profile_payload: dict[str, Any],
    pair_evidence: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    template = _seed_plan_dict(_seed_plan_dict(pair_evidence).get("recommended_profile_template"))
    defaults = [
        row
        for row in template.get("indicator_defaults") or []
        if isinstance(row, dict) and _seed_plan_upper(row.get("indicator_id"))
    ]
    if not defaults:
        return []
    by_id = {_seed_plan_upper(row.get("indicator_id")): row for row in defaults}
    profile = _extract_profile(profile_payload)
    changes: list[dict[str, Any]] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        indicator_id = _seed_plan_upper(meta.get("id"))
        default_row = by_id.get(indicator_id)
        if not default_row:
            continue
        config = item.setdefault("config", {})
        if not isinstance(config, dict):
            item["config"] = {}
            config = item["config"]
        applied: dict[str, Any] = {}
        for key in SEED_TEMPLATE_CONFIG_KEYS:
            if key not in default_row:
                continue
            previous = copy.deepcopy(config.get(key))
            value = copy.deepcopy(default_row.get(key))
            if previous == value:
                continue
            config[key] = value
            applied[key] = {
                "previous": previous,
                "value": value,
            }
        if applied:
            changes.append(
                {
                    "indicator_index": index,
                    "indicator_id": indicator_id,
                    "template_probe_id": template.get("probe_id"),
                    "template_timeframe": template.get("timeframe"),
                    "config": applied,
                }
            )
    return changes


def _profile_timeframes(profile_payload: dict[str, Any]) -> list[str]:
    profile = _extract_profile(profile_payload)
    timeframes: list[str] = []
    for item in profile.get("indicators") or []:
        if not isinstance(item, dict):
            continue
        config = item.get("config") if isinstance(item.get("config"), dict) else {}
        if config.get("isActive") is False:
            continue
        timeframe = str(config.get("timeframe") or "").strip().upper()
        if timeframe and timeframe not in timeframes:
            timeframes.append(timeframe)
    return timeframes


def _lowest_profile_timeframe(profile_payload: dict[str, Any], fallback: str) -> str:
    candidates = [
        timeframe
        for timeframe in _profile_timeframes(profile_payload)
        if timeframe in TIMEFRAME_MINUTES
    ]
    if not candidates:
        return str(fallback or "M5").strip().upper() or "M5"
    return min(candidates, key=lambda timeframe: TIMEFRAME_MINUTES[timeframe])


def _numeric_axis_values(value: float) -> list[Any]:
    if value <= 0:
        return [value]
    if float(value).is_integer():
        base = int(value)
        candidates = {
            max(2, int(round(base * 0.6))),
            max(2, int(round(base * 0.8))),
            base,
            max(2, int(round(base * 1.25))),
            max(2, int(round(base * 1.6))),
        }
        return sorted(candidates)
    candidates = {
        round(value * 0.5, 6),
        round(value * 0.75, 6),
        round(value, 6),
        round(value * 1.25, 6),
        round(value * 1.5, 6),
    }
    return sorted(candidate for candidate in candidates if candidate > 0)


def build_coarse_axes(profile_payload: dict[str, Any], *, max_axes: int | None = None) -> list[str]:
    profile = _extract_profile(profile_payload)
    axes: list[str] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        for talib_item in _talib_config(item):
            if not isinstance(talib_item, dict):
                continue
            name = str(talib_item.get("name") or "").strip()
            value = talib_item.get("value")
            if (
                not name
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
                or "matype" in name.lower()
            ):
                continue
            values = _numeric_axis_values(float(value))
            if len(values) >= 2:
                axes.append(
                    f"indicator[{index}].talib.{name}="
                    + ",".join(_format_axis_value(candidate) for candidate in values)
                )
            if max_axes is not None and len(axes) >= max_axes:
                return axes
    return axes


def build_required_lookback_axes(profile_payload: dict[str, Any]) -> list[str]:
    return build_lookback_axes(profile_payload)


def _format_axis_value(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _parse_axis_values(axis: str) -> tuple[str, list[Any]]:
    left, _, right = axis.partition("=")
    values: list[Any] = []
    for raw in right.split(","):
        text = raw.strip()
        if not text:
            continue
        try:
            numeric = float(text)
            values.append(int(numeric) if numeric.is_integer() else numeric)
        except ValueError:
            values.append(text)
    return left.strip(), values


def _axis_cardinality(axis: str) -> int:
    _key, values = _parse_axis_values(axis)
    return max(1, len(values))


def _permutation_count(axes: list[str]) -> int:
    total = 1
    for axis in axes:
        total *= _axis_cardinality(axis)
    return total


def _axis_index(axis_key: str) -> int | None:
    match = re.match(r"indicator\[(\d+)\]\.", axis_key)
    if not match:
        return None
    return int(match.group(1))


def _axis_indicator_item(profile_payload: dict[str, Any], axis_key: str) -> dict[str, Any] | None:
    index = _axis_index(axis_key)
    if index is None:
        return None
    indicators = _extract_profile(profile_payload).get("indicators")
    if not isinstance(indicators, list) or index >= len(indicators):
        return None
    item = indicators[index]
    return item if isinstance(item, dict) else None


def _axis_current_value(profile_payload: dict[str, Any] | None, axis_key: str) -> Any:
    if not isinstance(profile_payload, dict):
        return None
    item = _axis_indicator_item(profile_payload, axis_key)
    if not isinstance(item, dict):
        return None
    match = re.fullmatch(r"indicator\[(\d+)\]\.(config|talib)\.([A-Za-z0-9_]+)", axis_key)
    if not match:
        return None
    section = match.group(2)
    key = match.group(3)
    config = item.get("config") if isinstance(item.get("config"), dict) else {}
    if section == "config":
        return config.get(key)
    for parameter in _talib_config(item):
        if isinstance(parameter, dict) and parameter.get("name") == key:
            return parameter.get("value")
    return None


def _axis_role(profile_payload: dict[str, Any] | None, axis_key: str) -> str:
    if not isinstance(profile_payload, dict):
        return "state"
    item = _axis_indicator_item(profile_payload, axis_key)
    return _profile_indicator_role(item) if isinstance(item, dict) else "state"


def _axis_kind(axis_key: str) -> str:
    normalized = axis_key.lower()
    if ".config.timeframe" in normalized:
        return "timeframe"
    if ".config.lookbackbars" in normalized:
        return "lookback"
    if ".talib." not in normalized:
        return "other"
    name = normalized.rsplit(".", 1)[-1]
    if "signal" in name or "smooth" in name or name in {"slowk_period", "slowd_period"}:
        return "smoothing"
    if any(token in name for token in ("threshold", "multiplier", "deviation", "factor", "delta", "fraction")):
        return "threshold_scale"
    if name in {"levels", "acceleration", "maximum", "minimum"}:
        return "threshold_scale"
    if any(token in name for token in ("period", "window", "length", "fast", "slow", "roc", "sma", "ema", "normalize", "hp_", "lp_")):
        return "period"
    return "numeric"


def _axis_priority(axis_key: str, *, phase: str, role: str, kind: str) -> float:
    phase_token = str(phase or "").lower()
    if "focused" in phase_token:
        base = 100.0
    elif kind == "timeframe":
        base = 96.0
    elif kind == "threshold_scale":
        base = 86.0
    elif kind == "period":
        base = 82.0
    elif kind == "lookback":
        base = 74.0
    elif kind == "smoothing":
        base = 62.0
    elif kind == "numeric":
        base = 58.0
    else:
        base = 45.0

    role_bonus = {
        "trigger": 8.0,
        "setup": 5.0,
        "context": 2.0,
        "filter": 1.0,
    }.get(role, 0.0)
    name = axis_key.lower().rsplit(".", 1)[-1]
    if any(token in name for token in ("fast", "slow")) and kind == "period":
        role_bonus += 3.0
    if "signal" in name:
        role_bonus -= 2.0
    return base + role_bonus


def _values_numeric(values: list[Any]) -> bool:
    return bool(values) and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values)


def _closest_value(values: list[Any], target: Any) -> Any | None:
    if target is None or not values:
        return None
    if target in values:
        return target
    target_float = _as_float(target)
    if target_float is None:
        target_text = str(target).strip().upper()
        for value in values:
            if str(value).strip().upper() == target_text:
                return value
        return None
    numeric_values = [value for value in values if _as_float(value) is not None]
    if not numeric_values:
        return None
    return min(numeric_values, key=lambda value: abs(float(value) - target_float))


def _select_representative_values(
    values: list[Any],
    count: int,
    *,
    current_value: Any = None,
) -> list[Any]:
    if count <= 0:
        return []
    unique_values: list[Any] = []
    for value in values:
        if value not in unique_values:
            unique_values.append(value)
    if len(unique_values) <= count:
        return unique_values

    selected: list[Any] = []
    current = _closest_value(unique_values, current_value)
    if current is not None:
        selected.append(current)

    def add(value: Any) -> None:
        if value not in selected and len(selected) < count:
            selected.append(value)

    if _values_numeric(unique_values):
        ordered = sorted(unique_values, key=lambda value: float(value))
    else:
        ordered = unique_values

    if count >= 3:
        add(ordered[0])
        add(ordered[-1])
    elif count == 2:
        if current is None:
            add(ordered[0])
            add(ordered[-1])
        else:
            current_index = ordered.index(current)
            lower_distance = current_index
            upper_distance = len(ordered) - 1 - current_index
            if lower_distance == upper_distance:
                choose_lower = sum(ord(char) for char in str(current_value)) % 2 == 0
                add(ordered[0] if choose_lower else ordered[-1])
            else:
                add(ordered[0] if lower_distance > upper_distance else ordered[-1])

    while len(selected) < count:
        if not selected:
            add(ordered[len(ordered) // 2])
            continue
        best_value = None
        best_distance = -1
        selected_indexes = [ordered.index(value) for value in selected if value in ordered]
        for index, value in enumerate(ordered):
            if value in selected:
                continue
            distance = min(abs(index - selected_index) for selected_index in selected_indexes)
            if distance > best_distance:
                best_value = value
                best_distance = distance
        if best_value is None:
            break
        add(best_value)

    order_lookup = {value: index for index, value in enumerate(unique_values)}
    return sorted(selected, key=lambda value: order_lookup.get(value, 0))


def _format_axis(axis_key: str, values: list[Any]) -> str:
    return axis_key + "=" + ",".join(_format_axis_value(value) for value in values)


def _evolutionary_axis_values(
    values: list[Any],
    *,
    current_value: Any = None,
    max_count: int = EVOLUTIONARY_AXIS_MAX_VALUE_COUNT,
) -> list[Any]:
    unique_values: list[Any] = []
    for value in values:
        if value not in unique_values:
            unique_values.append(value)
    if len(unique_values) < 2 or not _values_numeric(unique_values):
        return unique_values

    ordered = sorted(unique_values, key=lambda value: float(value))
    expanded: list[Any] = list(ordered)
    all_integral = all(float(value).is_integer() for value in ordered)
    for left, right in zip(ordered, ordered[1:]):
        midpoint = (float(left) + float(right)) / 2.0
        candidate: Any = int(midpoint + 0.5) if all_integral else round(midpoint, 6)
        if candidate > 0 and candidate not in expanded:
            expanded.append(candidate)
    expanded = sorted(expanded, key=lambda value: float(value))
    if len(expanded) > max_count:
        expanded = _select_representative_values(
            expanded,
            max_count,
            current_value=current_value,
        )
    return expanded


def _build_axis_candidates(
    axes: list[str],
    *,
    profile_payload: dict[str, Any] | None,
    phase: str,
) -> list[SweepAxisCandidate]:
    candidates: list[SweepAxisCandidate] = []
    for axis in axes:
        key, values = _parse_axis_values(axis)
        unique_values: list[Any] = []
        for value in values:
            if value not in unique_values:
                unique_values.append(value)
        if not key or not unique_values:
            continue
        current_value = _axis_current_value(profile_payload, key)
        kind = _axis_kind(key)
        role = _axis_role(profile_payload, key)
        max_count = len(unique_values)
        min_count = 2 if max_count >= 2 else 1
        candidates.append(
            SweepAxisCandidate(
                axis=axis,
                key=key,
                values=unique_values,
                current_value=current_value,
                kind=kind,
                role=role,
                priority=_axis_priority(key, phase=phase, role=role, kind=kind),
                min_count=min_count,
                max_count=max_count,
            )
        )
    return candidates


def plan_sweep_axes(
    axes: list[str],
    *,
    profile_payload: dict[str, Any] | None = None,
    phase: str = "sweep",
    max_permutations: int = SWEEP_PERMUTATION_HARD_LIMIT,
    search_mode: str = "deterministic",
) -> SweepAxisPlan:
    original_axes = list(axes)
    original_permutations = _permutation_count(original_axes)
    max_permutations = max(1, int(max_permutations or 1))
    normalized_search_mode = str(search_mode or "deterministic").strip().lower()
    if normalized_search_mode not in {"deterministic", "evolutionary"}:
        normalized_search_mode = "deterministic"
    candidates = _build_axis_candidates(
        original_axes,
        profile_payload=profile_payload,
        phase=phase,
    )
    if normalized_search_mode == "evolutionary":
        selected_axes: list[str] = []
        axis_plans: list[dict[str, Any]] = []
        dropped: list[dict[str, Any]] = []
        for candidate in candidates:
            sampled_values = _evolutionary_axis_values(
                candidate.values,
                current_value=candidate.current_value,
            )
            if len(sampled_values) >= 2:
                selected_axes.append(_format_axis(candidate.key, sampled_values))
                status = "selected"
                selected_values = sampled_values
                selected_value_count = len(sampled_values)
            else:
                status = "dropped"
                selected_values = sampled_values[:1]
                selected_value_count = 1
                dropped.append(
                    {
                        "axis": candidate.axis,
                        "key": candidate.key,
                        "reason": "not_enough_values",
                        "kind": candidate.kind,
                        "role": candidate.role,
                        "priority": round(candidate.priority, 3),
                    }
                )
            axis_plans.append(
                {
                    "axis": candidate.axis,
                    "key": candidate.key,
                    "kind": candidate.kind,
                    "role": candidate.role,
                    "priority": round(candidate.priority, 3),
                    "status": status,
                    "original_value_count": len(candidate.values),
                    "selected_value_count": selected_value_count,
                    "selected_values": selected_values,
                    "current_value": candidate.current_value,
                }
            )
        return SweepAxisPlan(
            axes=selected_axes,
            original_axes=original_axes,
            original_permutations=original_permutations,
            selected_permutations=_permutation_count(selected_axes),
            max_permutations=max_permutations,
            search_mode=normalized_search_mode,
            axis_plans=axis_plans,
            anchored_axes=[],
            dropped_axes=dropped,
        )

    counts = {candidate.key: candidate.min_count for candidate in candidates}
    active = {candidate.key for candidate in candidates if counts.get(candidate.key, 0) >= 2}
    anchored: dict[str, dict[str, Any]] = {}
    dropped: list[dict[str, Any]] = []

    def active_product() -> int:
        product = 1
        for candidate in candidates:
            if candidate.key in active:
                product *= max(1, int(counts.get(candidate.key, 1)))
        return product

    while active and active_product() > max_permutations:
        active_counts_by_indicator: dict[int, int] = {}
        for candidate in candidates:
            if candidate.key not in active:
                continue
            index = _axis_index(candidate.key)
            if index is not None:
                active_counts_by_indicator[index] = active_counts_by_indicator.get(index, 0) + 1
        eligible = []
        for candidate in candidates:
            if candidate.key not in active:
                continue
            index = _axis_index(candidate.key)
            if index is None or active_counts_by_indicator.get(index, 0) > 1:
                eligible.append(candidate)
        if not eligible:
            eligible = [candidate for candidate in candidates if candidate.key in active]
        lowest = min(
            eligible,
            key=lambda candidate: (candidate.priority, candidate.max_count, -original_axes.index(candidate.axis)),
        )
        active.remove(lowest.key)
        counts[lowest.key] = 1
        anchored[lowest.key] = {
            "axis": lowest.axis,
            "key": lowest.key,
            "reason": "anchored_to_fit_budget",
            "kind": lowest.kind,
            "role": lowest.role,
            "priority": round(lowest.priority, 3),
            "current_value": lowest.current_value,
        }

    improved = True
    while improved:
        improved = False
        current_product = active_product()
        upgrade_candidates: list[tuple[float, SweepAxisCandidate]] = []
        for candidate in candidates:
            if candidate.key not in active:
                continue
            current_count = int(counts.get(candidate.key, 1))
            if current_count >= candidate.max_count:
                continue
            next_product = current_product // max(1, current_count) * (current_count + 1)
            if next_product <= max_permutations:
                # Prioritize important axes, but give under-sampled broad axes a small boost.
                remaining_room = candidate.max_count - current_count
                score = candidate.priority + min(6.0, remaining_room * 1.5)
                upgrade_candidates.append((score, candidate))
        if not upgrade_candidates:
            break
        _score, best = max(
            upgrade_candidates,
            key=lambda item: (item[0], -original_axes.index(item[1].axis)),
        )
        counts[best.key] = int(counts.get(best.key, 1)) + 1
        improved = True

    selected_axes: list[str] = []
    axis_plans: list[dict[str, Any]] = []
    for candidate in candidates:
        count = int(counts.get(candidate.key, 1))
        sampled_values = _select_representative_values(
            candidate.values,
            count,
            current_value=candidate.current_value,
        )
        if candidate.key in active and len(sampled_values) >= 2:
            axis_text = _format_axis(candidate.key, sampled_values)
            selected_axes.append(axis_text)
            status = "selected"
        elif len(candidate.values) >= 2:
            status = "anchored"
            anchored.setdefault(
                candidate.key,
                {
                    "axis": candidate.axis,
                    "key": candidate.key,
                    "reason": "anchored_to_fit_budget",
                    "kind": candidate.kind,
                    "role": candidate.role,
                    "priority": round(candidate.priority, 3),
                    "current_value": candidate.current_value,
                },
            )
        else:
            status = "dropped"
            dropped.append(
                {
                    "axis": candidate.axis,
                    "key": candidate.key,
                    "reason": "not_enough_values",
                    "kind": candidate.kind,
                    "role": candidate.role,
                    "priority": round(candidate.priority, 3),
                }
            )
        axis_plans.append(
            {
                "axis": candidate.axis,
                "key": candidate.key,
                "kind": candidate.kind,
                "role": candidate.role,
                "priority": round(candidate.priority, 3),
                "status": status,
                "original_value_count": len(candidate.values),
                "selected_value_count": len(sampled_values) if status == "selected" else 1,
                "selected_values": sampled_values if status == "selected" else sampled_values[:1],
                "current_value": candidate.current_value,
            }
        )

    selected_permutations = _permutation_count(selected_axes)
    return SweepAxisPlan(
        axes=selected_axes,
        original_axes=original_axes,
        original_permutations=original_permutations,
        selected_permutations=selected_permutations,
        max_permutations=max_permutations,
        search_mode=normalized_search_mode,
        axis_plans=axis_plans,
        anchored_axes=list(anchored.values()),
        dropped_axes=dropped,
    )


def fit_axes_to_permutation_budget(
    axes: list[str],
    *,
    max_permutations: int = SWEEP_PERMUTATION_HARD_LIMIT,
) -> tuple[list[str], list[str], int]:
    plan = plan_sweep_axes(axes, max_permutations=max_permutations)
    constrained = [
        str(item.get("axis"))
        for item in [*plan.anchored_axes, *plan.dropped_axes]
        if item.get("axis")
    ]
    return plan.axes, constrained, plan.original_permutations


def _refine_values(values: list[Any], best_value: Any) -> list[Any]:
    if not isinstance(best_value, (int, float)) or isinstance(best_value, bool):
        return [best_value] if best_value not in (None, "") else values[:1]
    numeric_values = sorted(
        {
            float(value)
            for value in values
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    if not numeric_values:
        return []
    lower_bound = numeric_values[0]
    upper_bound = numeric_values[-1]
    best = min(max(float(best_value), lower_bound), upper_bound)
    diffs = [
        abs(right - left)
        for left, right in zip(numeric_values, numeric_values[1:])
        if abs(right - left) > 0
    ]
    step = min(diffs) / 2.0 if diffs else max(abs(best) * 0.1, 1.0)
    all_integral = float(best_value).is_integer() and all(float(v).is_integer() for v in numeric_values)
    target_count = 5
    offsets = [-2, -1, 0, 1, 2]

    def in_bounds(value: float) -> bool:
        return lower_bound <= value <= upper_bound

    if all_integral:
        int_step = max(1, int(round(step)))
        best_int = int(round(best))
        candidates: set[int] = {
            int(best_int + offset * int_step)
            for offset in offsets
            if in_bounds(best_int + offset * int_step)
        }
        distance = 3
        while len(candidates) < target_count:
            added = False
            for side in (-1, 1):
                candidate = int(best_int + side * distance * int_step)
                if in_bounds(candidate) and candidate not in candidates:
                    candidates.add(candidate)
                    added = True
                    if len(candidates) >= target_count:
                        break
            if not added:
                break
            distance += 1
        return sorted(candidates)

    candidates_float: set[float] = {
        round(best + offset * step, 6)
        for offset in offsets
        if in_bounds(best + offset * step)
    }
    distance = 3
    while len(candidates_float) < target_count:
        added = False
        for side in (-1, 1):
            candidate = round(best + side * distance * step, 6)
            if in_bounds(candidate) and candidate not in candidates_float:
                candidates_float.add(candidate)
                added = True
                if len(candidates_float) >= target_count:
                    break
        if not added:
            break
        distance += 1
    return sorted(candidates_float)


def build_focused_axes(
    parameter_importance: list[dict[str, Any]],
    prior_axes: list[str],
    *,
    max_axes: int = 2,
    min_importance_pct: float = 5.0,
) -> list[str]:
    prior_by_key = dict(_parse_axis_values(axis) for axis in prior_axes)
    focused: list[str] = []
    ranked = sorted(
        [
            item
            for item in parameter_importance
            if isinstance(item, dict)
            and isinstance(item.get("importance_pct"), (int, float))
            and float(item.get("importance_pct") or 0.0) >= min_importance_pct
        ],
        key=lambda item: float(item.get("importance_pct") or 0.0),
        reverse=True,
    )
    for item in ranked:
        axis = str(item.get("axis") or "").strip()
        if not axis or axis not in prior_by_key:
            continue
        best_value = item.get("best_value")
        values = _refine_values(prior_by_key[axis], best_value)
        if len(values) < 2:
            continue
        focused.append(axis + "=" + ",".join(_format_axis_value(value) for value in values))
        if len(focused) >= max_axes:
            break
    return focused


def _apply_axis_value(profile_payload: dict[str, Any], axis: str, value: Any) -> None:
    profile = _extract_profile(profile_payload)
    match = re.fullmatch(r"indicator\[(\d+)\]\.(config|talib)\.([A-Za-z0-9_]+)", axis)
    if not match:
        return
    index = int(match.group(1))
    section = match.group(2)
    key = match.group(3)
    indicators = profile.get("indicators")
    if not isinstance(indicators, list) or index >= len(indicators):
        return
    indicator = indicators[index]
    if not isinstance(indicator, dict):
        return
    config = indicator.setdefault("config", {})
    if not isinstance(config, dict):
        return
    if section == "config":
        config[key] = value
        return
    talib = config.setdefault("talibConfig", [])
    if not isinstance(talib, list):
        return
    for item in talib:
        if isinstance(item, dict) and item.get("name") == key:
            item["value"] = value
            return
    talib.append({"name": key, "value": value})


def materialize_profile_variant(
    source_profile_path: Path,
    output_profile_path: Path,
    parameters: dict[str, Any],
    *,
    name_suffix: str,
) -> Path:
    payload = _load_json(source_profile_path)
    profile = _extract_profile(payload)
    for axis, value in parameters.items():
        _apply_axis_value(payload, str(axis), value)
    if isinstance(profile, dict):
        original_name = str(profile.get("name") or source_profile_path.stem)
        profile["name"] = f"{original_name} {name_suffix}".strip()
        profile["isActive"] = False
    _write_json(output_profile_path, payload)
    return output_profile_path


def _metadata_value(item: dict[str, Any], snake_key: str, camel_key: str) -> str | None:
    value = item.get(snake_key) or item.get(camel_key)
    text = str(value or "").strip()
    return text or None


def _seed_indicator_from_metadata(indicator_id: str, metadata: dict[str, Any] | None) -> SeedIndicator:
    metadata = metadata or {}
    return SeedIndicator(
        id=indicator_id,
        signal_role=_metadata_value(metadata, "signal_role", "signalRole"),
        signal_persistence=_metadata_value(
            metadata,
            "signal_persistence",
            "signalPersistence",
        ),
        preferred_timeframe_role=_metadata_value(
            metadata,
            "preferred_timeframe_role",
            "preferredTimeframeRole",
        ),
    )


def _seed_hand(config: AppConfig, cli: FuzzfolioCli, run_dir: Path) -> list[SeedIndicator]:
    seed_path = run_dir / "seed-prompt.json"
    result = cli.seed_prompt(seed_path)
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else _load_json(seed_path)
    indicators = payload.get("indicators") if isinstance(payload, dict) else None
    metadata_items = payload.get("indicator_metadata") if isinstance(payload, dict) else None
    metadata_by_id: dict[str, dict[str, Any]] = {}
    if isinstance(metadata_items, list):
        for item in metadata_items:
            if not isinstance(item, dict):
                continue
            indicator_id = str(item.get("id") or "").strip()
            if indicator_id:
                metadata_by_id[indicator_id.upper()] = item

    hand: list[SeedIndicator] = []
    for item in indicators or []:
        if isinstance(item, dict):
            indicator_id = str(item.get("id") or "").strip()
            metadata_by_id.setdefault(indicator_id.upper(), item)
        else:
            indicator_id = str(item).strip()
        if not indicator_id:
            continue
        hand.append(
            _seed_indicator_from_metadata(
                indicator_id,
                metadata_by_id.get(indicator_id.upper()),
            )
        )
    return hand


def _append_seed_plan_indicator_id(ids: list[str], value: Any) -> None:
    indicator_id = str(value or "").strip().upper()
    if indicator_id and indicator_id not in ids:
        ids.append(indicator_id)


def _seed_plan_indicator_ids(seed_plan: dict[str, Any] | None) -> list[str]:
    if not isinstance(seed_plan, dict):
        return []
    ids: list[str] = []
    recipes = seed_plan.get("recipes")
    if not isinstance(recipes, dict):
        return ids
    for recipe_payload in recipes.values():
        if not isinstance(recipe_payload, dict):
            continue
        for pair in recipe_payload.get("pair_menu") or []:
            if not isinstance(pair, dict):
                continue
            for key in (
                "anchor_id",
                "trigger_id",
                "first_indicator_id",
                "second_indicator_id",
            ):
                _append_seed_plan_indicator_id(ids, pair.get(key))
            template = _seed_plan_dict(pair.get("recommended_profile_template"))
            for default in template.get("indicator_defaults") or []:
                if isinstance(default, dict):
                    _append_seed_plan_indicator_id(ids, default.get("indicator_id"))
        slot_menus = recipe_payload.get("slot_menus")
        if isinstance(slot_menus, dict):
            for rows in slot_menus.values():
                for row in rows or []:
                    if isinstance(row, dict):
                        _append_seed_plan_indicator_id(ids, row.get("indicator_id"))
    return ids


def _seed_plan_indicator_metadata(config: AppConfig) -> dict[str, dict[str, Any]]:
    atlas_path = config.derived_root / "indicator-atlas" / "indicator-atlas.json"
    payload = _load_json(atlas_path)
    rows = payload.get("indicators")
    metadata: dict[str, dict[str, Any]] = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            indicator_id = str(row.get("id") or "").strip().upper()
            if not indicator_id:
                continue
            metadata[indicator_id] = row
    return metadata


def _seed_plan_indicator_candidates(
    config: AppConfig,
    seed_plan: dict[str, Any] | None,
) -> list[SeedIndicator]:
    ids = _seed_plan_indicator_ids(seed_plan)
    if not ids:
        return []
    metadata_by_id = _seed_plan_indicator_metadata(config)
    return [
        _seed_indicator_from_metadata(indicator_id, metadata_by_id.get(indicator_id))
        for indicator_id in ids
    ]


def _register_profile(ctx: PlayHandContext, profile_path: Path) -> str:
    if ctx.dry_run:
        return f"dry-{profile_path.stem}"
    result = ctx.cli.run(["profiles", "create", "--file", str(profile_path), "--pretty"])
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    profile_ref = str(data.get("id") or "").strip()
    if not profile_ref:
        raise RuntimeError(f"profiles create did not return an id for {profile_path}")
    ctx.registered_profile_refs.append(profile_ref)
    return profile_ref


def _cleanup_registered_profiles(
    ctx: PlayHandContext,
    *,
    keep_cloud_profiles: bool,
    reason: str,
    stage: PlayHandStage | None = None,
) -> dict[str, Any]:
    profile_refs = list(
        dict.fromkeys(
            profile_ref
            for profile_ref in ctx.registered_profile_refs
            if str(profile_ref or "").strip()
        )
    )
    summary: dict[str, Any] = {
        "status": "skipped",
        "reason": reason,
        "keep_cloud_profiles": bool(keep_cloud_profiles),
        "attempted_count": 0,
        "deleted_count": 0,
        "failed_count": 0,
    }
    if not profile_refs:
        summary["skip_reason"] = "no_registered_profiles"
        return summary
    if keep_cloud_profiles:
        summary["skip_reason"] = "keep_cloud_profiles"
        summary["attempted_count"] = len(profile_refs)
        _append_event(
            ctx,
            "cloud_profile_cleanup",
            "skipped",
            stage=stage,
            reason=summary["skip_reason"],
            profile_refs=profile_refs,
        )
        return summary

    deleted: list[str] = []
    failures: list[dict[str, Any]] = []
    for profile_ref in profile_refs:
        try:
            result = ctx.cli.run(
                ["profiles", "delete", "--profile-ref", profile_ref, "--pretty"],
                check=False,
            )
            if result.returncode == 0:
                deleted.append(profile_ref)
            else:
                failures.append(
                    {
                        "profile_ref": profile_ref,
                        "returncode": result.returncode,
                        "stderr": result.stderr.strip()[:800],
                        "stdout": result.stdout.strip()[:800],
                    }
                )
        except Exception as exc:
            failures.append({"profile_ref": profile_ref, "error": str(exc)[:800]})

    summary.update(
        {
            "status": "completed" if not failures else "partial",
            "attempted_count": len(profile_refs),
            "deleted_count": len(deleted),
            "failed_count": len(failures),
            "deleted_profile_refs": deleted,
        }
    )
    if failures:
        summary["failures"] = failures
    _append_event(
        ctx,
        "cloud_profile_cleanup",
        summary["status"],
        stage=stage,
        reason=reason,
        attempted_count=summary["attempted_count"],
        deleted_count=summary["deleted_count"],
        failed_count=summary["failed_count"],
        failures=failures[:5],
    )
    return summary


def _scaffold_profile(
    ctx: PlayHandContext,
    indicator_ids: list[str],
    instruments: list[str],
    timeframe: str,
    candidate_name: str,
) -> Path:
    out_path = ctx.profiles_dir / f"{candidate_name}.json"
    if ctx.dry_run:
        payload = {
            "format": "fuzzfolio.scoring-profile",
            "formatVersion": 1,
            "profile": {
                "name": candidate_name,
                "description": "Dry-run play-hand scaffold.",
                "directionMode": "both",
                "isActive": False,
                "version": "v1",
                "instruments": instruments,
                "notificationThreshold": 80,
                "indicators": [
                    {
                        "meta": {"id": indicator_id, "instanceId": f"dry-{index + 1}"},
                        "config": {
                            "label": indicator_id,
                            "timeframe": timeframe,
                            "lookbackBars": 1,
                            "isActive": True,
                            "weight": 1.0,
                            "talibConfig": [{"name": "timeperiod", "value": 14}],
                        },
                    }
                    for index, indicator_id in enumerate(indicator_ids)
                ],
            },
        }
        _write_json(out_path, payload)
        return out_path
    args = ["profiles", "scaffold"]
    for indicator_id in indicator_ids:
        args.extend(["--indicator", indicator_id])
    for instrument in instruments:
        args.extend(["--instrument", instrument])
    args.extend(["--timeframe", timeframe])
    args.extend(["--out", str(out_path), "--pretty"])
    ctx.cli.run(args)
    _repair_degenerate_profile_ranges(out_path)
    return out_path


def _repair_degenerate_profile_ranges(profile_path: Path) -> bool:
    payload = _load_json(profile_path)
    profile = payload.get("profile") if isinstance(payload, dict) else None
    indicators = profile.get("indicators") if isinstance(profile, dict) else None
    if not isinstance(indicators, list):
        return False
    changed = False
    for indicator in indicators:
        config = indicator.get("config") if isinstance(indicator, dict) else None
        ranges = config.get("ranges") if isinstance(config, dict) else None
        if not isinstance(ranges, dict):
            continue
        for side in ("buy", "sell"):
            values = ranges.get(side)
            if not isinstance(values, list) or len(values) != 2:
                continue
            try:
                lower = float(values[0])
                upper = float(values[1])
            except (TypeError, ValueError):
                continue
            if lower < upper:
                continue
            midpoint = lower
            if lower > upper:
                lower, upper = upper, lower
            if lower == upper:
                if midpoint >= 1.0:
                    lower, upper = midpoint - 0.5, midpoint
                elif midpoint <= 0.0:
                    lower, upper = midpoint, midpoint + 0.5
                else:
                    lower, upper = midpoint - 0.25, midpoint + 0.25
            ranges[side] = [lower, upper]
            changed = True
    if changed:
        _write_json(profile_path, payload)
    return changed


def _evaluate_profile(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    phase: str,
    profile_ref: str,
    profile_path: Path,
    instruments: list[str],
    timeframe: str,
    lookback_months: int,
    reward_matrix: dict[str, Any] | None = None,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    out_dir = (ctx.evals_dir / f"eval_{phase}_{_utc_stamp()}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if ctx.dry_run:
        score = None
        _append_event(
            ctx,
            phase,
            "dry_run",
            stage=stage,
            artifact_dir=str(out_dir),
            score=score,
            reward_matrix=reward_matrix,
        )
        return {"artifact_dir": str(out_dir), "score": score, "profile_ref": profile_ref}
    job_timeout_seconds = max(1, int(ctx.job_timeout_seconds))
    args = [
        "--job-timeout-seconds",
        str(job_timeout_seconds),
        "sensitivity-basket",
        "--profile-ref",
        profile_ref,
        "--output-dir",
        str(out_dir),
        "--timeframe",
        timeframe,
        "--lookback-months",
        str(int(lookback_months)),
    ]
    if as_of_date:
        args.extend(["--as-of-date", str(as_of_date)])
    args.extend(_reward_matrix_cli_args(reward_matrix))
    args.extend(execution_cost_cli_args(ctx.config))
    for instrument in instruments:
        args.extend(["--instrument", instrument])
    args.append("--pretty")
    ctx.cli.run(
        args,
        timeout_seconds=_play_hand_eval_cli_timeout_seconds(job_timeout_seconds),
    )
    compare_payload = ctx.cli.score_artifact(out_dir)
    snapshot_path = out_dir / "sensitivity-response.json"
    snapshot = load_sensitivity_snapshot(out_dir) if snapshot_path.exists() else None
    attempt_score = build_attempt_score(compare_payload, snapshot)
    play_hand_role = _play_hand_role_for_phase(phase)
    play_hand_decision = _play_hand_default_decision(play_hand_role)
    with ctx.io_lock:
        record = make_attempt_record(
            ctx.config,
            ctx.attempts_path,
            ctx.run_id,
            out_dir,
            attempt_score,
            candidate_name=phase,
            profile_ref=profile_ref,
            profile_path=profile_path,
            sensitivity_snapshot_path=snapshot_path if snapshot_path.exists() else None,
            note=f"play_hand_v1:{phase}",
            requested_horizon_months=int(lookback_months),
            requested_timeframe=timeframe,
            max_reward_r=(
                reward_matrix.get("requested_max_reward_r")
                if isinstance(reward_matrix, dict)
                else None
            ),
            reward_matrix=dict(reward_matrix) if isinstance(reward_matrix, dict) else None,
            reward_step_r=(
                reward_matrix.get("reward_step_r")
                if isinstance(reward_matrix, dict)
                else None
            ),
            reward_columns=(
                reward_matrix.get("reward_columns")
                if isinstance(reward_matrix, dict)
                else None
            ),
            effective_max_reward_r=(
                reward_matrix.get("effective_max_reward_r")
                if isinstance(reward_matrix, dict)
                else None
            ),
            runner=PLAY_HAND_RUNNER,
            attempt_role=play_hand_role,
            attempt_decision=play_hand_decision,
            strategy_family_id=ctx.run_id,
            canonical_attempt_id=None,
            is_canonical_attempt=False,
            is_canonical_playhand_attempt=False,
            play_hand_role=play_hand_role,
            play_hand_stage=stage.label,
            play_hand_instrument=_play_hand_instrument_for_phase(phase),
        )
        append_attempt(ctx.attempts_path, record)
        render_progress_artifacts(
            load_attempts(ctx.attempts_path),
            ctx.run_dir / "progress.png",
            run_metadata_path=ctx.run_dir / "run-metadata.json",
            lower_is_better=ctx.config.research.plot_lower_is_better,
        )
        _append_event(
            ctx,
            phase,
            "evaluated",
            stage=stage,
            artifact_dir=str(out_dir),
            attempt_id=record.attempt_id,
            score=record.composite_score,
            score_basis=record.score_basis,
            reward_matrix=reward_matrix,
            as_of_date=as_of_date,
        )
    return {
        "artifact_dir": str(out_dir),
        "attempt_id": record.attempt_id,
        "score": record.composite_score,
        "score_basis": record.score_basis,
        "profile_ref": profile_ref,
        "profile_path": str(profile_path),
    }


def _as_float(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _final_scrutiny_outcome(scrutiny: dict[str, Any]) -> dict[str, Any]:
    score = _as_float(scrutiny.get("score") if isinstance(scrutiny, dict) else None)
    if score is None:
        return {
            "passed": False,
            "score": None,
            "reason": "missing_final_36mo_score",
            "reasons": [PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON, "missing_final_36mo_score"],
        }
    if score <= PLAY_HAND_FINAL_SCRUTINY_MIN_SCORE:
        return {
            "passed": False,
            "score": score,
            "reason": "final_36mo_score_not_positive",
            "reasons": [
                PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON,
                "final_36mo_score_not_positive",
            ],
        }
    return {
        "passed": True,
        "score": score,
        "reason": None,
        "reasons": [],
    }


def _resolve_calendar_gate_mode(cli_value: str | None) -> str:
    env_value = str(os.environ.get(PLAY_HAND_CALENDAR_GATE_ENV) or "").strip().lower()
    if env_value in PLAY_HAND_CALENDAR_GATE_MODES:
        return env_value
    token = str(cli_value or "").strip().lower()
    if token in PLAY_HAND_CALENDAR_GATE_MODES:
        return token
    return PLAY_HAND_CALENDAR_GATE_DEFAULT_MODE


def _resolve_screen_anchor_mode(cli_value: str | None) -> str:
    env_value = str(os.environ.get(PLAY_HAND_SCREEN_ANCHOR_ENV) or "").strip().lower()
    if env_value in PLAY_HAND_SCREEN_ANCHOR_MODES:
        return env_value
    token = str(cli_value or "").strip().lower()
    if token in PLAY_HAND_SCREEN_ANCHOR_MODES:
        return token
    return PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MODE


def _resolve_early_exit_mode(cli_value: str | None) -> str:
    env_value = str(os.environ.get(PLAY_HAND_EARLY_EXIT_ENV) or "").strip().lower()
    if env_value in PLAY_HAND_EARLY_EXIT_MODES:
        return env_value
    token = str(cli_value or "").strip().lower()
    if token in PLAY_HAND_EARLY_EXIT_MODES:
        return token
    return PLAY_HAND_EARLY_EXIT_DEFAULT_MODE


def _resolve_coarse_halving_mode(cli_value: str | None) -> str:
    token = str(cli_value or "").strip().lower()
    if token in PLAY_HAND_COARSE_HALVING_MODES:
        return token
    return PLAY_HAND_COARSE_HALVING_DEFAULT_MODE


def _resolve_coarse_probe_budget(value: int | None) -> int:
    try:
        parsed = int(value) if value is not None else PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET
    except (TypeError, ValueError):
        parsed = PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET
    return max(1, parsed)


def _resolve_family_policy_mode(cli_value: str | None) -> str:
    token = str(cli_value or "").strip().lower()
    if token in PLAY_HAND_FAMILY_POLICY_MODES:
        return token
    return PLAY_HAND_FAMILY_POLICY_DEFAULT_MODE


def _coerce_play_hand_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return None


def resolve_playhand_family_policy(
    indicator_deal: dict[str, Any] | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    deal = indicator_deal if isinstance(indicator_deal, dict) else {}
    meta = metadata if isinstance(metadata, dict) else {}
    raw_policy = deal.get("family_policy")
    source = "indicator_deal.family_policy"
    if not isinstance(raw_policy, dict):
        raw_policy = meta.get("dealt_pair_family_policy")
        source = "metadata.dealt_pair_family_policy"
    if not isinstance(raw_policy, dict):
        raw_policy = _seed_pair_family_policy(deal.get("pair"))
        source = "indicator_deal.pair"
    if not isinstance(raw_policy, dict):
        raw_policy = {}
        source = "none"

    family_policy = str(raw_policy.get("family_policy") or "").strip().lower()
    allowed = {
        "template_locked",
        "template_guarded",
        "mutation_friendly",
        "unstable",
        "under_sampled",
    }
    if family_policy not in allowed:
        family_policy = "none"

    exact_required = _coerce_play_hand_bool(raw_policy.get("exact_branch_required"))
    return {
        "family_id": raw_policy.get("family_id"),
        "family_policy": family_policy,
        "family_policy_source": raw_policy.get("family_policy_source"),
        "family_cap_share": raw_policy.get("family_cap_share"),
        "recommended_max_indicators": raw_policy.get("recommended_max_indicators"),
        "role_balanced_fill_limit": raw_policy.get("role_balanced_fill_limit"),
        "mutation_pressure": raw_policy.get("mutation_pressure"),
        "sampling_weight_multiplier": raw_policy.get("sampling_weight_multiplier"),
        "exact_branch_required": bool(exact_required) if exact_required is not None else False,
        "exact_rescue_rate": raw_policy.get("exact_rescue_rate"),
        "mutated_win_rate": raw_policy.get("mutated_win_rate"),
        "avg_mutation_delta": raw_policy.get("avg_mutation_delta"),
        "promotion_rate": raw_policy.get("promotion_rate"),
        "observation_count": raw_policy.get("observation_count"),
        "source": source,
    }


def build_family_policy_execution_state(
    *,
    mode: str,
    family_policy: dict[str, Any],
    exact_template_available: bool,
) -> dict[str, Any]:
    policy = dict(family_policy)
    family = str(policy.get("family_policy") or "none")
    reasons: list[str] = []
    if mode == "off":
        decision = "disabled"
        reasons.append("family_policy_mode_off")
    elif not exact_template_available:
        decision = "not_applicable"
        reasons.append("no_exact_template")
    elif family == "none":
        decision = "not_applicable"
        reasons.append("no_family_policy")
    elif family in {"unstable", "under_sampled"}:
        decision = "metadata_only"
        reasons.append(f"family_policy_{family}")
    elif family == "mutation_friendly":
        decision = "mutation_friendly_no_extra_restriction"
        reasons.append("family_policy_mutation_friendly")
    elif family == "template_locked":
        decision = (
            "template_locked_exact_only"
            if mode == "enforce"
            else "would_template_locked_exact_only"
        )
        reasons.append("family_policy_template_locked")
    elif family == "template_guarded":
        decision = (
            "template_guarded_exact_benchmark_mutation_allowed"
            if mode == "enforce"
            else "would_template_guarded_exact_benchmark_mutation_allowed"
        )
        reasons.append("family_policy_template_guarded")
    else:
        decision = "not_applicable"
        reasons.append("unrecognized_family_policy")

    locked_skip_stages = [
        "lookback_timing",
        "coarse_probe",
        "coarse_expand",
        "focused",
        "instrument_scout",
        "mutated_final_36mo",
    ]
    mutation_allowed = not (mode == "enforce" and family == "template_locked" and exact_template_available)
    skipped_stages = locked_skip_stages if not mutation_allowed else []
    would_skip_stages = (
        locked_skip_stages
        if mode == "report" and family == "template_locked" and exact_template_available
        else []
    )
    if policy.get("exact_branch_required"):
        reasons.append("exact_branch_required")
    if str(policy.get("role_balanced_fill_limit") or "").strip() == "0":
        reasons.append("role_balanced_fill_limit_zero")

    return {
        "version": PLAY_HAND_FAMILY_POLICY_EXECUTION_VERSION,
        "mode": mode,
        **policy,
        "exact_template_available": bool(exact_template_available),
        "decision": decision,
        "mutation_allowed": mutation_allowed,
        "skipped_stages": skipped_stages,
        "would_skip_stages": would_skip_stages,
        "reasons": reasons,
        "would_reduce_mutation_pressure": bool(mode == "report" and family in {"unstable", "under_sampled"}),
        "would_require_extra_screen": bool(mode == "report" and family in {"unstable", "under_sampled"}),
    }


def build_coarse_halving_budget_plan(
    *,
    mode: str,
    total_budget: int,
    probe_budget: int,
) -> dict[str, Any]:
    total = max(1, int(total_budget))
    probe = max(1, int(probe_budget))
    if mode != "enforce":
        return {
            "version": PLAY_HAND_COARSE_HALVING_VERSION,
            "mode": mode,
            "total_budget": total,
            "probe_budget": min(probe, total),
            "expand_budget": 0,
            "split": False,
            "reason": "halving_disabled",
        }
    if total <= probe:
        return {
            "version": PLAY_HAND_COARSE_HALVING_VERSION,
            "mode": mode,
            "total_budget": total,
            "probe_budget": total,
            "expand_budget": 0,
            "split": False,
            "reason": "budget_not_above_probe",
        }
    return {
        "version": PLAY_HAND_COARSE_HALVING_VERSION,
        "mode": mode,
        "total_budget": total,
        "probe_budget": probe,
        "expand_budget": max(0, total - probe),
        "split": True,
        "reason": None,
    }


def should_accept_stage_candidate(
    *,
    incumbent_score: Any,
    candidate_score: Any,
    tolerance: float = STAGE_ACCEPTANCE_DROP_TOLERANCE,
) -> bool:
    candidate = _as_float(candidate_score)
    if candidate is None:
        return False
    incumbent = _as_float(incumbent_score)
    if incumbent is None:
        return True
    return candidate >= incumbent - float(tolerance)


def build_stage_acceptance_decision(
    *,
    stage: str,
    incumbent_score: Any,
    candidate_score: Any,
    tolerance: float = STAGE_ACCEPTANCE_DROP_TOLERANCE,
) -> dict[str, Any]:
    incumbent = _as_float(incumbent_score)
    candidate = _as_float(candidate_score)
    accepted = should_accept_stage_candidate(
        incumbent_score=incumbent,
        candidate_score=candidate,
        tolerance=tolerance,
    )
    if candidate is None:
        reason = "missing_candidate_score"
    elif incumbent is None:
        reason = "missing_incumbent_score"
    elif accepted and candidate >= incumbent:
        reason = "candidate_improved_incumbent"
    elif accepted:
        reason = "candidate_within_incumbent_tolerance"
    else:
        reason = "candidate_below_incumbent_tolerance"
    return {
        "stage": str(stage),
        "accepted": accepted,
        "reason": reason,
        "incumbent_score": incumbent,
        "candidate_score": candidate,
        "tolerance": float(tolerance),
    }


def build_coarse_halving_decision(
    *,
    mode: str,
    total_budget: int,
    probe_budget: int,
    incumbent_score: Any,
    probe_score: Any,
) -> dict[str, Any]:
    plan = build_coarse_halving_budget_plan(
        mode=mode,
        total_budget=total_budget,
        probe_budget=probe_budget,
    )
    incumbent = _as_float(incumbent_score)
    probe = _as_float(probe_score)
    reasons: list[str] = []
    expanded = False
    if not plan["split"]:
        expanded = True
        decision = "use_original_coarse"
        reasons.append(str(plan.get("reason") or "not_split"))
    elif probe is None:
        decision = "skip_expansion"
        reasons.append("missing_probe_score")
    elif probe >= COARSE_HALVING_EXPAND_SCORE:
        expanded = True
        decision = "expand"
        reasons.append("probe_score_met_expand_threshold")
    elif (
        incumbent is not None
        and incumbent >= 60.0
        and probe >= COARSE_HALVING_MIN_NEAR_INCUMBENT_SCORE
        and probe >= incumbent - COARSE_HALVING_NEAR_INCUMBENT_TOLERANCE
    ):
        expanded = True
        decision = "expand"
        reasons.append("probe_score_near_strong_incumbent")
    else:
        decision = "skip_expansion"
        reasons.append("probe_score_below_expand_threshold")
        if incumbent is not None and incumbent >= 60.0:
            reasons.append("probe_not_near_strong_incumbent")

    expand_budget = int(plan["expand_budget"])
    estimated_saved = 0 if expanded else expand_budget
    skipped_stages: list[str] = []
    if not expanded:
        skipped_stages = ["coarse_expand", "focused", "instrument_scout"]
    return {
        "version": PLAY_HAND_COARSE_HALVING_VERSION,
        "mode": mode,
        "probe_budget": int(plan["probe_budget"]),
        "expand_budget": expand_budget,
        "total_budget": int(plan["total_budget"]),
        "expanded": expanded,
        "decision": decision,
        "reasons": reasons,
        "incumbent_score": incumbent,
        "probe_score": probe,
        "estimated_saved_evaluations": estimated_saved,
        "skipped_stages": skipped_stages,
    }


def _early_exit_saved_if_enforced(checkpoint: str) -> dict[str, bool]:
    order = [
        "after_baseline",
        "after_lookback_top",
        "after_coarse_top",
        "after_focused_top",
        "before_instrument_scout",
        "before_final_scrutiny",
    ]
    index = order.index(checkpoint) if checkpoint in order else len(order)
    return {
        "would_skip_lookback": index <= order.index("after_baseline"),
        "would_skip_coarse": index <= order.index("after_lookback_top"),
        "would_skip_focused": index <= order.index("after_coarse_top"),
        "would_skip_instrument_scout": index <= order.index("after_focused_top"),
        "would_skip_final_36mo": False,
    }


def build_early_exit_decision(
    *,
    checkpoint: str,
    evidence: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    scores = evidence.get("scores") if isinstance(evidence.get("scores"), dict) else {}
    inputs = (
        evidence.get("early_exit_inputs")
        if isinstance(evidence.get("early_exit_inputs"), dict)
        else {}
    )
    source = evidence.get("source") if isinstance(evidence.get("source"), dict) else {}
    baseline = _as_float(scores.get("baseline_3mo"))
    lookback = _as_float(scores.get("lookback_top_3mo"))
    coarse = _as_float(scores.get("coarse_top_3mo"))
    focused = _as_float(scores.get("focused_top_3mo"))
    candidate = focused if focused is not None else coarse if coarse is not None else lookback
    if candidate is None:
        candidate = baseline
    deltas = {
        "lookback_delta_vs_baseline": _as_float(
            inputs.get("lookback_delta_vs_baseline")
        ),
        "coarse_delta_vs_baseline": _as_float(
            inputs.get("coarse_delta_vs_baseline")
        ),
        "focused_delta_vs_baseline": _as_float(
            inputs.get("focused_delta_vs_baseline")
        ),
        "final_delta_vs_focused": _as_float(inputs.get("final_delta_vs_focused")),
    }
    reasons: list[str] = []
    rules_fired: list[str] = []
    would_exit = False

    def _fire(rule: str) -> None:
        reasons.append(rule)
        rules_fired.append(rule)

    if not bool(inputs.get("safe_for_report_mode", False)):
        _fire("insufficient_health_context")
        would_exit = False
    elif checkpoint == "after_baseline":
        if baseline is None:
            would_exit = True
            _fire("missing_baseline_score")
        elif baseline <= 0.0:
            would_exit = True
            _fire("baseline_score_not_positive")

    elif checkpoint == "after_lookback_top":
        if lookback is None:
            would_exit = True
            _fire("missing_lookback_top_score")
        elif baseline is not None and lookback < baseline - 15.0 and lookback < 55.0:
            would_exit = True
            _fire(f"lookback_score_below_baseline_by_{baseline - lookback:.2f}")
    elif checkpoint == "after_coarse_top":
        if coarse is None:
            would_exit = True
            _fire("missing_coarse_top_score")
        elif coarse < 45.0:
            would_exit = True
            _fire("coarse_top_score_below_45")
        elif baseline is not None and coarse < baseline - 10.0:
            would_exit = True
            _fire(f"coarse_top_score_below_baseline_by_{baseline - coarse:.2f}")
    elif checkpoint == "after_focused_top":
        if focused is None:
            would_exit = True
            _fire("missing_focused_top_score")
        elif focused < 50.0 and (coarse is None or focused <= coarse + 2.0):
            would_exit = True
            _fire("focused_top_below_50_without_material_improvement")
    elif checkpoint == "before_instrument_scout":
        if candidate is None:
            would_exit = True
            _fire("missing_scout_candidate_score")
        elif candidate < 45.0:
            would_exit = True
            _fire("candidate_below_scout_floor")
    elif checkpoint == "before_final_scrutiny":
        reasons.append("final_scrutiny_retained_for_learning")

    if source.get("family_policy"):
        reasons.append(f"family_policy:{source['family_policy']}")
    guided_source = str(inputs.get("guided_or_role_balanced") or "").strip()
    if guided_source:
        reasons.append(f"source:{guided_source}")

    return {
        "version": PLAY_HAND_EARLY_EXIT_VERSION,
        "mode": mode,
        "checkpoint": checkpoint,
        "would_exit": bool(would_exit),
        "would_exit_research": False,
        "would_exit_compute_expansion": bool(would_exit),
        "reasons": reasons,
        "rules_fired": rules_fired,
        "source": {
            "dealt_indicator_source": source.get("dealt_indicator_source"),
            "dealt_recipe": source.get("dealt_recipe"),
            "template_branch_source_probe_id": source.get(
                "template_branch_source_probe_id"
            ),
            "family_policy": source.get("family_policy"),
        },
        "scores": {
            "baseline_3mo": baseline,
            "lookback_top_3mo": lookback,
            "coarse_top_3mo": coarse,
            "focused_top_3mo": focused,
            "candidate_score": candidate,
        },
        "deltas": deltas,
        "saved_if_enforced": _early_exit_saved_if_enforced(checkpoint),
    }


def sample_screen_anchor(
    *,
    mode: str,
    screen_months: int,
    max_offset_months: int = PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MAX_OFFSET_MONTHS,
    seed: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Sample the screen-phase evaluation anchor.

    In ``random`` mode the discovery screen is anchored at a uniformly random
    point in the recent past instead of always at "now", so the corpus stops
    selecting exclusively for strategies fit to the current regime. The
    36-month scrutiny stays anchored at now regardless of this anchor.
    """
    current = now or datetime.now(timezone.utc)
    requested_max = max(0, int(max_offset_months))
    effective_max = min(
        requested_max,
        max(0, PLAY_HAND_SCREEN_ANCHOR_OFFSET_BUDGET_MONTHS - int(screen_months)),
    )
    anchor: dict[str, Any] = {
        "mode": mode,
        "as_of_date": None,
        "offset_days": 0,
        "requested_max_offset_months": requested_max,
        "effective_max_offset_months": effective_max,
    }
    if mode != "random" or effective_max <= 0:
        return anchor
    # Dedicated stream keyed off the run seed so the anchor is reproducible
    # without perturbing the existing indicator/instrument deal sequence.
    rng = random.Random(f"play-hand-screen-anchor:{seed}") if seed is not None else random.Random()
    offset_days = int(round(rng.uniform(0.0, effective_max * AVERAGE_DAYS_PER_MONTH)))
    anchor["offset_days"] = offset_days
    if offset_days > 0:
        anchor["as_of_date"] = (current - timedelta(days=offset_days)).date().isoformat()
    return anchor


def _branch_calendar_gate(branch: dict[str, Any], *, mode: str) -> dict[str, Any]:
    scrutiny = branch.get("scrutiny") if isinstance(branch.get("scrutiny"), dict) else {}
    artifact_dir = str(scrutiny.get("artifact_dir") or "").strip()
    points = _curve_points_from_artifact(Path(artifact_dir)) if artifact_dir else []
    robustness = compute_calendar_robustness(points)
    decision = evaluate_calendar_gate(robustness)
    return {
        "mode": mode,
        "passed": bool(decision.passed),
        "reasons": list(decision.reasons),
        "metrics": dict(decision.metrics),
        "artifact_dir": artifact_dir or None,
    }


def _select_final_scrutiny_branch(
    branches: list[dict[str, Any]],
    *,
    enforce_calendar_gate: bool = False,
) -> dict[str, Any] | None:
    candidates = [branch for branch in branches if isinstance(branch, dict)]
    if not candidates:
        return None

    def rank(branch: dict[str, Any]) -> tuple[int, int, float, int]:
        outcome = branch.get("outcome") if isinstance(branch.get("outcome"), dict) else {}
        score = _as_float(outcome.get("score"))
        score_value = score if score is not None else float("-inf")
        passed = 1 if outcome.get("passed") else 0
        gate = (
            branch.get("calendar_gate")
            if isinstance(branch.get("calendar_gate"), dict)
            else None
        )
        gate_passed = 1 if (not enforce_calendar_gate or gate is None or gate.get("passed")) else 0
        # Keep the normal mutated branch as the tie-breaker when evidence is equal.
        branch_bias = 1 if str(branch.get("branch") or "") == "mutated" else 0
        # In enforce mode a branch passing both score and gate outranks a branch
        # passing score alone; otherwise the legacy ordering is preserved exactly.
        return passed * gate_passed, passed, score_value, branch_bias

    return max(candidates, key=rank)


def _as_int(value: Any) -> int | None:
    numeric = _as_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _sensitivity_aggregate_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    data = snapshot.get("data") if isinstance(snapshot.get("data"), dict) else snapshot
    aggregate = data.get("aggregate") if isinstance(data, dict) else None
    if isinstance(aggregate, dict):
        return aggregate
    return data if isinstance(data, dict) else {}


def _score_lab_score_from_snapshot(snapshot: dict[str, Any]) -> float | None:
    aggregate = _sensitivity_aggregate_payload(snapshot)
    score_lab = aggregate.get("score_lab") if isinstance(aggregate.get("score_lab"), dict) else {}
    return _as_float(score_lab.get("score"))


def _curve_points_from_artifact(artifact_dir: Path) -> list[dict[str, Any]]:
    payload = _load_json(artifact_dir / "best-cell-path-detail.json")
    curve = payload.get("curve") if isinstance(payload.get("curve"), dict) else {}
    points = curve.get("points") if isinstance(curve, dict) else None
    return [point for point in points if isinstance(point, dict)] if isinstance(points, list) else []


def _coerce_curve_date(point: dict[str, Any]) -> str:
    date_value = str(point.get("date") or "").strip()
    if date_value:
        return date_value[:10]
    timestamp = _as_float(point.get("time"))
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()


def _curve_value(point: dict[str, Any]) -> float | None:
    for key in ("equity_r", "realized_r", "cumulative_realized_r"):
        value = _as_float(point.get(key))
        if value is not None:
            return value
    return None


def _curve_realized_value(point: dict[str, Any]) -> float | None:
    for key in ("realized_r", "cumulative_realized_r", "equity_r"):
        value = _as_float(point.get(key))
        if value is not None:
            return value
    return None


def _curve_features(points: list[dict[str, Any]]) -> dict[str, Any]:
    dated: dict[str, dict[str, Any]] = {}
    for point in points:
        date_key = _coerce_curve_date(point)
        value = _curve_value(point)
        if not date_key or value is None:
            continue
        dated[date_key] = {
            "value": value,
            "realized": _curve_realized_value(point),
            "drawdown": _as_float(point.get("drawdown_r")) or 0.0,
            "closed_trades": _as_int(point.get("closed_trade_count")),
        }
    ordered_dates = sorted(dated)
    series = {date_key: float(dated[date_key]["value"]) for date_key in ordered_dates}
    daily_changes: dict[str, float] = {}
    active_dates: set[str] = set()
    previous_value: float | None = None
    previous_realized: float | None = None
    previous_closed: int | None = None
    max_drawdown = max((float(item["drawdown"]) for item in dated.values()), default=0.0)
    drawdown_threshold = max(0.25, max_drawdown * 0.25)
    drawdown_dates = {
        date_key
        for date_key, item in dated.items()
        if max_drawdown > 0.0 and float(item["drawdown"]) >= drawdown_threshold
    }
    for date_key in ordered_dates:
        item = dated[date_key]
        value = float(item["value"])
        realized = _as_float(item.get("realized"))
        closed = _as_int(item.get("closed_trades"))
        delta = 0.0 if previous_value is None else value - previous_value
        daily_changes[date_key] = delta
        realized_delta = (
            None if previous_realized is None or realized is None else realized - previous_realized
        )
        if (
            abs(delta) > 1e-9
            or (realized_delta is not None and abs(realized_delta) > 1e-9)
            or (previous_closed is not None and closed is not None and closed != previous_closed)
        ):
            active_dates.add(date_key)
        previous_value = value
        previous_realized = realized
        previous_closed = closed
    return {
        "series": series,
        "daily_changes": daily_changes,
        "active_dates": active_dates,
        "drawdown_dates": drawdown_dates,
        "point_count": len(ordered_dates),
        "max_drawdown_r": max_drawdown,
    }


def _pearson_correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 5:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0.0 or right_var <= 0.0:
        return None
    covariance = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    return covariance / (left_var**0.5 * right_var**0.5)


def _jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _instrument_curve_similarity(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_features = left.get("_curve_features") if isinstance(left.get("_curve_features"), dict) else {}
    right_features = right.get("_curve_features") if isinstance(right.get("_curve_features"), dict) else {}
    left_changes = left_features.get("daily_changes") if isinstance(left_features.get("daily_changes"), dict) else {}
    right_changes = right_features.get("daily_changes") if isinstance(right_features.get("daily_changes"), dict) else {}
    common_dates = sorted(set(left_changes) & set(right_changes))
    correlation = None
    if len(common_dates) >= 5:
        correlation = _pearson_correlation(
            [float(left_changes[date]) for date in common_dates],
            [float(right_changes[date]) for date in common_dates],
        )
    positive_correlation = max(0.0, float(correlation)) if correlation is not None else 0.0
    active_overlap = _jaccard(
        set(left_features.get("active_dates") or set()),
        set(right_features.get("active_dates") or set()),
    )
    drawdown_overlap = _jaccard(
        set(left_features.get("drawdown_dates") or set()),
        set(right_features.get("drawdown_dates") or set()),
    )
    similarity_score = max(
        0.0,
        min(1.0, positive_correlation * 0.60 + active_overlap * 0.25 + drawdown_overlap * 0.15),
    )
    return {
        "left_instrument": left.get("instrument"),
        "right_instrument": right.get("instrument"),
        "correlation": round(correlation, 6) if correlation is not None else None,
        "positive_correlation": round(positive_correlation, 6),
        "active_overlap_ratio": round(active_overlap, 6),
        "drawdown_overlap_ratio": round(drawdown_overlap, 6),
        "overlap_days": len(common_dates),
        "similarity_score": round(similarity_score, 6),
    }


def _public_scout_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key != "_curve_features"
    }


def _instrument_scout_record(
    instrument: str,
    evaluation: dict[str, Any],
) -> dict[str, Any]:
    artifact_dir_raw = str(evaluation.get("artifact_dir") or "").strip()
    artifact_dir = Path(artifact_dir_raw) if artifact_dir_raw else Path("__missing__")
    snapshot = load_sensitivity_snapshot(artifact_dir) if artifact_dir.exists() else None
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    aggregate = _sensitivity_aggregate_payload(snapshot)
    best_cell = aggregate.get("best_cell") if isinstance(aggregate.get("best_cell"), dict) else {}
    matrix_summary = (
        aggregate.get("matrix_summary") if isinstance(aggregate.get("matrix_summary"), dict) else {}
    )
    path_metrics = (
        aggregate.get("best_cell_path_metrics")
        if isinstance(aggregate.get("best_cell_path_metrics"), dict)
        else {}
    )
    score = _as_float(evaluation.get("score"))
    if score is None:
        score = _score_lab_score_from_snapshot(snapshot)
    resolved_trades = _as_int(best_cell.get("resolved_trades"))
    if resolved_trades is None:
        resolved_trades = _as_int(path_metrics.get("trade_count"))
    curve_features = _curve_features(_curve_points_from_artifact(artifact_dir))
    return {
        "instrument": instrument,
        "artifact_dir": artifact_dir_raw,
        "attempt_id": evaluation.get("attempt_id"),
        "score": score,
        "score_basis": evaluation.get("score_basis"),
        "resolved_trades": resolved_trades,
        "expectancy_r": _as_float(best_cell.get("avg_net_r_per_closed_trade")),
        "profit_factor": _as_float(best_cell.get("profit_factor")),
        "max_drawdown_r": _as_float(path_metrics.get("max_drawdown_r")),
        "reward_ridge_label": matrix_summary.get("reward_ridge_label"),
        "normal_r_positive_cell_ratio": _as_float(
            matrix_summary.get("normal_r_positive_cell_ratio")
        ),
        "curve_point_count": curve_features.get("point_count"),
        "_curve_features": curve_features,
    }


def _select_instrument_scout_records(
    primary: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    max_selected: int = INSTRUMENT_SCOUT_DEFAULT_MAX_SELECTED,
    min_score: float = INSTRUMENT_SCOUT_MIN_SCORE,
    score_tolerance: float = INSTRUMENT_SCOUT_SCORE_TOLERANCE,
    max_similarity: float = INSTRUMENT_SCOUT_MAX_SIMILARITY,
    min_resolved_trades: int = INSTRUMENT_SCOUT_MIN_RESOLVED_TRADES,
) -> dict[str, Any]:
    primary_score = _as_float(primary.get("score"))
    score_floor = (
        min_score if primary_score is None else max(min_score, primary_score - score_tolerance)
    )
    selected = [{**primary, "decision": "accepted", "decision_reason": "primary_anchor"}]
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    primary_features = primary.get("_curve_features") if isinstance(primary.get("_curve_features"), dict) else {}
    primary_curve_available = bool(primary_features.get("daily_changes"))
    ranked_candidates = sorted(
        candidates,
        key=lambda item: _as_float(item.get("score")) if _as_float(item.get("score")) is not None else float("-inf"),
        reverse=True,
    )
    for candidate in ranked_candidates:
        reasons: list[str] = []
        score = _as_float(candidate.get("score"))
        if score is None:
            reasons.append("missing_score")
        elif score < score_floor:
            reasons.append("score_below_floor")
        resolved_trades = _as_int(candidate.get("resolved_trades"))
        if resolved_trades is not None and resolved_trades < min_resolved_trades:
            reasons.append("too_few_resolved_trades")
        expectancy = _as_float(candidate.get("expectancy_r"))
        if expectancy is not None and expectancy <= 0.0:
            reasons.append("non_positive_expectancy")
        curve_features = candidate.get("_curve_features") if isinstance(candidate.get("_curve_features"), dict) else {}
        if not curve_features.get("daily_changes"):
            reasons.append("missing_curve")
        if not primary_curve_available:
            reasons.append("missing_primary_curve")

        similarities = [
            _instrument_curve_similarity(candidate, selected_record)
            for selected_record in selected
            if isinstance(selected_record.get("_curve_features"), dict)
            and selected_record["_curve_features"].get("daily_changes")
        ]
        max_pair = max(
            similarities,
            key=lambda item: float(item.get("similarity_score") or 0.0),
            default=None,
        )
        if max_pair and float(max_pair.get("similarity_score") or 0.0) > max_similarity:
            reasons.append("too_similar_to_selected")
        if len(selected) >= max(1, int(max_selected)):
            reasons.append("basket_full")

        decorated = {
            **candidate,
            "score_floor": round(score_floor, 4),
            "similarity_to_selected": similarities,
            "max_similarity_to_selected": (
                float(max_pair.get("similarity_score")) if max_pair else None
            ),
        }
        if reasons:
            rejected.append(
                {
                    **decorated,
                    "decision": "rejected",
                    "decision_reasons": reasons,
                }
            )
            continue
        accepted_record = {
            **decorated,
            "decision": "accepted",
            "decision_reasons": ["performance_and_diversification_gate_passed"],
        }
        selected.append(accepted_record)
        accepted.append(accepted_record)

    return {
        "version": "instrument_scout_v1",
        "policy": {
            "min_score": min_score,
            "score_tolerance": score_tolerance,
            "score_floor": round(score_floor, 4),
            "max_similarity": max_similarity,
            "max_selected": max(1, int(max_selected)),
            "min_resolved_trades": min_resolved_trades,
            "similarity_basis": "daily strategy-output changes, active-day overlap, and drawdown-overlap ratio",
        },
        "selected_instruments": [str(item.get("instrument")) for item in selected],
        "primary": _public_scout_record(selected[0]),
        "accepted": [_public_scout_record(item) for item in accepted],
        "rejected": [_public_scout_record(item) for item in rejected],
        "ranked_candidates": [_public_scout_record(item) for item in ranked_candidates],
    }


def _instrument_scout_worker_count(total_instruments: int) -> int:
    total = max(0, int(total_instruments))
    if total <= 1:
        return total
    raw = os.environ.get("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS")
    if raw is None or str(raw).strip() == "":
        requested = INSTRUMENT_SCOUT_DEFAULT_WORKERS
    else:
        try:
            requested = int(str(raw).strip())
        except ValueError:
            requested = INSTRUMENT_SCOUT_DEFAULT_WORKERS
    return max(1, min(total, INSTRUMENT_SCOUT_MAX_WORKERS, requested))


def _evaluate_instrument_scout_records(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    profile_ref: str,
    profile_path: Path,
    primary: str,
    candidates: list[str],
    timeframe: str,
    lookback_months: int,
    reward_matrix: dict[str, Any] | None,
    as_of_date: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    instruments = [primary, *candidates]
    worker_count = _instrument_scout_worker_count(len(instruments))

    def evaluate(instrument: str) -> tuple[str, dict[str, Any]]:
        evaluation = _evaluate_profile(
            ctx,
            stage=stage,
            phase=f"instrument_scout_{_safe_label(instrument)}_{lookback_months}mo",
            profile_ref=profile_ref,
            profile_path=profile_path,
            instruments=[instrument],
            timeframe=timeframe,
            lookback_months=lookback_months,
            reward_matrix=reward_matrix,
            as_of_date=as_of_date,
        )
        return instrument, _instrument_scout_record(instrument, evaluation)

    if worker_count <= 1:
        records = dict(evaluate(instrument) for instrument in instruments)
    else:
        records: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="playhand-scout",
        ) as executor:
            futures = {executor.submit(evaluate, instrument): instrument for instrument in instruments}
            for future in as_completed(futures):
                instrument, record = future.result()
                records[instrument] = record

    return records[primary], [records[instrument] for instrument in candidates], worker_count


def _run_instrument_scout(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    profile_ref: str,
    profile_path: Path,
    instrument_deal: dict[str, Any],
    instruments: list[str],
    timeframe: str,
    lookback_months: int,
    rng: random.Random,
    enabled: bool,
    scout_size: int,
    max_selected: int,
    reward_matrix: dict[str, Any] | None = None,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    primary = str((instruments or [instrument_deal.get("primary_instrument")])[0] or "").strip().upper()
    pool = _clean_tokens(instrument_deal.get("instrument_pool") or [])
    candidates = [instrument for instrument in pool if instrument != primary]
    rng.shuffle(candidates)
    candidates = candidates[: max(0, int(scout_size))]
    artifact_path = ctx.run_dir / "instrument-scout.json"
    if not enabled:
        result = {
            "version": "instrument_scout_v1",
            "status": "skipped",
            "reason": "disabled",
            "selected_instruments": instruments,
        }
        _write_json(artifact_path, result)
        _append_event(ctx, "instrument_scout", "skipped", stage=stage, reason="disabled")
        return result
    if str(instrument_deal.get("source") or "") == "pinned":
        result = {
            "version": "instrument_scout_v1",
            "status": "skipped",
            "reason": "pinned_instruments",
            "selected_instruments": instruments,
        }
        _write_json(artifact_path, result)
        _append_event(ctx, "instrument_scout", "skipped", stage=stage, reason="pinned_instruments")
        return result
    if not primary or not candidates or max_selected <= 1:
        result = {
            "version": "instrument_scout_v1",
            "status": "skipped",
            "reason": "no_candidate_instruments",
            "primary_instrument": primary,
            "candidate_instruments": candidates,
            "selected_instruments": [primary] if primary else instruments,
        }
        _write_json(artifact_path, result)
        _append_event(
            ctx,
            "instrument_scout",
            "skipped",
            stage=stage,
            reason="no_candidate_instruments",
            candidate_instruments=candidates,
        )
        return result
    if ctx.dry_run:
        result = {
            "version": "instrument_scout_v1",
            "status": "dry_run",
            "primary_instrument": primary,
            "candidate_instruments": candidates,
            "selected_instruments": [primary],
            "lookback_months": lookback_months,
        }
        _write_json(artifact_path, result)
        _append_event(
            ctx,
            "instrument_scout",
            "dry_run",
            stage=stage,
            primary_instrument=primary,
            candidate_instruments=candidates,
        )
        return result

    _append_event(
        ctx,
        "instrument_scout",
        "started",
        stage=stage,
        primary_instrument=primary,
        candidate_instruments=candidates,
        lookback_months=lookback_months,
        max_selected=max_selected,
        worker_count=_instrument_scout_worker_count(1 + len(candidates)),
        reward_matrix=reward_matrix,
    )
    primary_record, candidate_records, worker_count = _evaluate_instrument_scout_records(
        ctx,
        stage=stage,
        profile_ref=profile_ref,
        profile_path=profile_path,
        primary=primary,
        candidates=candidates,
        timeframe=timeframe,
        lookback_months=lookback_months,
        reward_matrix=reward_matrix,
        as_of_date=as_of_date,
    )

    result = _select_instrument_scout_records(
        primary_record,
        candidate_records,
        max_selected=max_selected,
    )
    result.update(
        {
            "status": "completed",
            "primary_instrument": primary,
            "candidate_instruments": candidates,
            "lookback_months": lookback_months,
            "worker_count": worker_count,
            "artifact_path": str(artifact_path.resolve()),
            "reward_matrix": reward_matrix,
        }
    )
    _write_json(artifact_path, result)
    _append_event(
        ctx,
        "instrument_scout",
        "completed",
        stage=stage,
        artifact_path=str(artifact_path),
        selected_instruments=result.get("selected_instruments"),
        accepted_count=len(result.get("accepted") or []),
        rejected_count=len(result.get("rejected") or []),
        worker_count=worker_count,
        reward_matrix=reward_matrix,
    )
    return result


def _sweep_id_from_stderr(stderr: str) -> str | None:
    sweep_id = None
    for match in re.finditer(r"\[sweep\]\s+Submitted sweep\s+(sweep-[A-Za-z0-9\-]+)", stderr or ""):
        sweep_id = match.group(1)
    return sweep_id


def _sweep_state_from_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        return data
    return payload if isinstance(payload, dict) else {}


def _sweep_progress_from_state(state: dict[str, Any], *, fallback_mode: str) -> dict[str, Any] | None:
    progress = state.get("progress") if isinstance(state.get("progress"), dict) else None
    if not isinstance(progress, dict):
        return None
    mode = str(state.get("mode") or fallback_mode or "").strip().lower()
    completed = _as_int(progress.get("completed"))
    total = _as_int(progress.get("total"))
    failed = _as_int(progress.get("failed")) or 0
    generation = _as_int(progress.get("generation"))
    max_generations = _as_int(progress.get("max_generations"))
    best_fitness = _as_float(progress.get("best_fitness"))

    percent: float | None = None
    if completed is not None and total and total > 0:
        percent = min(100.0, max(0.0, (float(completed) / float(total)) * 100.0))
    elif generation is not None and max_generations and max_generations > 0:
        percent = min(100.0, max(0.0, (float(generation) / float(max_generations)) * 100.0))

    if percent is None:
        return None

    display_bits: list[str] = []
    unit = "evals" if mode == "evolutionary" else "perms"
    if completed is not None and total:
        display_bits.append(f"{completed}/{total} {unit}")
    if failed:
        display_bits.append(f"{failed} failed")
    if generation is not None and max_generations:
        display_bits.append(f"gen {generation}/{max_generations}")
    if best_fitness is not None:
        display_bits.append(f"best={best_fitness:.4f}")

    return {
        "mode": mode,
        "percent": round(percent, 1),
        "completed": completed,
        "total": total,
        "failed": failed,
        "generation": generation,
        "max_generations": max_generations,
        "best_fitness": best_fitness,
        "display": f"{percent:.1f}% ({', '.join(display_bits)})",
    }


def _sweep_progress_from_cli_stderr(stderr: str, *, fallback_mode: str) -> dict[str, Any] | None:
    progress: dict[str, Any] | None = None
    for line in (stderr or "").splitlines():
        deterministic = re.search(
            r"\[sweep\]\s+(\d+)/(\d+)\s+permutations complete\s+\((\d+)\s+failed\)",
            line,
        )
        if deterministic:
            completed = int(deterministic.group(1))
            total = int(deterministic.group(2))
            failed = int(deterministic.group(3))
            state = {
                "mode": "deterministic",
                "progress": {"completed": completed, "total": total, "failed": failed},
            }
            progress = _sweep_progress_from_state(state, fallback_mode=fallback_mode)
            continue
        evolutionary = re.search(
            r"\[sweep\]\s+generation\s+(\d+)/(\d+)(?:,\s+best fitness:\s+([-+]?\d+(?:\.\d+)?))?",
            line,
        )
        if evolutionary:
            generation = int(evolutionary.group(1))
            max_generations = int(evolutionary.group(2))
            best_fitness = _as_float(evolutionary.group(3))
            state = {
                "mode": "evolutionary",
                "progress": {
                    "generation": generation,
                    "max_generations": max_generations,
                    "best_fitness": best_fitness,
                },
            }
            progress = _sweep_progress_from_state(state, fallback_mode=fallback_mode)
    return progress


def _fetch_sweep_progress(
    cli: FuzzfolioCli,
    sweep_id: str | None,
    *,
    fallback_mode: str,
) -> dict[str, Any] | None:
    if not sweep_id:
        return None
    try:
        result = cli.run(
            ["sweep", "status", "--sweep-id", sweep_id],
            check=False,
            timeout_seconds=10,
        )
    except Exception:
        return None
    if result.returncode != 0 or not isinstance(result.parsed_json, dict):
        return None
    state = _sweep_state_from_status_payload(result.parsed_json)
    return _sweep_progress_from_state(state, fallback_mode=fallback_mode)


def _run_sweep(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    phase: str,
    profile_ref: str,
    profile_payload: dict[str, Any] | None,
    instruments: list[str],
    axes: list[str],
    mode: str,
    sweep_budget: str,
    max_permutations: int,
    reward_matrix: dict[str, Any] | None = None,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    original_axes = list(axes)
    sweep_reward_matrix = reward_matrix
    if reward_matrix and not ctx.dry_run and not _sweep_reward_matrix_supported(ctx.cli):
        sweep_reward_matrix = None
        _append_event(
            ctx,
            phase,
            "reward_matrix_not_supported",
            stage=stage,
            reward_matrix=reward_matrix,
            reason="active fuzzfolio-agent-cli sweep run does not expose --reward-step-r/--reward-columns",
        )
        console.print(
            f"{stage.prefix} [cyan]{phase}[/] [yellow]reward cap not applied to sweep[/] "
            "(active fuzzfolio-agent-cli needs rebuild)"
        )
    evolutionary_settings = (
        evolutionary_budget_settings(sweep_budget, evaluation_budget=max_permutations)
        if mode == "evolutionary"
        else None
    )
    evaluation_budget = (
        int(evolutionary_settings["evaluation_budget"])
        if evolutionary_settings is not None
        else None
    )
    axis_plan = plan_sweep_axes(
        original_axes,
        profile_payload=profile_payload,
        phase=phase,
        max_permutations=max_permutations,
        search_mode=mode,
    )
    axes = axis_plan.axes
    original_permutations = axis_plan.original_permutations
    selected_permutations = axis_plan.selected_permutations
    axis_plan_payload = axis_plan.event_payload()
    axis_plan_payload["sweep_budget"] = sweep_budget
    if sweep_reward_matrix:
        axis_plan_payload["reward_matrix"] = sweep_reward_matrix
    if evolutionary_settings is not None:
        axis_plan_payload.update(
            {
                "population_size": evolutionary_settings["population_size"],
                "max_generations": evolutionary_settings["max_generations"],
                "evaluation_budget": evolutionary_settings["evaluation_budget"],
                "search_space_permutations": selected_permutations,
            }
        )
    constrained_axes = [
        str(item.get("axis"))
        for item in [*axis_plan.anchored_axes, *axis_plan.dropped_axes]
        if item.get("axis")
    ]
    if constrained_axes or selected_permutations != original_permutations:
        _append_event(
            ctx,
            phase,
            "budgeted",
            stage=stage,
            **axis_plan_payload,
        )
    if not axes:
        result = {
            "sweep_id": f"skipped-{phase}",
            "mode": mode,
            "ranked_permutations": [],
            "parameter_importance": [],
            "axes": [],
        }
        _append_event(
            ctx,
            phase,
            "skipped",
            stage=stage,
            reason="no axes fit sweep permutation budget",
            original_axes=original_axes,
            original_permutations=original_permutations,
            max_permutations=max_permutations,
            sweep_budget=sweep_budget,
            evaluation_budget=evaluation_budget,
            reward_matrix=sweep_reward_matrix,
        )
        return {"artifact_dir": None, "result": result, "axes": []}
    _render_sweep_plan(
        stage=stage,
        phase=phase,
        axes=axes,
        dropped_axes=constrained_axes,
        permutation_count=selected_permutations,
        original_permutations=original_permutations,
        max_permutations=max_permutations,
        mode=mode,
        evaluation_budget=evaluation_budget,
        reward_matrix=sweep_reward_matrix,
    )
    out_dir = (ctx.evals_dir / f"sweep_{phase}_{_utc_stamp()}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(out_dir / "sweep-axis-plan.json", axis_plan_payload)
    if ctx.dry_run:
        result = {
            "sweep_id": f"dry-{phase}",
            "mode": mode,
            "ranked_permutations": [],
            "parameter_importance": [],
            "axes": axes,
            "axis_plan": axis_plan_payload,
        }
        _write_json(out_dir / "sweep-results.json", result)
        _append_event(
            ctx,
            phase,
            "dry_run",
            stage=stage,
            artifact_dir=str(out_dir),
            axes=axes,
            permutation_count=selected_permutations,
            sweep_budget=sweep_budget,
            evaluation_budget=evaluation_budget,
            reward_matrix=sweep_reward_matrix,
        )
        return {"artifact_dir": str(out_dir), "result": result, "axes": axes}
    job_timeout_seconds = max(1, int(ctx.job_timeout_seconds))
    sweep_timeout_seconds = max(1, int(ctx.sweep_timeout_seconds))
    args = [
        "--job-timeout-seconds",
        str(job_timeout_seconds),
        "sweep",
        "run",
        "--profile-ref",
        profile_ref,
        "--output-dir",
        str(out_dir),
        "--quality-score-preset",
        ctx.config.research.quality_score_preset,
        "--mode",
        mode,
    ]
    if as_of_date:
        args.extend(["--as-of-date", str(as_of_date)])
    args.extend(["--timeout-seconds", str(sweep_timeout_seconds)])
    args.extend(_reward_matrix_cli_args(sweep_reward_matrix))
    args.extend(execution_cost_cli_args(ctx.config))
    if evolutionary_settings is not None:
        args.extend(
            [
                "--population-size",
                str(evolutionary_settings["population_size"]),
                "--max-generations",
                str(evolutionary_settings["max_generations"]),
            ]
        )
    for instrument in instruments:
        args.extend(["--instrument", instrument])
    for axis in axes:
        args.extend(["--axis", axis])
    if selected_permutations > SWEEP_PERMUTATION_HARD_LIMIT:
        args.append("--allow-large-sweep")
    args.append("--pretty")
    planned_work_count = evaluation_budget if evaluation_budget is not None else selected_permutations
    planned_work_label = "evolutionary evaluations" if evaluation_budget is not None else "permutations"
    live_sweep_id: str | None = None

    def heartbeat(elapsed: float, stdout_snapshot: str = "", stderr_snapshot: str = "") -> None:
        nonlocal live_sweep_id
        elapsed_text = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        detected_sweep_id = _sweep_id_from_stderr(stderr_snapshot)
        if detected_sweep_id:
            live_sweep_id = detected_sweep_id
        progress = _fetch_sweep_progress(
            ctx.cli,
            live_sweep_id,
            fallback_mode=mode,
        ) or _sweep_progress_from_cli_stderr(stderr_snapshot, fallback_mode=mode)
        progress_text = (
            str(progress.get("display"))
            if isinstance(progress, dict) and progress.get("display")
            else f"{planned_work_count} planned {planned_work_label}"
        )
        ctx.events_path.parent.mkdir(parents=True, exist_ok=True)
        with ctx.events_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "run_id": ctx.run_id,
                        "phase": phase,
                        "status": "running",
                        **stage.event_payload(),
                        "elapsed_seconds": round(elapsed, 1),
                        "permutation_count": selected_permutations,
                        "evaluation_budget": evaluation_budget,
                        "sweep_id": live_sweep_id,
                        "progress_percent": progress.get("percent") if isinstance(progress, dict) else None,
                        "progress_completed": progress.get("completed") if isinstance(progress, dict) else None,
                        "progress_total": progress.get("total") if isinstance(progress, dict) else None,
                        "progress_failed": progress.get("failed") if isinstance(progress, dict) else None,
                        "progress_generation": progress.get("generation") if isinstance(progress, dict) else None,
                        "progress_max_generations": progress.get("max_generations") if isinstance(progress, dict) else None,
                        "progress_best_fitness": progress.get("best_fitness") if isinstance(progress, dict) else None,
                        "progress_text": progress_text,
                        "reward_matrix": sweep_reward_matrix,
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )
        console.print(
            f"{stage.prefix} [cyan]{phase}[/] [bold]running[/] "
            f"{progress_text} elapsed={elapsed_text}"
        )

    result = ctx.cli.run_with_heartbeat(
        args,
        timeout_seconds=_play_hand_sweep_cli_timeout_seconds(sweep_timeout_seconds),
        heartbeat_seconds=30,
        heartbeat_snapshot=heartbeat,
    )
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else _load_json(out_dir / "sweep-results.json")
    _normalize_sweep_payload(payload, requested_axes=axes, definition_path=out_dir / "sweep-definition.json")
    _append_event(
        ctx,
        phase,
        "swept",
        stage=stage,
        artifact_dir=str(out_dir),
        axes=axes,
        axis_plan_path=str(out_dir / "sweep-axis-plan.json"),
        axis_plans=axis_plan.axis_plans,
        anchored_axes=axis_plan.anchored_axes,
        dropped_axes=axis_plan.dropped_axes,
        mode=mode,
        sweep_budget=sweep_budget,
        permutation_count=selected_permutations,
        search_space_permutations=selected_permutations,
        evaluation_budget=evaluation_budget,
        population_size=evolutionary_settings["population_size"] if evolutionary_settings is not None else None,
        max_generations=evolutionary_settings["max_generations"] if evolutionary_settings is not None else None,
        max_permutations=max_permutations,
        allow_large_sweep=selected_permutations > SWEEP_PERMUTATION_HARD_LIMIT,
        top_score=_top_sweep_score(payload),
        reward_matrix=sweep_reward_matrix,
    )
    return {"artifact_dir": str(out_dir), "result": payload, "axes": axes}


def _sweep_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _axis_map_from_definition(definition_path: Path, requested_axes: list[str]) -> dict[str, str]:
    definition = _load_json(definition_path)
    definition_axes = definition.get("axes")
    if not isinstance(definition_axes, list):
        return {}
    axis_map: dict[str, str] = {}
    for requested_axis, axis_info in zip(requested_axes, definition_axes):
        if not isinstance(axis_info, dict):
            continue
        instance_id = str(axis_info.get("indicator_instance_id") or "").strip()
        param_key = str(axis_info.get("param_key") or "").strip()
        if instance_id and param_key:
            axis_map[f"{instance_id}.{param_key}"] = requested_axis.partition("=")[0]
    return axis_map


def _normalize_sweep_payload(
    payload: dict[str, Any],
    *,
    requested_axes: list[str],
    definition_path: Path,
) -> None:
    data = _sweep_data(payload)
    axis_map = _axis_map_from_definition(definition_path, requested_axes)
    if not axis_map:
        return

    importance = data.get("parameter_importance")
    if isinstance(importance, list):
        for item in importance:
            if not isinstance(item, dict):
                continue
            axis = str(item.get("axis") or "").strip()
            mapped = axis_map.get(axis)
            if mapped:
                item["backend_axis"] = axis
                item["axis"] = mapped

    ranked = data.get("ranked_permutations") or data.get("ranked") or []
    if isinstance(ranked, list):
        for item in ranked:
            if not isinstance(item, dict):
                continue
            params = item.get("parameters")
            if not isinstance(params, dict):
                continue
            mapped_params: dict[str, Any] = {}
            for key, value in params.items():
                mapped_params[axis_map.get(str(key), str(key))] = value
            if mapped_params != params:
                item["backend_parameters"] = dict(params)
                item["parameters"] = mapped_params


def _top_sweep_score(payload: dict[str, Any]) -> float | None:
    data = _sweep_data(payload)
    ranked = data.get("ranked_permutations") or data.get("ranked") or []
    if isinstance(ranked, list) and ranked:
        first = ranked[0]
        if isinstance(first, dict):
            value = first.get("fitness_value") or first.get("score_lab") or first.get("score")
            if value is None and isinstance(first.get("fitness"), dict):
                fitness = first["fitness"]
                metric = str(data.get("fitness_metric") or "score_lab")
                value = fitness.get(metric) or fitness.get("score_lab") or fitness.get("quality_score")
            if isinstance(value, (int, float)):
                return float(value)
    return None


def _best_sweep_parameters(payload: dict[str, Any]) -> dict[str, Any]:
    candidates = _sweep_parameter_candidates(payload)
    return dict(candidates[0]) if candidates else {}


def _sweep_parameter_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = _sweep_data(payload)
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(parameters: Any) -> None:
        if not isinstance(parameters, dict) or not parameters:
            return
        candidate = dict(parameters)
        signature = json.dumps(candidate, sort_keys=True, default=str)
        if signature in seen:
            return
        seen.add(signature)
        candidates.append(candidate)

    ranked = data.get("ranked_permutations") or data.get("ranked") or []
    if isinstance(ranked, list):
        for item in ranked:
            if isinstance(item, dict):
                add(item.get("parameters"))
    best = data.get("best")
    if isinstance(best, dict):
        add(best.get("parameters"))
    return candidates


def _parameter_importance(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = _sweep_data(payload)
    value = data.get("parameter_importance")
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _materialize_and_register(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    source_profile_path: Path,
    parameters: dict[str, Any],
    phase: str,
    candidate_rank: int = 1,
) -> tuple[Path, str]:
    output_path = ctx.profiles_dir / f"{phase}_top.json"
    materialize_profile_variant(
        source_profile_path,
        output_path,
        parameters,
        name_suffix=f"[{phase} top]",
    )
    profile_ref = _register_profile(ctx, output_path)
    _append_event(
        ctx,
        phase,
        "materialized",
        stage=stage,
        profile_path=str(output_path),
        profile_ref=profile_ref,
        parameters=parameters,
        candidate_rank=candidate_rank,
    )
    return output_path, profile_ref


def _copy_and_register_profile(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    source_profile_path: Path,
    output_name: str,
    phase: str,
) -> tuple[Path, str]:
    output_path = ctx.profiles_dir / output_name
    payload = _load_json(source_profile_path)
    profile = _extract_profile(payload)
    if profile:
        original_name = str(profile.get("name") or source_profile_path.stem)
        profile["name"] = f"{original_name} [exact template]".strip()
        profile["isActive"] = False
        _write_json(output_path, payload)
    else:
        shutil.copyfile(source_profile_path, output_path)
    profile_ref = _register_profile(ctx, output_path)
    _append_event(
        ctx,
        phase,
        "registered",
        stage=stage,
        profile_path=str(output_path),
        profile_ref=profile_ref,
    )
    return output_path, profile_ref


def _seed_template_profile_path(template: dict[str, Any]) -> Path | None:
    for key in ("profile_path", "source_profile_path", "recommended_profile_template_path"):
        raw_path = str(template.get(key) or "").strip()
        if not raw_path:
            continue
        candidate = Path(raw_path)
        candidates = [candidate]
        if not candidate.is_absolute():
            candidates.append(Path.cwd() / candidate)
        for path in candidates:
            if path.exists() and path.is_file():
                return path
    return None


def _materialize_and_register_best_sweep_candidate(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    source_profile_path: Path,
    sweep_payload: dict[str, Any],
    phase: str,
) -> tuple[Path, str, dict[str, Any]] | None:
    candidates = _sweep_parameter_candidates(sweep_payload)
    for rank, parameters in enumerate(candidates, start=1):
        try:
            output_path, profile_ref = _materialize_and_register(
                ctx,
                stage=stage,
                source_profile_path=source_profile_path,
                parameters=parameters,
                phase=phase,
                candidate_rank=rank,
            )
        except CliError as exc:
            _append_event(
                ctx,
                phase,
                "materialize_rejected",
                stage=stage,
                candidate_rank=rank,
                parameters=parameters,
                error=str(exc)[:2000],
            )
            continue
        return output_path, profile_ref, parameters
    return None


def _render_phase_table(rows: list[dict[str, Any]]) -> None:
    table = Table(title="Play Hand v1", show_lines=False)
    table.add_column("Phase")
    table.add_column("Status")
    table.add_column("Score", justify="right")
    table.add_column("Detail")
    for row in rows:
        score = row.get("score")
        score_text = f"{score:.2f}" if isinstance(score, (int, float)) else ""
        table.add_row(str(row.get("phase") or ""), str(row.get("status") or ""), score_text, str(row.get("detail") or ""))
    console.print(table)


def _play_hand_artifact_commands(
    *,
    run_id: str,
    profile_drop_count: int,
    profile_drop_workers: int,
    final_attempt_id: str | None = None,
) -> list[list[str]]:
    drop_count = max(0, int(profile_drop_count))
    drop_workers = max(1, int(profile_drop_workers))
    if drop_count <= 0:
        return []
    command = [
        "uv",
        "run",
        "finalize-corpus",
        "--run-id",
        run_id,
        "--lookback-months",
        "36",
        "--profile-drop-workers",
        str(drop_workers),
        "--json",
    ]
    if final_attempt_id:
        command.extend(["--attempt-id", str(final_attempt_id)])
    else:
        command.extend(["--scope", "dashboard"])
    return [command]


def _json_payload_from_stdout(stdout: str) -> dict[str, Any]:
    text = str(stdout or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {"value": payload}
    except json.JSONDecodeError:
        start = text.rfind("\n{")
        if start >= 0:
            start += 1
        else:
            start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end >= start:
            try:
                payload = json.loads(text[start : end + 1])
                return payload if isinstance(payload, dict) else {"value": payload}
            except json.JSONDecodeError:
                pass
    return {"raw_stdout": text[-4000:]}


def _run_child_public_command(args: list[str], *, cwd: Path) -> dict[str, Any]:
    result = subprocess.run(
        args,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    payload = _json_payload_from_stdout(result.stdout)
    payload["_returncode"] = result.returncode
    if result.stderr.strip():
        payload["_stderr"] = result.stderr.strip()[-4000:]
    if result.returncode != 0:
        rendered = " ".join(args)
        raise RuntimeError(
            f"Child command failed ({result.returncode}): {rendered}"
            + (f"\n{payload.get('_stderr')}" if payload.get("_stderr") else "")
        )
    return payload


def _finalize_run_artifacts(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    profile_drop_count: int,
    profile_drop_workers: int,
    final_attempt_id: str | None = None,
) -> dict[str, Any]:
    if ctx.dry_run:
        payload = {
            "status": "skipped",
            "reason": "dry_run",
            "full_backtests": None,
            "profile_drops": None,
        }
        _append_event(ctx, "final_artifacts", "skipped", stage=stage, reason="dry_run")
        return payload

    commands = _play_hand_artifact_commands(
        run_id=ctx.run_id,
        profile_drop_count=profile_drop_count,
        profile_drop_workers=profile_drop_workers,
        final_attempt_id=final_attempt_id,
    )
    full_backtests: dict[str, Any] | None = None
    profile_drops: dict[str, Any] | None = None
    _append_event(
        ctx,
        "final_artifacts",
        "started",
        stage=stage,
        profile_drop_count=max(0, int(profile_drop_count)),
        profile_drop_workers=max(1, int(profile_drop_workers)),
        final_attempt_id=final_attempt_id,
    )
    try:
        if commands:
            console.print(f"{stage.prefix} [cyan]final_artifacts[/] [bold]finalize corpus[/]")
            full_backtests = _run_child_public_command(
                commands[0], cwd=ctx.config.repo_root
            )
            _append_event(
                ctx,
                "final_artifacts",
                "full_backtests_done",
                stage=stage,
                selected_count=full_backtests.get("selected_count"),
                render_exit_code=full_backtests.get("render_exit_code"),
                finalize_status=full_backtests.get("status"),
            )
            if isinstance(full_backtests.get("render_summary"), dict):
                profile_drops = dict(full_backtests["render_summary"])
            _append_event(
                ctx,
                "final_artifacts",
                "profile_drops_done",
                stage=stage,
                selected_count=profile_drops.get("selected_count") if profile_drops else None,
                rendered=profile_drops.get("profile_drop_rendered") if profile_drops else None,
                cached=profile_drops.get("profile_drop_cached") if profile_drops else None,
                skipped=profile_drops.get("profile_drop_skipped") if profile_drops else None,
                failed=profile_drops.get("profile_drop_failed") if profile_drops else None,
                final_attempt_id=final_attempt_id,
            )
        return {
            "status": "completed",
            "full_backtests": full_backtests,
            "profile_drops": profile_drops,
        }
    except Exception as exc:
        _append_event(ctx, "final_artifacts", "failed", stage=stage, error=str(exc))
        return {
            "status": "failed",
            "error": str(exc),
            "full_backtests": full_backtests,
            "profile_drops": profile_drops,
        }


def cmd_play_hand(
    *,
    instrument: list[str] | None = None,
    instrument_pool: list[str] | None = None,
    timeframe: str,
    sweep_budget: str | None,
    max_sweep_permutations: int | None,
    max_reward_r: float | None,
    min_indicators: int,
    max_indicators: int,
    seed: int | None,
    screen_months: int,
    scrutiny_months: int,
    coarse_mode: str,
    evolutionary_budget: str | None,
    instrument_scout: bool,
    instrument_scout_size: int,
    instrument_scout_max_selected: int,
    instrument_scout_months: int | None,
    final_artifacts: bool,
    final_profile_drop_count: int,
    final_profile_drop_workers: int,
    job_timeout_seconds: int,
    sweep_timeout_seconds: int,
    dry_run: bool,
    as_json: bool,
    calendar_gate: str | None = None,
    screen_anchor_mode: str | None = None,
    early_exit_mode: str | None = None,
    coarse_halving_mode: str | None = None,
    family_policy_mode: str | None = None,
    coarse_probe_budget: int | None = PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET,
    screen_anchor_max_offset_months: int = PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MAX_OFFSET_MONTHS,
    keep_cloud_profiles: bool = False,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    job_timeout_seconds = max(1, int(job_timeout_seconds))
    sweep_timeout_seconds = max(1, int(sweep_timeout_seconds))
    run_id = f"{_utc_stamp()}-playhand-v1"
    run_dir = config.runs_root / run_id
    profiles_dir = run_dir / "profiles"
    evals_dir = run_dir / "evals"
    ctx = PlayHandContext(
        config=config,
        cli=cli,
        run_id=run_id,
        run_dir=run_dir,
        profiles_dir=profiles_dir,
        evals_dir=evals_dir,
        attempts_path=attempts_path_for_run_dir(run_dir),
        events_path=run_dir / "play-hand-events.jsonl",
        summary_path=run_dir / "play-hand-summary.json",
        dry_run=dry_run,
        job_timeout_seconds=job_timeout_seconds,
        sweep_timeout_seconds=sweep_timeout_seconds,
    )
    profiles_dir.mkdir(parents=True, exist_ok=True)
    evals_dir.mkdir(parents=True, exist_ok=True)

    if not dry_run:
        cli.ensure_login()

    hand = _seed_hand(config, cli, run_dir) if not dry_run else [
        SeedIndicator("RSI_CROSSBACK", "trigger", "event-with-lookback", "entry"),
        SeedIndicator("STOCH_CROSSOVER", "trigger", "event-with-lookback", "entry"),
        SeedIndicator("MA_SLOPE_TREND", "context", "state", "higher-context"),
        SeedIndicator("ADX", "filter", "state", "higher-context"),
        SeedIndicator("WICK_REJECTION", "trigger", "event-with-lookback", "entry"),
    ]
    rng = random.Random(seed)
    shuffled = list(hand)
    rng.shuffle(shuffled)
    seed_plan, seed_plan_path = _load_play_hand_seed_plan(config)
    seed_plan_candidates = _seed_plan_indicator_candidates(config, seed_plan)
    guided_available_count = len(
        _merge_seed_indicator_candidates(shuffled, seed_plan_candidates)
    )
    effective_min_indicators = min_indicators
    effective_max_indicators = max_indicators
    if seed_plan is not None:
        effective_min_indicators = max(effective_min_indicators, 2)
        effective_max_indicators = max(effective_max_indicators, effective_min_indicators)
    dealt_count = deal_indicator_count(
        available_count=max(len(shuffled), guided_available_count),
        min_indicators=effective_min_indicators,
        max_indicators=effective_max_indicators,
        rng=rng,
    )
    indicator_deal = deal_seed_plan_indicators(
        shuffled,
        target_count=dealt_count,
        seed_plan=seed_plan,
        rng=rng,
        seed_plan_candidates=seed_plan_candidates,
    )
    dealt_entries = list(indicator_deal.get("indicators") or [])
    if not dealt_entries:
        indicator_deal = _fallback_indicator_deal(
            shuffled,
            target_count=dealt_count,
            source="role_balanced",
            reason="empty_guided_deal",
        )
        dealt_entries = list(indicator_deal.get("indicators") or [])
    dealt = [indicator.id for indicator in dealt_entries]
    template_instrument_policy = _seed_plan_template_instrument_policy(seed_plan)
    template_instrument_pool = _seed_pair_template_instruments(indicator_deal.get("pair"))
    effective_instrument_pool = instrument_pool
    template_instrument_pool_applied = False
    if (
        template_instrument_policy == "seed_pool"
        and template_instrument_pool
        and not _clean_tokens(instrument)
        and not _clean_tokens(instrument_pool)
    ):
        effective_instrument_pool = template_instrument_pool
        template_instrument_pool_applied = True
    instrument_deal = deal_instruments(
        instrument=instrument,
        instrument_pool=effective_instrument_pool,
        rng=rng,
    )
    instruments = list(instrument_deal["instruments"])
    timeframe = str(timeframe or "M5").strip().upper() or "M5"
    budget = resolve_sweep_budget(
        sweep_budget=sweep_budget,
        max_sweep_permutations=max_sweep_permutations,
        evolutionary_budget=evolutionary_budget,
    )
    sweep_budget_label = str(budget["label"])
    sweep_budget_value = int(budget["value"])
    max_sweep_permutations = sweep_budget_value
    reward_matrix = play_hand_reward_matrix(max_reward_r)
    calendar_gate_mode = _resolve_calendar_gate_mode(calendar_gate)
    early_exit_mode = _resolve_early_exit_mode(early_exit_mode)
    coarse_halving_mode = _resolve_coarse_halving_mode(coarse_halving_mode)
    family_policy_mode = _resolve_family_policy_mode(family_policy_mode)
    coarse_probe_budget = _resolve_coarse_probe_budget(coarse_probe_budget)
    screen_anchor = sample_screen_anchor(
        mode=_resolve_screen_anchor_mode(screen_anchor_mode),
        screen_months=screen_months,
        max_offset_months=screen_anchor_max_offset_months,
        seed=seed,
    )
    screen_as_of_date = screen_anchor.get("as_of_date")
    resolved_family_policy = resolve_playhand_family_policy(indicator_deal)

    metadata = {
        "run_id": run_id,
        "runner": "play_hand_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "canonical_score_lab_version": "score_lab_v2_5_3",
        "seed": seed,
        "instrument_source": instrument_deal["source"],
        "primary_instrument": instrument_deal["primary_instrument"],
        "instrument_pool": instrument_deal["instrument_pool"],
        "instruments": instruments,
        "timeframe": timeframe,
        "requested_timeframe": timeframe,
        "effective_timeframe": timeframe,
        "dealt_indicator_ids": dealt,
        "dealt_indicator_metadata": [
            indicator.as_metadata() for indicator in dealt_entries
        ],
        "dealt_indicator_source": indicator_deal.get("source"),
        "dealt_indicator_source_reason": indicator_deal.get("reason"),
        "play_hand_seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
        "play_hand_seed_plan_loaded": seed_plan is not None,
        "template_instrument_policy": template_instrument_policy,
        "template_instrument_pool": template_instrument_pool,
        "template_instrument_pool_applied": template_instrument_pool_applied,
        "dealt_recipe": indicator_deal.get("recipe"),
        "dealt_recipe_source": indicator_deal.get("recipe_source"),
        "dealt_recipe_confidence": indicator_deal.get("recipe_confidence"),
        "guided_recipe_source_mix_expected": indicator_deal.get(
            "guided_recipe_source_mix_expected"
        ),
        "guided_recipe_source_bucket": indicator_deal.get("guided_recipe_source_bucket"),
        "guided_recipe_source_bucket_matched": indicator_deal.get(
            "guided_recipe_source_bucket_matched"
        ),
        "guided_recipe_source_bucket_fallback": indicator_deal.get(
            "guided_recipe_source_bucket_fallback"
        ),
        "dealt_recipe_pair": indicator_deal.get("pair"),
        "dealt_pair_family_policy": indicator_deal.get("family_policy"),
        "dealt_policy_target_count": indicator_deal.get("policy_target_count"),
        "dealt_recipe_slots": indicator_deal.get("selected_slots"),
        "dealt_indicator_count": len(dealt),
        "min_indicators": effective_min_indicators,
        "max_indicators": effective_max_indicators,
        "requested_min_indicators": min_indicators,
        "requested_max_indicators": max_indicators,
        "screen_months": screen_months,
        "scrutiny_months": scrutiny_months,
        "screen_anchor": screen_anchor,
        "screen_as_of_date": screen_as_of_date,
        "coarse_mode": coarse_mode,
        "sweep_budget": sweep_budget_label,
        "sweep_budget_tier": budget.get("tier"),
        "sweep_budget_value": sweep_budget_value,
        "sweep_budget_source": budget.get("source"),
        "legacy_evolutionary_budget": evolutionary_budget,
        "max_reward_r": reward_matrix.get("requested_max_reward_r") if reward_matrix else max_reward_r,
        "reward_matrix": reward_matrix,
        "reward_step_r": reward_matrix.get("reward_step_r") if reward_matrix else None,
        "reward_columns": reward_matrix.get("reward_columns") if reward_matrix else None,
        "effective_max_reward_r": reward_matrix.get("effective_max_reward_r") if reward_matrix else None,
        "instrument_scout": bool(instrument_scout),
        "instrument_scout_size": int(instrument_scout_size),
        "instrument_scout_max_selected": int(instrument_scout_max_selected),
        "instrument_scout_months": int(instrument_scout_months or screen_months),
        "final_artifacts": bool(final_artifacts),
        "final_profile_drop_count": int(final_profile_drop_count),
        "final_profile_drop_workers": int(final_profile_drop_workers),
        "deep_replay_job_timeout_seconds": job_timeout_seconds,
        "sweep_timeout_seconds": sweep_timeout_seconds,
        "max_sweep_permutations": max_sweep_permutations,
        "calendar_gate_mode": calendar_gate_mode,
        "early_exit_mode": early_exit_mode,
        "early_exit_policy": {
            "version": PLAY_HAND_EARLY_EXIT_VERSION,
            "mode": early_exit_mode,
            "decisions": [],
        },
        "coarse_halving_mode": coarse_halving_mode,
        "coarse_probe_budget": coarse_probe_budget,
        "coarse_halving": {
            "version": PLAY_HAND_COARSE_HALVING_VERSION,
            "mode": coarse_halving_mode,
            "probe_budget": coarse_probe_budget,
            "decisions": [],
        },
        "family_policy_mode": family_policy_mode,
        "family_policy_execution": build_family_policy_execution_state(
            mode=family_policy_mode,
            family_policy=resolved_family_policy,
            exact_template_available=False,
        ),
        "stage_acceptance_drop_tolerance": STAGE_ACCEPTANCE_DROP_TOLERANCE,
        "stage_acceptance_decisions": [],
        "play_hand_phase_scores": {},
        "dry_run": dry_run,
        "keep_cloud_profiles": keep_cloud_profiles,
    }
    write_run_metadata(run_dir, metadata)
    if screen_as_of_date:
        console.print(
            f"[bold]screen anchor[/]: random as-of [cyan]{screen_as_of_date}[/] "
            f"(offset {screen_anchor.get('offset_days')}d; scrutiny stays now-anchored)"
        )
    _render_dealt_hand(
        indicators=dealt,
        min_indicators=effective_min_indicators,
        max_indicators=effective_max_indicators,
        instrument_deal=instrument_deal,
        timeframe=timeframe,
        sweep_budget_label=sweep_budget_label,
        sweep_budget_value=sweep_budget_value,
        screen_months=screen_months,
        scrutiny_months=scrutiny_months,
        coarse_mode=coarse_mode,
        reward_matrix=reward_matrix,
    )
    stage_total = 9
    stages = {
        "deal": PlayHandStage(1, stage_total, "Deal hand"),
        "scaffold": PlayHandStage(2, stage_total, "Scaffold profile"),
        "baseline": PlayHandStage(3, stage_total, "Baseline screen"),
        "lookback": PlayHandStage(4, stage_total, "Lookback timing sweep"),
        "coarse": PlayHandStage(5, stage_total, "Coarse parameter sweep"),
        "focused": PlayHandStage(6, stage_total, "Focused refinement sweep"),
        "instrument_scout": PlayHandStage(7, stage_total, "Instrument scout"),
        "scrutiny": PlayHandStage(8, stage_total, "Final scrutiny"),
        "artifacts": PlayHandStage(9, stage_total, "Finalize artifacts"),
    }

    def _record_phase_score(phase_key: str, score: Any) -> None:
        phase_scores = metadata.setdefault("play_hand_phase_scores", {})
        if not isinstance(phase_scores, dict):
            phase_scores = {}
            metadata["play_hand_phase_scores"] = phase_scores
        phase_scores[phase_key] = _as_float(score)
        write_run_metadata(run_dir, metadata)

    def _report_early_exit(checkpoint: str, stage_key: str) -> dict[str, Any] | None:
        if early_exit_mode != "report":
            return None
        attempts = load_attempts(ctx.attempts_path)
        evidence = build_play_hand_evidence(run_metadata=metadata, attempts=attempts)
        decision = build_early_exit_decision(
            checkpoint=checkpoint,
            evidence=evidence,
            mode=early_exit_mode,
        )
        policy = metadata.setdefault(
            "early_exit_policy",
            {
                "version": PLAY_HAND_EARLY_EXIT_VERSION,
                "mode": early_exit_mode,
                "decisions": [],
            },
        )
        if not isinstance(policy, dict):
            policy = {
                "version": PLAY_HAND_EARLY_EXIT_VERSION,
                "mode": early_exit_mode,
                "decisions": [],
            }
            metadata["early_exit_policy"] = policy
        decisions = policy.setdefault("decisions", [])
        if not isinstance(decisions, list):
            decisions = []
            policy["decisions"] = decisions
        decisions.append(decision)
        write_run_metadata(run_dir, metadata)
        _append_event(
            ctx,
            "early_exit",
            "reported",
            stage=stages.get(stage_key),
            **decision,
        )
        return decision

    def _record_family_policy_execution(
        status: str,
        *,
        stage_key: str = "scaffold",
        **updates: Any,
    ) -> dict[str, Any]:
        state = metadata.setdefault(
            "family_policy_execution",
            build_family_policy_execution_state(
                mode=family_policy_mode,
                family_policy=resolved_family_policy,
                exact_template_available=bool(updates.get("exact_template_available")),
            ),
        )
        if not isinstance(state, dict):
            state = build_family_policy_execution_state(
                mode=family_policy_mode,
                family_policy=resolved_family_policy,
                exact_template_available=bool(updates.get("exact_template_available")),
            )
            metadata["family_policy_execution"] = state
        state.update(updates)
        decisions = state.setdefault("decisions", [])
        if not isinstance(decisions, list):
            decisions = []
            state["decisions"] = decisions
        decisions.append(
            {
                "status": status,
                "decision": state.get("decision"),
                "mutation_allowed": state.get("mutation_allowed"),
                "skipped_stages": list(state.get("skipped_stages") or []),
                "reasons": list(state.get("reasons") or []),
            }
        )
        write_run_metadata(run_dir, metadata)
        _append_event(
            ctx,
            "family_policy_execution",
            status,
            stage=stages.get(stage_key),
            **state,
        )
        return state

    cleanup_state = {"ran": False}

    def _run_play_hand_cloud_cleanup(reason: str = "process_exit") -> dict[str, Any]:
        if cleanup_state["ran"]:
            return {
                "status": "skipped",
                "reason": reason,
                "skip_reason": "already_ran",
                "keep_cloud_profiles": bool(keep_cloud_profiles),
                "attempted_count": 0,
                "deleted_count": 0,
                "failed_count": 0,
            }
        cleanup_state["ran"] = True
        return _cleanup_registered_profiles(
            ctx,
            keep_cloud_profiles=keep_cloud_profiles,
            reason=reason,
            stage=stages["artifacts"],
        )

    cleanup_registered_with_atexit = not dry_run and not keep_cloud_profiles
    if cleanup_registered_with_atexit:
        atexit.register(_run_play_hand_cloud_cleanup)

    _append_event(
        ctx,
        "deal",
        "dealt",
        stage=stages["deal"],
        indicators=dealt,
        indicator_metadata=[indicator.as_metadata() for indicator in dealt_entries],
        indicator_deal_source=indicator_deal.get("source"),
        indicator_deal_reason=indicator_deal.get("reason"),
        play_hand_seed_plan_path=str(seed_plan_path) if seed_plan_path else None,
        dealt_recipe=indicator_deal.get("recipe"),
        dealt_recipe_source=indicator_deal.get("recipe_source"),
        guided_recipe_source_mix_expected=indicator_deal.get(
            "guided_recipe_source_mix_expected"
        ),
        guided_recipe_source_bucket=indicator_deal.get("guided_recipe_source_bucket"),
        guided_recipe_source_bucket_matched=indicator_deal.get(
            "guided_recipe_source_bucket_matched"
        ),
        guided_recipe_source_bucket_fallback=indicator_deal.get(
            "guided_recipe_source_bucket_fallback"
        ),
        dealt_recipe_pair=indicator_deal.get("pair"),
        dealt_pair_family_policy=indicator_deal.get("family_policy"),
        dealt_policy_target_count=indicator_deal.get("policy_target_count"),
        dealt_recipe_slots=indicator_deal.get("selected_slots"),
        dealt_indicator_count=len(dealt),
        min_indicators=effective_min_indicators,
        max_indicators=effective_max_indicators,
        requested_min_indicators=min_indicators,
        requested_max_indicators=max_indicators,
        instrument_source=instrument_deal["source"],
        primary_instrument=instrument_deal["primary_instrument"],
        instrument_pool=instrument_deal["instrument_pool"],
        instruments=instruments,
        timeframe=timeframe,
        seed=seed,
        reward_matrix=reward_matrix,
    )

    phase_rows: list[dict[str, Any]] = []
    profile_path = _scaffold_profile(ctx, dealt, instruments, timeframe, "hand_base")
    profile_payload = _load_json(profile_path)
    metadata_changes = apply_seed_indicator_metadata(profile_payload, dealt_entries)
    timeframe_changes = apply_role_timeframe_defaults(profile_payload, rng=rng)
    template_changes = apply_seed_pair_template_defaults(profile_payload, indicator_deal.get("pair"))
    default_changes = apply_play_hand_profile_defaults(profile_payload, rng=rng)
    if metadata_changes or timeframe_changes or template_changes or default_changes:
        _write_json(profile_path, profile_payload)
    if metadata_changes:
        _append_event(
            ctx,
            "scaffold",
            "metadata_applied",
            stage=stages["scaffold"],
            profile_path=str(profile_path),
            changes=metadata_changes,
        )
    if timeframe_changes:
        _append_event(
            ctx,
            "scaffold",
            "role_timeframes_applied",
            stage=stages["scaffold"],
            profile_path=str(profile_path),
            changes=timeframe_changes,
        )
        metadata["indicator_timeframes"] = _profile_timeframes(profile_payload)
        metadata["timeframe_assignment"] = timeframe_changes
        metadata["effective_timeframe"] = _lowest_profile_timeframe(
            profile_payload,
            timeframe,
        )
        metadata["timeframe"] = metadata["effective_timeframe"]
        write_run_metadata(run_dir, metadata)
    if template_changes:
        _append_event(
            ctx,
            "scaffold",
            "validated_template_applied",
            stage=stages["scaffold"],
            profile_path=str(profile_path),
            changes=template_changes,
        )
        metadata["validated_template_defaults"] = template_changes
        metadata["indicator_timeframes"] = _profile_timeframes(profile_payload)
        metadata["effective_timeframe"] = _lowest_profile_timeframe(
            profile_payload,
            timeframe,
        )
        metadata["timeframe"] = metadata["effective_timeframe"]
        write_run_metadata(run_dir, metadata)
    if default_changes:
        _append_event(
            ctx,
            "scaffold",
            "defaults_applied",
            stage=stages["scaffold"],
            profile_path=str(profile_path),
            changes=default_changes,
        )
    evaluation_timeframe = _lowest_profile_timeframe(profile_payload, timeframe)
    pair_evidence = _seed_plan_dict(indicator_deal.get("pair"))
    pair_template = _seed_plan_dict(pair_evidence.get("recommended_profile_template"))
    template_branch_source_probe_id = (
        str(
            pair_template.get("probe_id")
            or pair_template.get("source_probe_id")
            or pair_evidence.get("probe_id")
            or pair_evidence.get("source_probe_id")
            or ""
        ).strip()
        or None
    )
    template_branch_instruments = (
        list(template_instrument_pool) if template_instrument_pool else list(instruments)
    )
    exact_template_source_profile_path = _seed_template_profile_path(pair_template)
    exact_template_source = (
        "template_profile_path"
        if exact_template_source_profile_path is not None
        else "post_template_scaffold_fallback"
    )
    profile_ref = _register_profile(ctx, profile_path)
    _append_event(
        ctx,
        "scaffold",
        "registered",
        stage=stages["scaffold"],
        profile_path=str(profile_path),
        profile_ref=profile_ref,
    )
    exact_template_profile_path: Path | None = None
    exact_template_profile_ref: str | None = None
    exact_template_timeframe: str | None = None
    if pair_template.get("indicator_defaults"):
        exact_template_copy_source = exact_template_source_profile_path or profile_path
        exact_template_profile_path, exact_template_profile_ref = _copy_and_register_profile(
            ctx,
            stage=stages["scaffold"],
            source_profile_path=exact_template_copy_source,
            output_name="exact_template.json",
            phase="exact_template",
        )
        exact_template_timeframe = str(
            pair_template.get("timeframe") or evaluation_timeframe
        ).strip().upper() or evaluation_timeframe
        metadata.update(
            {
                "exact_template_profile_ref": exact_template_profile_ref,
                "exact_template_profile_path": str(exact_template_profile_path.resolve()),
                "exact_template_source": exact_template_source,
                "exact_template_source_profile_path": str(exact_template_copy_source.resolve()),
                "template_branch_instruments": template_branch_instruments,
                "template_branch_source_probe_id": template_branch_source_probe_id,
            }
        )
        write_run_metadata(run_dir, metadata)
    family_policy_execution = build_family_policy_execution_state(
        mode=family_policy_mode,
        family_policy=resolved_family_policy,
        exact_template_available=exact_template_profile_path is not None
        and exact_template_profile_ref is not None,
    )
    metadata["family_policy_execution"] = family_policy_execution
    _record_family_policy_execution(
        "resolved",
        exact_template_available=family_policy_execution["exact_template_available"],
        decision=family_policy_execution["decision"],
        mutation_allowed=family_policy_execution["mutation_allowed"],
        skipped_stages=family_policy_execution["skipped_stages"],
        reasons=family_policy_execution["reasons"],
        would_reduce_mutation_pressure=family_policy_execution.get(
            "would_reduce_mutation_pressure",
            False,
        ),
        would_require_extra_screen=family_policy_execution.get(
            "would_require_extra_screen",
            False,
        ),
    )

    baseline = _evaluate_profile(
        ctx,
        stage=stages["baseline"],
        phase="baseline_3mo",
        profile_ref=profile_ref,
        profile_path=profile_path,
        instruments=instruments,
        timeframe=evaluation_timeframe,
        lookback_months=screen_months,
        reward_matrix=reward_matrix,
        as_of_date=screen_as_of_date,
    )
    phase_rows.append({"phase": "baseline", "status": "evaluated", "score": baseline.get("score"), "detail": profile_ref})
    _record_phase_score("baseline", baseline.get("score"))
    _report_early_exit("after_baseline", "baseline")

    current_profile_path = profile_path
    current_profile_ref = profile_ref
    current_evaluation_timeframe = evaluation_timeframe
    last_sweep_payload: dict[str, Any] | None = None
    last_sweep_axes: list[str] = []
    skip_mutation_pipeline = False
    skip_focused_and_scout = False
    family_policy_name = str(resolved_family_policy.get("family_policy") or "none")
    stage_acceptance_enabled = coarse_halving_mode == "enforce" or (
        family_policy_mode == "enforce"
        and family_policy_name == "template_guarded"
    )
    incumbent: dict[str, Any] = {
        "profile_path": current_profile_path,
        "profile_ref": current_profile_ref,
        "evaluation_timeframe": current_evaluation_timeframe,
        "score": _as_float(baseline.get("score")),
        "phase": "baseline_3mo",
    }

    def _public_incumbent(snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
        source = snapshot or incumbent
        path = source.get("profile_path")
        return {
            "profile_path": str(path.resolve() if isinstance(path, Path) else path),
            "profile_ref": source.get("profile_ref"),
            "evaluation_timeframe": source.get("evaluation_timeframe"),
            "score": _as_float(source.get("score")),
            "phase": source.get("phase"),
        }

    metadata["stage_incumbent"] = _public_incumbent()
    write_run_metadata(run_dir, metadata)

    def _apply_stage_candidate(
        *,
        stage_key: str,
        phase: str,
        candidate_profile_path: Path,
        candidate_profile_ref: str,
        candidate_timeframe: str,
        candidate_score: Any,
        detail: str,
        phase_score_key: str,
    ) -> dict[str, Any]:
        nonlocal current_profile_path
        nonlocal current_profile_ref
        nonlocal current_evaluation_timeframe
        nonlocal incumbent

        decision = build_stage_acceptance_decision(
            stage=stage_key,
            incumbent_score=incumbent.get("score"),
            candidate_score=candidate_score,
        )
        decision.update(
            {
                "phase": phase,
                "candidate_profile_path": str(candidate_profile_path.resolve()),
                "candidate_profile_ref": candidate_profile_ref,
                "candidate_evaluation_timeframe": candidate_timeframe,
                "incumbent_profile_path": _public_incumbent().get("profile_path"),
                "incumbent_profile_ref": incumbent.get("profile_ref"),
                "incumbent_phase": incumbent.get("phase"),
            }
        )
        decisions = metadata.setdefault("stage_acceptance_decisions", [])
        if not isinstance(decisions, list):
            decisions = []
            metadata["stage_acceptance_decisions"] = decisions
        decisions.append(decision)
        if decision["accepted"]:
            incumbent = {
                "profile_path": candidate_profile_path,
                "profile_ref": candidate_profile_ref,
                "evaluation_timeframe": candidate_timeframe,
                "score": _as_float(candidate_score),
                "phase": phase_score_key,
            }
            current_profile_path = candidate_profile_path
            current_profile_ref = candidate_profile_ref
            current_evaluation_timeframe = candidate_timeframe
        else:
            current_profile_path = incumbent["profile_path"]
            current_profile_ref = str(incumbent.get("profile_ref") or "")
            current_evaluation_timeframe = str(
                incumbent.get("evaluation_timeframe") or evaluation_timeframe
            )
        metadata["stage_incumbent"] = _public_incumbent()
        write_run_metadata(run_dir, metadata)
        event_decision = dict(decision)
        event_decision["acceptance_stage"] = event_decision.pop("stage", stage_key)
        event_decision["candidate_phase"] = event_decision.pop("phase", phase)
        _append_event(
            ctx,
            "stage_acceptance",
            "accepted" if decision["accepted"] else "rejected",
            stage=stages.get(stage_key),
            **event_decision,
        )
        phase_rows.append(
            {
                "phase": stage_key.replace("_", " "),
                "status": "top evaluated" if decision["accepted"] else "top rejected",
                "score": candidate_score,
                "detail": detail,
            }
        )
        _record_phase_score(phase_score_key, candidate_score)
        return decision

    def _record_coarse_halving_decision(decision: dict[str, Any]) -> None:
        policy = metadata.setdefault(
            "coarse_halving",
            {
                "version": PLAY_HAND_COARSE_HALVING_VERSION,
                "mode": coarse_halving_mode,
                "probe_budget": coarse_probe_budget,
                "decisions": [],
            },
        )
        if not isinstance(policy, dict):
            policy = {
                "version": PLAY_HAND_COARSE_HALVING_VERSION,
                "mode": coarse_halving_mode,
                "probe_budget": coarse_probe_budget,
                "decisions": [],
            }
            metadata["coarse_halving"] = policy
        decisions = policy.setdefault("decisions", [])
        if not isinstance(decisions, list):
            decisions = []
            policy["decisions"] = decisions
        decisions.append(dict(decision))
        policy.update({key: value for key, value in decision.items() if key != "decisions"})
        write_run_metadata(run_dir, metadata)
        _append_event(
            ctx,
            "coarse_halving",
            str(decision.get("decision") or "reported"),
            stage=stages["coarse"],
            **decision,
        )

    exact_template_screen: dict[str, Any] | None = None

    lookback_axes = build_timing_axes(profile_payload)
    should_screen_exact_template = (
        family_policy_mode in {"report", "enforce"}
        and family_policy_name in PLAY_HAND_FAMILY_POLICY_ACTIVE_POLICIES
        and exact_template_profile_path is not None
        and bool(exact_template_profile_ref)
    )
    if should_screen_exact_template:
        exact_template_screen = _evaluate_profile(
            ctx,
            stage=stages["baseline"],
            phase="exact_template_screen_3mo",
            profile_ref=str(exact_template_profile_ref),
            profile_path=exact_template_profile_path,
            instruments=template_branch_instruments,
            timeframe=exact_template_timeframe or evaluation_timeframe,
            lookback_months=screen_months,
            reward_matrix=reward_matrix,
            as_of_date=screen_as_of_date,
        )
        exact_template_screen_summary = {
            "attempt_id": str(exact_template_screen.get("attempt_id") or "") or None,
            "score": _as_float(exact_template_screen.get("score")),
            "profile_ref": exact_template_profile_ref,
            "profile_path": str(exact_template_profile_path.resolve()),
            "instruments": list(template_branch_instruments),
            "timeframe": exact_template_timeframe or evaluation_timeframe,
        }
        phase_rows.append(
            {
                "phase": "exact template screen",
                "status": "evaluated",
                "score": exact_template_screen.get("score"),
                "detail": family_policy_name,
            }
        )
        _record_phase_score("exact_template_screen_3mo", exact_template_screen.get("score"))
        exact_used_as_incumbent = False
        if family_policy_mode == "enforce" and family_policy_name == "template_locked":
            skip_mutation_pipeline = True
            current_profile_path = exact_template_profile_path
            current_profile_ref = str(exact_template_profile_ref)
            current_evaluation_timeframe = exact_template_timeframe or evaluation_timeframe
            incumbent = {
                "profile_path": current_profile_path,
                "profile_ref": current_profile_ref,
                "evaluation_timeframe": current_evaluation_timeframe,
                "score": _as_float(exact_template_screen.get("score")),
                "phase": "exact_template_screen_3mo",
            }
            metadata["stage_incumbent"] = _public_incumbent()
            write_run_metadata(run_dir, metadata)
            exact_used_as_incumbent = True
        elif family_policy_mode == "enforce" and family_policy_name == "template_guarded":
            acceptance = _apply_stage_candidate(
                stage_key="exact_template_screen",
                phase="exact_template_screen",
                candidate_profile_path=exact_template_profile_path,
                candidate_profile_ref=str(exact_template_profile_ref),
                candidate_timeframe=exact_template_timeframe or evaluation_timeframe,
                candidate_score=exact_template_screen.get("score"),
                detail="family policy exact benchmark",
                phase_score_key="exact_template_screen_3mo",
            )
            exact_used_as_incumbent = bool(acceptance.get("accepted"))
        _record_family_policy_execution(
            "enforced" if family_policy_mode == "enforce" else "reported",
            stage_key="baseline",
            exact_template_screen=exact_template_screen_summary,
            exact_template_used_as_incumbent=exact_used_as_incumbent,
            decision=(
                "template_locked_exact_only"
                if family_policy_mode == "enforce" and family_policy_name == "template_locked"
                else (
                    "template_guarded_exact_benchmark_mutation_allowed"
                    if family_policy_mode == "enforce" and family_policy_name == "template_guarded"
                    else metadata["family_policy_execution"].get("decision")
                )
            ),
            mutation_allowed=not skip_mutation_pipeline,
            skipped_stages=(
                [
                    "lookback_timing",
                    "coarse_probe",
                    "coarse_expand",
                    "focused",
                    "instrument_scout",
                    "mutated_final_36mo",
                ]
                if skip_mutation_pipeline
                else []
            ),
        )

    lookback_axes = [] if skip_mutation_pipeline else lookback_axes
    if skip_mutation_pipeline:
        _append_event(
            ctx,
            "lookback_timing",
            "skipped",
            stage=stages["lookback"],
            reason="family_policy_template_locked_exact_only",
        )
        phase_rows.append(
            {
                "phase": "lookback",
                "status": "skipped",
                "score": None,
                "detail": "family policy template-locked exact-only",
            }
        )
        _report_early_exit("after_lookback_top", "lookback")
    elif lookback_axes:
        sweep = _run_sweep(
            ctx,
            stage=stages["lookback"],
            phase="lookback_timing",
            profile_ref=current_profile_ref,
            profile_payload=profile_payload,
            instruments=instruments,
            axes=lookback_axes,
            mode="deterministic",
            sweep_budget=sweep_budget_label,
            max_permutations=max_sweep_permutations,
            reward_matrix=reward_matrix,
            as_of_date=screen_as_of_date,
        )
        last_sweep_payload = sweep["result"]
        last_sweep_axes = list(sweep.get("axes") or lookback_axes)
        source_profile_path = current_profile_path
        materialized = _materialize_and_register_best_sweep_candidate(
            ctx,
            stage=stages["lookback"],
            source_profile_path=source_profile_path,
            sweep_payload=last_sweep_payload,
            phase="lookback_timing",
        )
        if materialized:
            candidate_profile_path, candidate_profile_ref, _params = materialized
            candidate_timeframe = _lowest_profile_timeframe(
                _load_json(candidate_profile_path),
                timeframe,
            )
            if not stage_acceptance_enabled:
                current_profile_path = candidate_profile_path
                current_profile_ref = candidate_profile_ref
                current_evaluation_timeframe = candidate_timeframe
            result = _evaluate_profile(
                ctx,
                stage=stages["lookback"],
                phase="lookback_timing_top_3mo",
                profile_ref=candidate_profile_ref,
                profile_path=candidate_profile_path,
                instruments=instruments,
                timeframe=candidate_timeframe,
                lookback_months=screen_months,
                reward_matrix=reward_matrix,
                as_of_date=screen_as_of_date,
            )
            if stage_acceptance_enabled:
                _apply_stage_candidate(
                    stage_key="lookback",
                    phase="lookback_timing",
                    candidate_profile_path=candidate_profile_path,
                    candidate_profile_ref=candidate_profile_ref,
                    candidate_timeframe=candidate_timeframe,
                    candidate_score=result.get("score"),
                    detail=", ".join(lookback_axes),
                    phase_score_key="lookback_top_3mo",
                )
            else:
                phase_rows.append({"phase": "lookback", "status": "top evaluated", "score": result.get("score"), "detail": ", ".join(lookback_axes)})
                _record_phase_score("lookback_top_3mo", result.get("score"))
            _report_early_exit("after_lookback_top", "lookback")
    else:
        _append_event(ctx, "lookback_timing", "skipped", stage=stages["lookback"], reason="no active indicators available")
        _report_early_exit("after_lookback_top", "lookback")

    current_profile_payload = _load_json(current_profile_path)
    coarse_axes = build_coarse_axes(current_profile_payload)
    if skip_mutation_pipeline:
        _append_event(
            ctx,
            "coarse",
            "skipped",
            stage=stages["coarse"],
            reason="family_policy_template_locked_exact_only",
        )
        phase_rows.append(
            {
                "phase": "coarse",
                "status": "skipped",
                "score": None,
                "detail": "family policy template-locked exact-only",
            }
        )
        _record_phase_score("coarse_top_3mo", incumbent.get("score"))
        _report_early_exit("after_coarse_top", "coarse")
    elif coarse_axes:
        coarse_halving_plan = build_coarse_halving_budget_plan(
            mode=coarse_halving_mode,
            total_budget=max_sweep_permutations,
            probe_budget=coarse_probe_budget,
        )
        use_coarse_halving = (
            coarse_mode == "evolutionary"
            and coarse_halving_mode == "enforce"
            and bool(coarse_halving_plan.get("split"))
        )
        if use_coarse_halving:
            probe_budget = int(coarse_halving_plan["probe_budget"])
            expand_budget = int(coarse_halving_plan["expand_budget"])
            pre_probe_incumbent_score = incumbent.get("score")
            probe_source_profile_path = current_profile_path
            sweep = _run_sweep(
                ctx,
                stage=stages["coarse"],
                phase="coarse_probe",
                profile_ref=current_profile_ref,
                profile_payload=current_profile_payload,
                instruments=instruments,
                axes=coarse_axes,
                mode=coarse_mode,
                sweep_budget=str(probe_budget),
                max_permutations=probe_budget,
                reward_matrix=reward_matrix,
                as_of_date=screen_as_of_date,
            )
            last_sweep_payload = sweep["result"]
            last_sweep_axes = list(sweep.get("axes") or coarse_axes)
            probe_score = _top_sweep_score(last_sweep_payload)
            probe_candidate_path: Path | None = None
            probe_candidate_ref: str | None = None
            materialized = _materialize_and_register_best_sweep_candidate(
                ctx,
                stage=stages["coarse"],
                source_profile_path=probe_source_profile_path,
                sweep_payload=last_sweep_payload,
                phase="coarse_probe",
            )
            if materialized:
                probe_candidate_path, probe_candidate_ref, _params = materialized
                probe_timeframe = _lowest_profile_timeframe(
                    _load_json(probe_candidate_path),
                    timeframe,
                )
                result = _evaluate_profile(
                    ctx,
                    stage=stages["coarse"],
                    phase="coarse_probe_top_3mo",
                    profile_ref=probe_candidate_ref,
                    profile_path=probe_candidate_path,
                    instruments=instruments,
                    timeframe=probe_timeframe,
                    lookback_months=screen_months,
                    reward_matrix=reward_matrix,
                    as_of_date=screen_as_of_date,
                )
                probe_score = _as_float(result.get("score"))
                _apply_stage_candidate(
                    stage_key="coarse_probe",
                    phase="coarse_probe",
                    candidate_profile_path=probe_candidate_path,
                    candidate_profile_ref=probe_candidate_ref,
                    candidate_timeframe=probe_timeframe,
                    candidate_score=result.get("score"),
                    detail=f"probe {probe_budget}/{max_sweep_permutations} evals",
                    phase_score_key="coarse_probe_top_3mo",
                )
            halving_decision = build_coarse_halving_decision(
                mode=coarse_halving_mode,
                total_budget=max_sweep_permutations,
                probe_budget=coarse_probe_budget,
                incumbent_score=pre_probe_incumbent_score,
                probe_score=probe_score,
            )
            _record_coarse_halving_decision(halving_decision)
            if halving_decision["expanded"]:
                expand_source_profile_path = probe_candidate_path or current_profile_path
                expand_source_profile_ref = probe_candidate_ref or current_profile_ref
                expand_source_payload = _load_json(expand_source_profile_path)
                sweep = _run_sweep(
                    ctx,
                    stage=stages["coarse"],
                    phase="coarse_expand",
                    profile_ref=expand_source_profile_ref,
                    profile_payload=expand_source_payload,
                    instruments=instruments,
                    axes=coarse_axes,
                    mode=coarse_mode,
                    sweep_budget=str(expand_budget),
                    max_permutations=expand_budget,
                    reward_matrix=reward_matrix,
                    as_of_date=screen_as_of_date,
                )
                last_sweep_payload = sweep["result"]
                last_sweep_axes = list(sweep.get("axes") or coarse_axes)
                materialized = _materialize_and_register_best_sweep_candidate(
                    ctx,
                    stage=stages["coarse"],
                    source_profile_path=expand_source_profile_path,
                    sweep_payload=last_sweep_payload,
                    phase="coarse_expand",
                )
                if materialized:
                    candidate_profile_path, candidate_profile_ref, _params = materialized
                    candidate_timeframe = _lowest_profile_timeframe(
                        _load_json(candidate_profile_path),
                        timeframe,
                    )
                    result = _evaluate_profile(
                        ctx,
                        stage=stages["coarse"],
                        phase="coarse_expand_top_3mo",
                        profile_ref=candidate_profile_ref,
                        profile_path=candidate_profile_path,
                        instruments=instruments,
                        timeframe=candidate_timeframe,
                        lookback_months=screen_months,
                        reward_matrix=reward_matrix,
                        as_of_date=screen_as_of_date,
                    )
                    _apply_stage_candidate(
                        stage_key="coarse_expand",
                        phase="coarse_expand",
                        candidate_profile_path=candidate_profile_path,
                        candidate_profile_ref=candidate_profile_ref,
                        candidate_timeframe=candidate_timeframe,
                        candidate_score=result.get("score"),
                        detail=f"expand {expand_budget} evals",
                        phase_score_key="coarse_expand_top_3mo",
                    )
            else:
                skip_focused_and_scout = True
                phase_rows.append(
                    {
                        "phase": "coarse expand",
                        "status": "skipped",
                        "score": None,
                        "detail": "coarse halving skipped expansion",
                    }
                )
            _record_phase_score("coarse_top_3mo", incumbent.get("score"))
            _report_early_exit("after_coarse_top", "coarse")
        else:
            if coarse_halving_mode == "enforce":
                if coarse_mode != "evolutionary":
                    _record_coarse_halving_decision(
                        {
                            "version": PLAY_HAND_COARSE_HALVING_VERSION,
                            "mode": coarse_halving_mode,
                            "probe_budget": coarse_probe_budget,
                            "expand_budget": 0,
                            "total_budget": max_sweep_permutations,
                            "expanded": True,
                            "decision": "use_original_coarse",
                            "reasons": ["coarse_mode_not_evolutionary"],
                            "incumbent_score": _as_float(incumbent.get("score")),
                            "probe_score": None,
                            "estimated_saved_evaluations": 0,
                            "skipped_stages": [],
                        }
                    )
                else:
                    _record_coarse_halving_decision(
                        build_coarse_halving_decision(
                            mode=coarse_halving_mode,
                            total_budget=max_sweep_permutations,
                            probe_budget=coarse_probe_budget,
                            incumbent_score=incumbent.get("score"),
                            probe_score=None,
                        )
                    )
            sweep = _run_sweep(
                ctx,
                stage=stages["coarse"],
                phase="coarse",
                profile_ref=current_profile_ref,
                profile_payload=current_profile_payload,
                instruments=instruments,
                axes=coarse_axes,
                mode=coarse_mode,
                sweep_budget=sweep_budget_label,
                max_permutations=max_sweep_permutations,
                reward_matrix=reward_matrix,
                as_of_date=screen_as_of_date,
            )
            last_sweep_payload = sweep["result"]
            last_sweep_axes = list(sweep.get("axes") or coarse_axes)
            source_profile_path = current_profile_path
            materialized = _materialize_and_register_best_sweep_candidate(
                ctx,
                stage=stages["coarse"],
                source_profile_path=source_profile_path,
                sweep_payload=last_sweep_payload,
                phase="coarse",
            )
            if materialized:
                candidate_profile_path, candidate_profile_ref, _params = materialized
                candidate_timeframe = _lowest_profile_timeframe(
                    _load_json(candidate_profile_path),
                    timeframe,
                )
                if not stage_acceptance_enabled:
                    current_profile_path = candidate_profile_path
                    current_profile_ref = candidate_profile_ref
                    current_evaluation_timeframe = candidate_timeframe
                result = _evaluate_profile(
                    ctx,
                    stage=stages["coarse"],
                    phase="coarse_top_3mo",
                    profile_ref=candidate_profile_ref,
                    profile_path=candidate_profile_path,
                    instruments=instruments,
                    timeframe=candidate_timeframe,
                    lookback_months=screen_months,
                    reward_matrix=reward_matrix,
                    as_of_date=screen_as_of_date,
                )
                if stage_acceptance_enabled:
                    _apply_stage_candidate(
                        stage_key="coarse",
                        phase="coarse",
                        candidate_profile_path=candidate_profile_path,
                        candidate_profile_ref=candidate_profile_ref,
                        candidate_timeframe=candidate_timeframe,
                        candidate_score=result.get("score"),
                        detail=f"{len(coarse_axes)} axes",
                        phase_score_key="coarse_top_3mo",
                    )
                else:
                    phase_rows.append({"phase": "coarse", "status": "top evaluated", "score": result.get("score"), "detail": f"{len(coarse_axes)} axes"})
                    _record_phase_score("coarse_top_3mo", result.get("score"))
                _report_early_exit("after_coarse_top", "coarse")
    else:
        _append_event(ctx, "coarse", "skipped", stage=stages["coarse"], reason="no numeric talib axes found")
        _report_early_exit("after_coarse_top", "coarse")

    focused_axes = (
        build_focused_axes(_parameter_importance(last_sweep_payload or {}), last_sweep_axes)
        if last_sweep_payload and not skip_focused_and_scout and not skip_mutation_pipeline
        else []
    )
    if skip_mutation_pipeline:
        _append_event(
            ctx,
            "focused",
            "skipped",
            stage=stages["focused"],
            reason="family_policy_template_locked_exact_only",
        )
        phase_rows.append(
            {
                "phase": "focused",
                "status": "skipped",
                "score": None,
                "detail": "family policy template-locked exact-only",
            }
        )
        _report_early_exit("after_focused_top", "focused")
    elif skip_focused_and_scout:
        _append_event(
            ctx,
            "focused",
            "skipped",
            stage=stages["focused"],
            reason="coarse_halving_skip_expansion",
        )
        phase_rows.append(
            {
                "phase": "focused",
                "status": "skipped",
                "score": None,
                "detail": "coarse halving skipped focused refinement",
            }
        )
        _report_early_exit("after_focused_top", "focused")
    elif focused_axes:
        sweep = _run_sweep(
            ctx,
            stage=stages["focused"],
            phase="focused",
            profile_ref=current_profile_ref,
            profile_payload=_load_json(current_profile_path),
            instruments=instruments,
            axes=focused_axes,
            mode="deterministic",
            sweep_budget=sweep_budget_label,
            max_permutations=max_sweep_permutations,
            reward_matrix=reward_matrix,
            as_of_date=screen_as_of_date,
        )
        materialized = _materialize_and_register_best_sweep_candidate(
            ctx,
            stage=stages["focused"],
            source_profile_path=current_profile_path,
            sweep_payload=sweep["result"],
            phase="focused",
        )
        if materialized:
            candidate_profile_path, candidate_profile_ref, _params = materialized
            candidate_timeframe = _lowest_profile_timeframe(
                _load_json(candidate_profile_path),
                timeframe,
            )
            if not stage_acceptance_enabled:
                current_profile_path = candidate_profile_path
                current_profile_ref = candidate_profile_ref
                current_evaluation_timeframe = candidate_timeframe
            result = _evaluate_profile(
                ctx,
                stage=stages["focused"],
                phase="focused_top_3mo",
                profile_ref=candidate_profile_ref,
                profile_path=candidate_profile_path,
                instruments=instruments,
                timeframe=candidate_timeframe,
                lookback_months=screen_months,
                reward_matrix=reward_matrix,
                as_of_date=screen_as_of_date,
            )
            if stage_acceptance_enabled:
                _apply_stage_candidate(
                    stage_key="focused",
                    phase="focused",
                    candidate_profile_path=candidate_profile_path,
                    candidate_profile_ref=candidate_profile_ref,
                    candidate_timeframe=candidate_timeframe,
                    candidate_score=result.get("score"),
                    detail=", ".join(focused_axes),
                    phase_score_key="focused_top_3mo",
                )
            else:
                phase_rows.append({"phase": "focused", "status": "top evaluated", "score": result.get("score"), "detail": ", ".join(focused_axes)})
                _record_phase_score("focused_top_3mo", result.get("score"))
            _report_early_exit("after_focused_top", "focused")
    else:
        _append_event(ctx, "focused", "skipped", stage=stages["focused"], reason="no high-impact axes available from previous sweep")
        _report_early_exit("after_focused_top", "focused")

    _report_early_exit("before_instrument_scout", "instrument_scout")
    if skip_mutation_pipeline:
        scout_result = {
            "version": "instrument_scout_v1",
            "status": "skipped",
            "reason": "family_policy_template_locked_exact_only",
            "selected_instruments": list(instruments),
            "primary_instrument": instrument_deal["primary_instrument"],
            "accepted": [],
            "rejected": [],
        }
        metadata["instrument_scout"] = scout_result
        write_run_metadata(run_dir, metadata)
        _append_event(
            ctx,
            "instrument_scout",
            "skipped",
            stage=stages["instrument_scout"],
            reason="family_policy_template_locked_exact_only",
        )
    elif skip_focused_and_scout:
        scout_result = {
            "version": "instrument_scout_v1",
            "status": "skipped",
            "reason": "coarse_halving_skip_expansion",
            "selected_instruments": list(instruments),
            "primary_instrument": instrument_deal["primary_instrument"],
            "accepted": [],
            "rejected": [],
        }
        metadata["instrument_scout"] = scout_result
        write_run_metadata(run_dir, metadata)
        _append_event(
            ctx,
            "instrument_scout",
            "skipped",
            stage=stages["instrument_scout"],
            reason="coarse_halving_skip_expansion",
        )
    else:
        scout_result = _run_instrument_scout(
            ctx,
            stage=stages["instrument_scout"],
            profile_ref=current_profile_ref,
            profile_path=current_profile_path,
            instrument_deal=instrument_deal,
            instruments=instruments,
            timeframe=current_evaluation_timeframe,
            lookback_months=int(instrument_scout_months or screen_months),
            rng=rng,
            enabled=bool(instrument_scout),
            scout_size=int(instrument_scout_size),
            max_selected=int(instrument_scout_max_selected),
            reward_matrix=reward_matrix,
            as_of_date=screen_as_of_date,
        )
    scout_selected = _clean_tokens(list(scout_result.get("selected_instruments") or []))
    if scout_selected:
        instruments = scout_selected
        metadata["instruments"] = instruments
        metadata["instrument_scout"] = scout_result
        write_run_metadata(run_dir, metadata)
    scout_status = str(scout_result.get("status") or "")
    scout_detail = ", ".join(instruments)
    if scout_result.get("accepted"):
        scout_detail += f" ({len(scout_result.get('accepted') or [])} added)"
    phase_rows.append(
        {
            "phase": "instrument scout",
            "status": scout_status or "completed",
            "score": None,
            "detail": scout_detail,
        }
    )

    _report_early_exit("before_final_scrutiny", "scrutiny")
    mutated_scrutiny: dict[str, Any] | None = None
    mutated_outcome: dict[str, Any] = {
        "passed": False,
        "score": None,
        "reason": "mutated_branch_skipped",
        "reasons": ["family_policy_template_locked_exact_only"],
    }
    final_branch_candidates: list[dict[str, Any]] = []
    if skip_mutation_pipeline:
        _append_event(
            ctx,
            "mutated_final_36mo",
            "skipped",
            stage=stages["scrutiny"],
            reason="family_policy_template_locked_exact_only",
        )
        phase_rows.append(
            {
                "phase": "mutated final",
                "status": "skipped",
                "score": None,
                "detail": "family policy template-locked exact-only",
            }
        )
    else:
        mutated_scrutiny = _evaluate_profile(
            ctx,
            stage=stages["scrutiny"],
            phase="mutated_final_36mo",
            profile_ref=current_profile_ref,
            profile_path=current_profile_path,
            instruments=instruments,
            timeframe=current_evaluation_timeframe,
            lookback_months=scrutiny_months,
            reward_matrix=reward_matrix,
        )
        mutated_outcome = _final_scrutiny_outcome(mutated_scrutiny)
        final_branch_candidates.append(
            {
            "branch": "mutated",
            "phase": "mutated_final_36mo",
            "scrutiny": mutated_scrutiny,
            "outcome": mutated_outcome,
            "attempt_id": str(mutated_scrutiny.get("attempt_id") or "").strip(),
            "profile_ref": current_profile_ref,
            "profile_path": current_profile_path,
            "instruments": list(instruments),
            "timeframe": current_evaluation_timeframe,
            }
        )
    if exact_template_profile_path is not None and exact_template_profile_ref:
        exact_template_scrutiny = _evaluate_profile(
            ctx,
            stage=stages["scrutiny"],
            phase="exact_template_36mo",
            profile_ref=exact_template_profile_ref,
            profile_path=exact_template_profile_path,
            instruments=template_branch_instruments,
            timeframe=exact_template_timeframe or evaluation_timeframe,
            lookback_months=scrutiny_months,
            reward_matrix=reward_matrix,
        )
        exact_template_outcome = _final_scrutiny_outcome(exact_template_scrutiny)
        final_branch_candidates.append(
            {
                "branch": "exact_template",
                "phase": "exact_template_36mo",
                "scrutiny": exact_template_scrutiny,
                "outcome": exact_template_outcome,
                "attempt_id": str(exact_template_scrutiny.get("attempt_id") or "").strip(),
                "profile_ref": exact_template_profile_ref,
                "profile_path": exact_template_profile_path,
                "instruments": list(template_branch_instruments),
                "timeframe": exact_template_timeframe or evaluation_timeframe,
            }
        )
    if not final_branch_candidates:
        raise RuntimeError("PlayHand final scrutiny has no branch candidates")
    if calendar_gate_mode != "off":
        for branch in final_branch_candidates:
            branch["calendar_gate"] = _branch_calendar_gate(branch, mode=calendar_gate_mode)
    selected_final_branch = (
        _select_final_scrutiny_branch(
            final_branch_candidates,
            enforce_calendar_gate=calendar_gate_mode == "enforce",
        )
        or final_branch_candidates[0]
    )
    scrutiny = selected_final_branch["scrutiny"]
    final_scrutiny = selected_final_branch["outcome"]
    final_attempt_id = str(selected_final_branch.get("attempt_id") or "").strip()
    final_scrutiny_passed = bool(final_scrutiny.get("passed"))
    selected_calendar_gate = (
        selected_final_branch.get("calendar_gate")
        if isinstance(selected_final_branch.get("calendar_gate"), dict)
        else None
    )
    calendar_gate_failed = bool(
        selected_calendar_gate is not None and not selected_calendar_gate.get("passed")
    )
    calendar_gate_blocked = bool(
        calendar_gate_mode == "enforce" and final_scrutiny_passed and calendar_gate_failed
    )
    run_promoted = final_scrutiny_passed and not calendar_gate_blocked
    if calendar_gate_blocked:
        run_tombstone_reason: str | None = PLAY_HAND_CALENDAR_GATE_FAILED_REASON
        run_tombstone_reasons = [
            PLAY_HAND_CALENDAR_GATE_FAILED_REASON,
            *[
                str(reason)
                for reason in list((selected_calendar_gate or {}).get("reasons") or [])
                if str(reason).strip()
            ],
        ]
    else:
        run_tombstone_reason = final_scrutiny.get("reason")
        run_tombstone_reasons = list(final_scrutiny.get("reasons") or [])
    canonical_attempt_id = final_attempt_id if run_promoted else None
    selected_final_branch_name = str(selected_final_branch.get("branch") or "mutated")
    selected_final_phase = str(selected_final_branch.get("phase") or "final_36mo")
    selected_final_instruments = _clean_tokens(selected_final_branch.get("instruments") or instruments)
    selected_final_timeframe = str(selected_final_branch.get("timeframe") or current_evaluation_timeframe)
    selected_final_profile_ref = str(selected_final_branch.get("profile_ref") or current_profile_ref)
    selected_profile_path_value = selected_final_branch.get("profile_path")
    selected_final_profile_path = (
        selected_profile_path_value
        if isinstance(selected_profile_path_value, Path)
        else Path(str(selected_profile_path_value or current_profile_path))
    )
    final_branch_scores = [
        {
            "branch": str(branch.get("branch") or ""),
            "phase": str(branch.get("phase") or ""),
            "attempt_id": str(branch.get("attempt_id") or "") or None,
            "score": (
                branch.get("outcome", {}).get("score")
                if isinstance(branch.get("outcome"), dict)
                else None
            ),
            "passed": bool(
                branch.get("outcome", {}).get("passed")
                if isinstance(branch.get("outcome"), dict)
                else False
            ),
            "instruments": list(branch.get("instruments") or []),
            "profile_ref": branch.get("profile_ref"),
        }
        for branch in final_branch_candidates
    ]
    exact_template_branch = next(
        (
            branch
            for branch in final_branch_candidates
            if str(branch.get("branch") or "") == "exact_template"
        ),
        {},
    )
    exact_template_branch_outcome = (
        exact_template_branch.get("outcome")
        if isinstance(exact_template_branch.get("outcome"), dict)
        else {}
    )
    if selected_final_branch_name == "exact_template" and skip_mutation_pipeline:
        canonical_selection_reason = "template_locked_exact_only"
    elif selected_final_branch_name == "exact_template":
        canonical_selection_reason = (
            "rescued_by_exact_template"
            if not mutated_outcome.get("passed") and exact_template_branch_outcome.get("passed")
            else "exact_template_outscored_mutated"
        )
    elif final_scrutiny_passed:
        canonical_selection_reason = "mutated_branch_selected"
    else:
        canonical_selection_reason = f"no_branch_passed_{selected_final_branch_name}_best_score"
    if calendar_gate_mode != "off":
        _append_event(
            ctx,
            "calendar_gate",
            "evaluated",
            stage=stages["scrutiny"],
            mode=calendar_gate_mode,
            selected_branch=selected_final_branch_name,
            attempt_id=final_attempt_id or None,
            gate_passed=not calendar_gate_failed,
            gate_reasons=list((selected_calendar_gate or {}).get("reasons") or []),
            gate_metrics=dict((selected_calendar_gate or {}).get("metrics") or {}),
            would_block_promotion=bool(final_scrutiny_passed and calendar_gate_failed),
            promotion_blocked=calendar_gate_blocked,
            branch_gates=[
                {
                    "branch": str(branch.get("branch") or ""),
                    "passed": bool((branch.get("calendar_gate") or {}).get("passed", True)),
                    "reasons": list((branch.get("calendar_gate") or {}).get("reasons") or []),
                }
                for branch in final_branch_candidates
            ],
        )
    attempt_metadata_summary = _finalize_play_hand_attempt_metadata(
        ctx,
        final_attempt_id=final_attempt_id,
        scout_result=scout_result,
        selected_instruments=selected_final_instruments,
        reward_matrix=reward_matrix,
        final_scrutiny_passed=final_scrutiny_passed,
        final_scrutiny_score=final_scrutiny.get("score"),
        tombstone_reason=run_tombstone_reason,
        tombstone_reasons=run_tombstone_reasons,
        run_promoted=run_promoted,
        calendar_gate=selected_calendar_gate,
    )
    metadata.update(
        {
            "run_status": "promoted" if run_promoted else "tombstoned",
            "run_tombstoned": not run_promoted,
            "tombstone_reason": run_tombstone_reason,
            "tombstone_reasons": run_tombstone_reasons,
            "final_attempt_id": final_attempt_id or None,
            "final_scrutiny_passed": final_scrutiny_passed,
            "final_scrutiny_score": final_scrutiny.get("score"),
            "mutated_attempt_id": (
                str(mutated_scrutiny.get("attempt_id") or "") or None
                if isinstance(mutated_scrutiny, dict)
                else None
            ),
            "mutated_score": mutated_outcome.get("score"),
            "exact_template_attempt_id": str(exact_template_branch.get("attempt_id") or "") or None,
            "exact_template_score": (
                exact_template_branch.get("outcome", {}).get("score")
                if isinstance(exact_template_branch.get("outcome"), dict)
                else None
            ),
            "selected_final_branch": selected_final_branch_name,
            "selected_final_phase": selected_final_phase,
            "canonical_selection_reason": canonical_selection_reason,
            "final_branch_scores": final_branch_scores,
            "exact_template_source": exact_template_source if exact_template_branch else None,
            "exact_template_source_profile_path": (
                str(exact_template_source_profile_path.resolve())
                if exact_template_source_profile_path is not None
                else None
            ),
            "template_branch_instruments": template_branch_instruments,
            "template_branch_source_probe_id": template_branch_source_probe_id,
            "canonical_attempt_id": canonical_attempt_id,
            "canonical_attempt_role": "final" if run_promoted else None,
            "canonical_candidate_name": selected_final_phase if run_promoted else None,
            "canonical_score": scrutiny.get("score") if run_promoted else None,
            "canonical_instruments": selected_final_instruments,
            "calendar_gate_mode": calendar_gate_mode,
            "calendar_gate": selected_calendar_gate,
            "strategy_family_id": run_id,
            "attempt_metadata": attempt_metadata_summary,
            "family_policy_execution": metadata.get("family_policy_execution"),
        }
    )
    write_run_metadata(run_dir, metadata)
    _append_event(
        ctx,
        "attempt_metadata",
        "updated",
        stage=stages["scrutiny"],
        **attempt_metadata_summary,
    )
    for branch in final_branch_candidates:
        branch_outcome = branch.get("outcome") if isinstance(branch.get("outcome"), dict) else {}
        branch_name = str(branch.get("branch") or "")
        branch_status = "evaluated" if branch_outcome.get("passed") else "failed"
        branch_detail = (
            f"{scrutiny_months}mo on {', '.join(branch.get('instruments') or [])}; "
            f"branch={branch_name}"
        )
        if not branch_outcome.get("passed"):
            branch_detail += f"; tombstoned={branch_outcome.get('reason') or 'failed'}"
        phase_rows.append(
            {
                "phase": branch_name.replace("_", " ") or "scrutiny",
                "status": branch_status,
                "score": (
                    branch.get("scrutiny", {}).get("score")
                    if isinstance(branch.get("scrutiny"), dict)
                    else None
                ),
                "detail": branch_detail,
            }
        )
    scrutiny_status = "evaluated" if run_promoted else "failed"
    scrutiny_detail = (
        f"selected={selected_final_branch_name}; "
        f"{scrutiny_months}mo on {', '.join(selected_final_instruments)}"
    )
    if not run_promoted:
        scrutiny_detail += f"; tombstoned={run_tombstone_reason or 'failed'}"
    elif calendar_gate_mode == "report" and calendar_gate_failed:
        scrutiny_detail += "; calendar_gate=would_fail"
    phase_rows.append(
        {
            "phase": "scrutiny",
            "status": scrutiny_status,
            "score": scrutiny.get("score"),
            "detail": scrutiny_detail,
        }
    )

    final_artifact_summary: dict[str, Any]
    if final_artifacts and not run_promoted:
        final_artifact_summary = {
            "status": "skipped",
            "reason": run_tombstone_reason or PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON,
            "run_tombstoned": True,
            "final_attempt_id": final_attempt_id or None,
            "final_scrutiny_score": final_scrutiny.get("score"),
        }
        _append_event(
            ctx,
            "final_artifacts",
            "skipped",
            stage=stages["artifacts"],
            reason=final_artifact_summary["reason"],
            run_tombstoned=True,
            final_attempt_id=final_attempt_id or None,
        )
        phase_rows.append(
            {
                "phase": "artifacts",
                "status": "skipped",
                "score": None,
                "detail": f"run tombstoned: {final_artifact_summary['reason']}",
            }
        )
    elif final_artifacts:
        final_artifact_summary = _finalize_run_artifacts(
            ctx,
            stage=stages["artifacts"],
            profile_drop_count=final_profile_drop_count,
            profile_drop_workers=final_profile_drop_workers,
            final_attempt_id=canonical_attempt_id,
        )
        status = str(final_artifact_summary.get("status") or "")
        detail_bits: list[str] = []
        full_backtests = final_artifact_summary.get("full_backtests")
        if isinstance(full_backtests, dict):
            detail_bits.append(
                "finalize "
                f"selected={full_backtests.get('selected_count', 0)} "
                f"status={full_backtests.get('status') or 'unknown'} "
                f"exit={full_backtests.get('render_exit_code')}"
            )
        profile_drops = final_artifact_summary.get("profile_drops")
        if isinstance(profile_drops, dict):
            detail_bits.append(
                "drop "
                f"render={profile_drops.get('profile_drop_rendered', 0)} "
                f"cache={profile_drops.get('profile_drop_cached', 0)} "
                f"skip={profile_drops.get('profile_drop_skipped', 0)} "
                f"fail={profile_drops.get('profile_drop_failed', 0)}"
            )
        phase_rows.append(
            {
                "phase": "artifacts",
                "status": status,
                "score": None,
                "detail": "; ".join(detail_bits) or final_artifact_summary.get("error") or "",
            }
        )
    else:
        final_artifact_summary = {
            "status": "skipped",
            "reason": "disabled",
        }
        _append_event(ctx, "final_artifacts", "skipped", stage=stages["artifacts"], reason="disabled")

    cloud_profile_cleanup = _run_play_hand_cloud_cleanup("completed")
    if cleanup_registered_with_atexit:
        try:
            atexit.unregister(_run_play_hand_cloud_cleanup)
        except ValueError:
            pass

    metadata["phase_rows"] = phase_rows
    metadata["final_artifacts"] = final_artifact_summary
    metadata["cloud_profile_cleanup"] = cloud_profile_cleanup
    play_hand_health = build_play_hand_health(
        run_metadata=metadata,
        attempts=load_attempts(ctx.attempts_path),
    )
    metadata["play_hand_health"] = play_hand_health
    write_run_metadata(run_dir, metadata)

    summary = {
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "runner": "play_hand_v1",
        "dealt_indicator_ids": dealt,
        "dealt_indicator_count": len(dealt),
        "min_indicators": effective_min_indicators,
        "max_indicators": effective_max_indicators,
        "requested_min_indicators": min_indicators,
        "requested_max_indicators": max_indicators,
        "instrument_source": instrument_deal["source"],
        "primary_instrument": instrument_deal["primary_instrument"],
        "instrument_pool": instrument_deal["instrument_pool"],
        "instruments": selected_final_instruments,
        "timeframe": selected_final_timeframe,
        "requested_timeframe": timeframe,
        "indicator_timeframes": _profile_timeframes(_load_json(selected_final_profile_path)),
        "sweep_budget": sweep_budget_label,
        "sweep_budget_value": sweep_budget_value,
        "sweep_budget_source": budget.get("source"),
        "max_sweep_permutations": max_sweep_permutations,
        "deep_replay_job_timeout_seconds": job_timeout_seconds,
        "sweep_timeout_seconds": sweep_timeout_seconds,
        "max_reward_r": reward_matrix.get("requested_max_reward_r") if reward_matrix else max_reward_r,
        "reward_matrix": reward_matrix,
        "dealt_indicator_source": indicator_deal.get("source"),
        "dealt_indicator_source_reason": indicator_deal.get("reason"),
        "dealt_recipe": indicator_deal.get("recipe"),
        "dealt_recipe_source": indicator_deal.get("recipe_source"),
        "dealt_recipe_confidence": indicator_deal.get("recipe_confidence"),
        "guided_recipe_source_mix_expected": indicator_deal.get(
            "guided_recipe_source_mix_expected"
        ),
        "guided_recipe_source_bucket": indicator_deal.get("guided_recipe_source_bucket"),
        "guided_recipe_source_bucket_matched": indicator_deal.get(
            "guided_recipe_source_bucket_matched"
        ),
        "guided_recipe_source_bucket_fallback": indicator_deal.get(
            "guided_recipe_source_bucket_fallback"
        ),
        "dealt_recipe_pair": indicator_deal.get("pair"),
        "dealt_pair_family_policy": indicator_deal.get("family_policy"),
        "dealt_policy_target_count": indicator_deal.get("policy_target_count"),
        "dealt_recipe_slots": indicator_deal.get("selected_slots"),
        "instrument_scout": scout_result,
        "run_status": "promoted" if run_promoted else "tombstoned",
        "run_tombstoned": not run_promoted,
        "tombstone_reason": run_tombstone_reason,
        "tombstone_reasons": run_tombstone_reasons,
        "final_attempt_id": final_attempt_id or None,
        "final_scrutiny_passed": final_scrutiny_passed,
        "final_scrutiny_score": final_scrutiny.get("score"),
        "mutated_attempt_id": (
            str(mutated_scrutiny.get("attempt_id") or "") or None
            if isinstance(mutated_scrutiny, dict)
            else None
        ),
        "mutated_score": mutated_outcome.get("score"),
        "exact_template_attempt_id": str(exact_template_branch.get("attempt_id") or "") or None,
        "exact_template_score": (
            exact_template_branch.get("outcome", {}).get("score")
            if isinstance(exact_template_branch.get("outcome"), dict)
            else None
        ),
        "selected_final_branch": selected_final_branch_name,
        "selected_final_phase": selected_final_phase,
        "canonical_selection_reason": canonical_selection_reason,
        "final_branch_scores": final_branch_scores,
        "exact_template_source": exact_template_source if exact_template_branch else None,
        "exact_template_source_profile_path": (
            str(exact_template_source_profile_path.resolve())
            if exact_template_source_profile_path is not None
            else None
        ),
        "template_branch_instruments": template_branch_instruments,
        "template_branch_source_probe_id": template_branch_source_probe_id,
        "canonical_attempt_id": canonical_attempt_id,
        "canonical_attempt_role": "final" if run_promoted else None,
        "canonical_candidate_name": selected_final_phase if run_promoted else None,
        "calendar_gate_mode": calendar_gate_mode,
        "calendar_gate": selected_calendar_gate,
        "strategy_family_id": run_id,
        "attempt_metadata": attempt_metadata_summary,
        "final_profile_ref": selected_final_profile_ref,
        "final_profile_path": str(selected_final_profile_path.resolve()),
        "final_score": scrutiny.get("score"),
        "events_path": str(ctx.events_path.resolve()),
        "attempts_path": str(ctx.attempts_path.resolve()),
        "phase_rows": phase_rows,
        "play_hand_health": play_hand_health,
        "early_exit_policy": metadata.get("early_exit_policy"),
        "coarse_halving": metadata.get("coarse_halving"),
        "family_policy_execution": metadata.get("family_policy_execution"),
        "stage_incumbent": metadata.get("stage_incumbent"),
        "stage_acceptance_decisions": metadata.get("stage_acceptance_decisions"),
        "final_artifacts": final_artifact_summary,
        "cloud_profile_cleanup": cloud_profile_cleanup,
    }
    _write_json(ctx.summary_path, summary)
    if as_json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
    else:
        _render_phase_table(phase_rows)
        console.print(f"Run dir: {run_dir}")
    return 0
