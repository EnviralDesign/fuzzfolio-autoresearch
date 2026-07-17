from __future__ import annotations

import csv
import hashlib
import json
import calendar
import math
import re
import shutil
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

import requests

from .anchor_pair_atlas import (
    DEFAULT_ANCHOR_PAIR_DIRNAME,
    DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME,
    DEFAULT_EXECUTION_COST_MODE,
    DEFAULT_QUALITY_SCORE_PRESET,
    _probe_results_fieldnames,
    _result_row_from_score,
    _timing_result_row_from_score,
    _timing_results_fieldnames,
    build_anchor_pair_atlas,
    build_anchor_pair_timing_atlas,
)
from .config import AppConfig
from .discovery_cluster_atlas import (
    DEFAULT_DISCOVERY_CLUSTER_DIRNAME,
    DEFAULT_MAX_RECIPES as DEFAULT_DISCOVERY_CLUSTER_MAX_RECIPES,
    DEFAULT_MIN_SHARED_PARTNERS as DEFAULT_DISCOVERY_CLUSTER_MIN_SHARED_PARTNERS,
    DEFAULT_MIN_SIMILARITY as DEFAULT_DISCOVERY_CLUSTER_MIN_SIMILARITY,
    build_discovery_cluster_atlas,
)
from .discovery_pair_atlas import (
    DEFAULT_DISCOVERY_PAIR_DIRNAME,
    _result_fieldnames as _discovery_pair_result_fieldnames,
    _result_row_from_discovery_score,
    build_discovery_pair_atlas,
)
from .discovery_recipe_validation import (
    DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME,
    DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME,
    DEFAULT_DIVERSITY_PENALTY_SCALE,
    DEFAULT_FIRST_MEMBER_LIMIT as DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_FIRST_MEMBER_LIMIT,
    DEFAULT_INCLUDED_CONFIDENCE as DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_INCLUDED_CONFIDENCE,
    DEFAULT_MAX_PAIRS_PER_RECIPE as DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_MAX_PAIRS_PER_RECIPE,
    DEFAULT_MAX_RECIPES as DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_MAX_RECIPES,
    DEFAULT_SECOND_MEMBER_LIMIT as DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_SECOND_MEMBER_LIMIT,
    _result_fieldnames as _discovery_recipe_result_fieldnames,
    _result_row_from_validation_score,
    build_discovery_recipe_scrutiny_atlas,
    build_discovery_recipe_validation_atlas,
)
from .durable_execution import (
    DurableExecutionError,
    DurableExecutionJournal,
    artifact_receipt,
    atomic_write_json,
    validate_artifact_receipt,
)
from .forward_response_atlas import (
    DEFAULT_FORWARD_RESPONSE_DIRNAME,
    build_forward_response_atlas,
)
from .fuzzfolio import FuzzfolioCli
from .indicator_atlas import DEFAULT_ATLAS_DIRNAME, build_indicator_atlas
from .play_hand_lab import (
    DEFAULT_LAB_GATEWAY_URL,
    PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    LabGatewayClient,
)
from .instrument_universe import research_eligible_instruments, universe_provenance
from .level_c_operator import validate_executor_runtime_binding
from .play_hand_lab_auth import load_lab_gateway_token
from .evidence_plan import (
    build_replay_evidence_plan,
    canonical_sha256,
    canonical_timestamp,
    normalize_evidence_profile_snapshot,
)
from .recipe_priors import DEFAULT_RECIPE_PRIORS_DIRNAME, build_recipe_priors
from .scoring import AttemptScore, build_attempt_score, load_sensitivity_snapshot
from .signal_atlas import (
    DEFAULT_BAR_LIMIT as DEFAULT_SIGNAL_BAR_LIMIT,
    DEFAULT_INSTRUMENTS as DEFAULT_SIGNAL_INSTRUMENTS,
    DEFAULT_SIGNAL_ATLAS_DIRNAME,
    DEFAULT_TIMEFRAMES as DEFAULT_SIGNAL_TIMEFRAMES,
    SCHEMA_VERSION as SIGNAL_ATLAS_SCHEMA_VERSION,
    SignalAtlasBuildResult,
    _aggregate_indicator_rows as _aggregate_signal_indicator_rows,
    _atlas_rows_by_id as _signal_atlas_rows_by_id,
    _error_type as _signal_error_type,
    _indicator_catalog_by_id as _signal_indicator_catalog_by_id,
    _normalize_signal_roles as _normalize_signal_roles_for_atlas,
    _profile_document_for_indicator,
    _select_indicator_ids as _select_signal_indicator_ids,
    _write_issues_csv as _write_signal_issues_csv,
    _write_rows_csv as _write_signal_rows_csv,
    build_signal_atlas,
    compute_signal_metrics,
    load_indicator_catalog,
)


ATLAS_LAB_RUNS_DIRNAME = "atlas-runs"
ATLAS_LAB_RUN_SCHEMA_VERSION = "atlas_lab_run_v1"
ATLAS_LAB_RUNNER = "atlas_lab_v1"
ATLAS_LAB_DURABLE_STAGES = (
    "01-indicator-atlas",
    "02-signal-atlas",
    "03-forward-response-atlas",
    "04-anchor-pair-atlas",
    "05-anchor-pair-probes",
    "06-anchor-pair-timing-atlas",
    "07-anchor-pair-timing-probes",
    "08-recipe-priors",
    "09-discovery-pair-atlas",
    "10-discovery-pair-probes",
    "11-discovery-cluster-atlas",
    "12-discovery-recipe-validation-atlas",
    "13-discovery-recipe-validation-probes",
    "14-discovery-recipe-scrutiny-atlas",
    "15-discovery-recipe-scrutiny-probes",
    "16-final-recipe-priors",
)
DEFAULT_ATLAS_LAB_ACTIVE_PROBES = 128
DEFAULT_ATLAS_LAB_ENQUEUE_CHUNK = 256
DEFAULT_ATLAS_LAB_RESULT_BATCH_SIZE = 250
DEFAULT_ATLAS_LAB_MAX_RESULTS_PER_CYCLE = 1000
DEFAULT_ATLAS_LAB_MAX_DRAIN_SECONDS = 0.5
DEFAULT_ATLAS_LAB_POLL_INTERVAL_SECONDS = 0.25
DEFAULT_ATLAS_LAB_DEADLINE_SECONDS = 3600
DEFAULT_ATLAS_LAB_MAX_ATTEMPTS = 8
DEFAULT_ATLAS_LAB_LOG_INTERVAL_SECONDS = 5.0
DEFAULT_ATLAS_LAB_WORKER_CONTRACT_SCHEMA = "replay-worker-contract-v1"
DEFAULT_ATLAS_PROFILE = "rich"
DEFAULT_DISCOVERY_VALIDATION_CONFIDENCE = ",".join(
    DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_INCLUDED_CONFIDENCE
)


def _ordered_unique_tokens(values: list[str] | tuple[str, ...]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip().upper()
        if token and token not in seen:
            selected.append(token)
            seen.add(token)
    return selected


ATLAS_STANDARD_SIGNAL_ROLES = ("trigger",)
ATLAS_RICH_SIGNAL_ROLES = ("trigger", "setup", "context", "filter")
ATLAS_STANDARD_INSTRUMENTS = research_eligible_instruments(
    asset_classes=("fx", "metal")
)
ATLAS_RICH_INSTRUMENTS = research_eligible_instruments()
ATLAS_STANDARD_TIMEFRAMES = tuple(DEFAULT_SIGNAL_TIMEFRAMES)
ATLAS_RICH_TIMEFRAMES = ("M1", "M5", "M15", "H1")
ATLAS_WIDE_DISCOVERY_TIMEFRAMES = ("M1", "M5", "M15", "M30", "H1", "H4", "D1")

ATLAS_PROFILE_CONFIGS: dict[str, dict[str, Any]] = {
    "standard": {
        "signal_roles": list(ATLAS_STANDARD_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_STANDARD_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor"],
        "instrument_buckets": ["standard"],
        "timeframe_panel": "standard",
        "description": "Current trigger-only empirical signal/forward-response surface.",
    },
    "rich-roles": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": ["standard"],
        "timeframe_panel": "standard",
        "description": (
            "First staged rich profile: broaden empirical signal/forward-response evidence "
            "across roles while retaining the current instrument and timeframe panel."
        ),
    },
    "rich-timeframes": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_RICH_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": ["standard"],
        "timeframe_panel": "m1_m5_m15_h1",
        "description": (
            "Second staged rich profile: role-broadened evidence on the current market panel "
            "with expanded timeframe coverage."
        ),
    },
    "rich-markets": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_RICH_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": [
            "fx-major",
            "fx-minor",
            "metals",
            "energies",
            "indices-core",
            "crypto-core",
        ],
        "timeframe_panel": "standard",
        "description": (
            "Second staged rich profile alternative: role-broadened evidence across the "
            "broader representative instrument panel while retaining current timeframes."
        ),
    },
    "rich": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_RICH_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_RICH_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": [
            "fx-major",
            "fx-minor",
            "metals",
            "energies",
            "indices-core",
            "crypto-core",
        ],
        "timeframe_panel": "m1_m5_m15_h1",
        "description": (
            "Role-, instrument-, and timeframe-broadened empirical signal/forward-response "
            "surface for richer Atlas priors."
        ),
    },
    "rich-discovery": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_RICH_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_STANDARD_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_WIDE_DISCOVERY_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": [
            "fx-major",
            "fx-minor",
            "metals",
            "energies",
            "indices-core",
            "crypto-core",
        ],
        "timeframe_panel": "standard",
        "discovery_panel": "m1_m5_m15_m30_h1_h4_d1",
        "description": (
            "Rich-market evidence surface with a wider discovery-pair probe matrix. "
            "Discovery probes keep the standard market basket while spanning seven "
            "timeframes, isolating the discovery-width experiment without the full "
            "rich-timeframe local precompute cost."
        ),
    },
    "rich-plus-discovery": {
        "signal_roles": list(ATLAS_RICH_SIGNAL_ROLES),
        "signal_role": ",".join(ATLAS_RICH_SIGNAL_ROLES),
        "signal_instruments": list(ATLAS_RICH_INSTRUMENTS),
        "signal_timeframes": list(ATLAS_RICH_TIMEFRAMES),
        "discovery_instruments": list(ATLAS_STANDARD_INSTRUMENTS),
        "discovery_timeframes": list(ATLAS_WIDE_DISCOVERY_TIMEFRAMES),
        "timing_variant_sides": ["trigger", "anchor", "both"],
        "instrument_buckets": [
            "fx-major",
            "fx-minor",
            "metals",
            "energies",
            "indices-core",
            "crypto-core",
        ],
        "timeframe_panel": "m1_m5_m15_h1",
        "discovery_panel": "m1_m5_m15_m30_h1_h4_d1",
        "description": (
            "Full rich upstream evidence surface with a wider discovery-pair probe matrix. "
            "This keeps the richer role, market, and timeframe signal atlas while widening "
            "the downstream recipe-discovery timeframe panel."
        ),
    },
}

ProbeKind = Literal[
    "anchor_pair",
    "anchor_pair_timing",
    "discovery_pair",
    "discovery_recipe_validation",
    "discovery_recipe_scrutiny",
]


@dataclass
class AtlasLabRuntimeConfig:
    gateway_url: str = DEFAULT_LAB_GATEWAY_URL
    gateway_token: str | None = None
    trading_dashboard_root: Path | None = None
    worker_contract_hash: str | None = None
    worker_contract_schema: str = DEFAULT_ATLAS_LAB_WORKER_CONTRACT_SCHEMA
    active_probes: int = DEFAULT_ATLAS_LAB_ACTIVE_PROBES
    enqueue_chunk_size: int = DEFAULT_ATLAS_LAB_ENQUEUE_CHUNK
    result_batch_size: int = DEFAULT_ATLAS_LAB_RESULT_BATCH_SIZE
    max_results_per_cycle: int = DEFAULT_ATLAS_LAB_MAX_RESULTS_PER_CYCLE
    max_drain_seconds: float = DEFAULT_ATLAS_LAB_MAX_DRAIN_SECONDS
    poll_interval_seconds: float = DEFAULT_ATLAS_LAB_POLL_INTERVAL_SECONDS
    deadline_seconds: float = DEFAULT_ATLAS_LAB_DEADLINE_SECONDS
    max_attempts: int = DEFAULT_ATLAS_LAB_MAX_ATTEMPTS
    log_interval_seconds: float = DEFAULT_ATLAS_LAB_LOG_INTERVAL_SECONDS
    strict_parity: bool = True
    force: bool = False
    json_output: bool = False
    publish: bool = False
    limit: int | None = None
    signal_max_indicators: int | None = None
    signal_instrument_limit: int | None = None
    signal_timeframe_limit: int | None = None
    signal_atlas_executor: Literal["local", "gateway"] = "local"
    full_discovery_queue: bool = True
    include_detail: bool = True
    compact_probe_artifacts: bool = True
    as_of_date: str | None = None
    lake_manifest_sha256: str | None = None
    # Formal historical Level C runs are deliberately bound to one immutable
    # research generation and protocol cutover.  These remain optional for
    # exploratory Atlas work.
    research_generation_id: str | None = None
    level_c_protocol_id: str | None = None
    cutoff_key: str | None = None
    source_snapshot_sha256: str | None = None
    universe_id: str | None = None
    universe_manifest_sha256: str | None = None
    execution_plan_path: Path | None = None
    execution_plan_id: str | None = None
    resume: bool = False
    signal_lookback_months: int = 3
    discovery_cluster_min_similarity: float = DEFAULT_DISCOVERY_CLUSTER_MIN_SIMILARITY
    discovery_cluster_min_shared_partners: int = DEFAULT_DISCOVERY_CLUSTER_MIN_SHARED_PARTNERS
    discovery_cluster_max_recipes: int = DEFAULT_DISCOVERY_CLUSTER_MAX_RECIPES
    discovery_validation_included_confidence: list[str] = field(
        default_factory=lambda: list(DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_INCLUDED_CONFIDENCE)
    )
    discovery_validation_instruments: list[str] | None = None
    discovery_validation_timeframes: list[str] | None = None
    discovery_validation_max_recipes: int = DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_MAX_RECIPES
    discovery_validation_max_pairs_per_recipe: int = (
        DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_MAX_PAIRS_PER_RECIPE
    )
    discovery_validation_first_member_limit: int = (
        DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_FIRST_MEMBER_LIMIT
    )
    discovery_validation_second_member_limit: int = (
        DEFAULT_DISCOVERY_RECIPE_VALIDATION_DEFAULT_SECOND_MEMBER_LIMIT
    )
    discovery_validation_diversity_penalty_scale: float = DEFAULT_DIVERSITY_PENALTY_SCALE
    atlas_profile: str = DEFAULT_ATLAS_PROFILE
    task_attempt_id: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    )


@dataclass
class AtlasLabPaths:
    run_id: str
    run_root: Path
    indicator_dir: Path
    signal_dir: Path
    forward_dir: Path
    anchor_pair_dir: Path
    anchor_pair_timing_dir: Path
    recipe_priors_dir: Path
    discovery_pair_dir: Path
    discovery_cluster_dir: Path
    discovery_validation_dir: Path
    discovery_scrutiny_dir: Path
    final_recipe_priors_dir: Path
    metadata_path: Path
    events_path: Path
    summary_path: Path


def atlas_profile_config(profile: str | None) -> dict[str, Any]:
    name = str(profile or DEFAULT_ATLAS_PROFILE).strip().lower() or DEFAULT_ATLAS_PROFILE
    config = ATLAS_PROFILE_CONFIGS.get(name)
    if config is None:
        known = ", ".join(sorted(ATLAS_PROFILE_CONFIGS))
        raise ValueError(f"Unknown Atlas profile {profile!r}. Expected one of: {known}.")
    return {"name": name, **config}


def _limit_profile_panel(values: list[str], limit: int | None) -> list[str]:
    if limit is None:
        return list(values)
    return list(values)[: max(int(limit), 0)]


def effective_atlas_build_profile(
    profile: dict[str, Any],
    runtime: AtlasLabRuntimeConfig,
) -> dict[str, Any]:
    payload = dict(profile)
    payload["signal_instruments"] = _limit_profile_panel(
        list(profile.get("signal_instruments") or []),
        runtime.signal_instrument_limit,
    )
    payload["signal_timeframes"] = _limit_profile_panel(
        list(profile.get("signal_timeframes") or []),
        runtime.signal_timeframe_limit,
    )
    payload["discovery_instruments"] = list(
        profile.get("discovery_instruments") or ATLAS_STANDARD_INSTRUMENTS
    )
    payload["discovery_timeframes"] = list(
        profile.get("discovery_timeframes") or ATLAS_STANDARD_TIMEFRAMES
    )
    payload["signal_max_indicators"] = runtime.signal_max_indicators
    payload["bounded_for_smoke"] = any(
        value is not None
        for value in (
            runtime.signal_max_indicators,
            runtime.signal_instrument_limit,
            runtime.signal_timeframe_limit,
        )
    )
    return payload


@dataclass
class ProbeRunSpec:
    kind: ProbeKind
    source_dir: Path
    atlas_filename: str
    results_filename: str
    summary_filename: str
    manifest_schema: str
    result_fieldnames: Callable[[], list[str]]
    row_builder: Callable[..., dict[str, Any]]
    default_lookback_months: int | None = None
    queue_key: str = "queue_rows"


@dataclass
class ProbeState:
    probe_id: str
    row: dict[str, Any]
    manifest_probe: dict[str, Any]
    profile_path: Path
    output_dir: Path
    profile_payload: dict[str, Any]
    replay_request: dict[str, Any]
    aggregate_task_id: str
    detail_task_id: str | None = None
    score: AttemptScore | None = None
    snapshot: dict[str, Any] | None = None
    execution_evidence: dict[str, Any] | None = None
    terminal_outcome: dict[str, Any] | None = None
    status: str = "queued"
    error: str | None = None


@dataclass
class ProbeRunOutcome:
    kind: ProbeKind
    results_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]


@dataclass
class AtlasLabRunResult:
    run_id: str
    run_root: Path
    status: str
    summary_path: Path
    published_manifest_path: Path | None = None
    probe_summaries: list[dict[str, Any]] = field(default_factory=list)
    pipeline_summaries: list[dict[str, Any]] = field(default_factory=list)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


_EXACT_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_RESEARCH_GENERATION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _require_exact_sha256(value: Any, *, label: str) -> str:
    identity = _clean_token(value)
    if not _EXACT_SHA256_RE.fullmatch(identity):
        raise ValueError(f"Historical Atlas requires an exact {label} sha256: identity.")
    return identity


def _historical_lineage(runtime: AtlasLabRuntimeConfig) -> dict[str, str] | None:
    """Return the formal Level C lineage or fail closed for historical Atlas."""
    if not runtime.as_of_date:
        return None
    execution_plan_id = _require_exact_sha256(
        runtime.execution_plan_id, label="execution_plan_id"
    )
    if runtime.execution_plan_path is None:
        raise ValueError("Historical Atlas requires one authoritative execution plan path.")
    generation_id = _clean_token(runtime.research_generation_id)
    if not _SAFE_RESEARCH_GENERATION_RE.fullmatch(generation_id):
        raise ValueError("Historical Atlas requires a safe, explicit research_generation_id.")
    cutoff_key = _clean_token(runtime.cutoff_key)
    if cutoff_key not in {"A", "B", "C", "D"}:
        raise ValueError("Historical Atlas requires cutoff_key to be one of A, B, C, or D.")
    universe = universe_provenance()
    universe_id = _clean_token(runtime.universe_id)
    if universe_id != str(universe["universe_id"]):
        raise ValueError("Historical Atlas universe_id does not match the active universe contract.")
    universe_manifest_sha256 = _require_exact_sha256(
        runtime.universe_manifest_sha256, label="universe_manifest_sha256"
    )
    if universe_manifest_sha256 != str(universe["universe_hash"]):
        raise ValueError(
            "Historical Atlas universe_manifest_sha256 does not match the active universe contract."
        )
    return {
        "research_generation_id": generation_id,
        "level_c_protocol_id": _require_exact_sha256(
            runtime.level_c_protocol_id, label="level_c_protocol_id"
        ),
        "cutoff_key": cutoff_key,
        "as_of_date": canonical_timestamp(runtime.as_of_date),
        "lake_manifest_sha256": _require_exact_sha256(
            runtime.lake_manifest_sha256, label="lake_manifest_sha256"
        ),
        "source_snapshot_sha256": _require_exact_sha256(
            runtime.source_snapshot_sha256, label="source_snapshot_sha256"
        ),
        "universe_id": universe_id,
        "universe_manifest_sha256": universe_manifest_sha256,
        "execution_plan_id": execution_plan_id,
    }


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _safe_stage_summary(name: str, result: Any | None) -> dict[str, Any]:
    if isinstance(result, dict):
        payload = result
    elif result is not None and hasattr(result, "summary"):
        payload = getattr(result, "summary")
    else:
        payload = None

    if isinstance(payload, dict):
        return {
            "stage": name,
            "status": "completed",
            "result_counts": _as_dict(payload.get("result_counts")),
            "summary_path": str(getattr(result, "summary_path", "")) if result is not None else None,
            "summary": payload,
        }
    return {
        "stage": name,
        "status": "completed" if result is not None else "skipped",
        "result_counts": {},
    }


def _write_json(path: Path, payload: Any) -> None:
    atomic_write_json(path, _json_safe(payload))


def _stage_artifact_files(*roots: Path) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        resolved = Path(root).resolve(strict=False)
        if resolved.is_file():
            files.append(resolved)
        elif resolved.is_dir():
            files.extend(path for path in resolved.rglob("*") if path.is_file())
    return files


def _probe_artifact_roots(spec: ProbeRunSpec) -> tuple[Path, ...]:
    return (
        spec.source_dir / spec.results_filename,
        spec.source_dir / spec.summary_filename,
        spec.source_dir / "results",
        spec.source_dir / "missing-manifest",
    )


def _run_durable_atlas_stage(
    *,
    journal: DurableExecutionJournal | None,
    run_root: Path,
    stage: str,
    payload: dict[str, Any],
    artifact_roots: tuple[Path, ...],
    action: Callable[[], Any],
) -> tuple[Any | None, bool]:
    """Run one Atlas stage or reuse only its receipt-verified exact artifacts."""
    if journal is None:
        return action(), False
    task_id = canonical_sha256({"execution": journal.execution_id, "stage": stage})
    existing = _as_dict(journal.load(create=True).get("tasks")).get(task_id)
    journal.register(task_id, {"stage": stage, **payload})
    terminal = journal.terminal(task_id)
    if terminal is not None:
        receipt = _as_dict(_as_dict(terminal.get("terminal_receipt")).get("payload"))
        artifact = _as_dict(receipt.get("artifact_receipt"))
        validate_artifact_receipt(artifact)
        return None, True
    if isinstance(existing, dict):
        partial_root = run_root / "partial-stages" / f"{stage}-{_utc_stamp()}"
        for index, raw_root in enumerate(artifact_roots):
            source = Path(raw_root).resolve(strict=False)
            if not source.exists():
                continue
            destination = partial_root / f"{index:02d}-{source.name}"
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))
    result = action()
    receipt = artifact_receipt(
        _stage_artifact_files(*artifact_roots),
        root=run_root,
    )
    journal.complete(task_id, {"stage": stage, "artifact_receipt": receipt})
    return result, False


def audit_or_rewind_atlas_lab_stages(
    *,
    run_root: Path,
    execution_id: str,
    from_stage: str,
    apply: bool = False,
) -> dict[str, Any]:
    resolved_run_root = Path(run_root).expanduser().resolve(strict=False)
    journal_path = resolved_run_root / "execution-journal.json"
    if not journal_path.is_file():
        raise FileNotFoundError(f"Atlas lab execution journal not found: {journal_path}")
    if from_stage not in ATLAS_LAB_DURABLE_STAGES:
        raise ValueError(f"Unknown Atlas stage {from_stage!r}")
    payload = _load_json(journal_path)
    if not isinstance(payload, dict) or not isinstance(payload.get("tasks"), dict):
        raise DurableExecutionError("Atlas lab journal is malformed")
    if str(payload.get("execution_id") or "") != str(execution_id):
        raise DurableExecutionError("Atlas lab journal execution_id mismatch")
    stage_index = ATLAS_LAB_DURABLE_STAGES.index(from_stage)
    tasks = payload["tasks"]
    inspected: list[dict[str, Any]] = []
    rewound: list[dict[str, Any]] = []
    for stage in ATLAS_LAB_DURABLE_STAGES[stage_index:]:
        task_id = canonical_sha256({"execution": execution_id, "stage": stage})
        task = tasks.get(task_id)
        if not isinstance(task, dict):
            inspected.append({"stage": stage, "task_id": task_id, "status": "missing"})
            continue
        status = str(task.get("status") or "unknown")
        receipt_payload = _as_dict(_as_dict(task.get("terminal_receipt")).get("payload"))
        artifact = _as_dict(receipt_payload.get("artifact_receipt"))
        artifact_valid = None
        if artifact:
            try:
                validate_artifact_receipt(artifact)
                artifact_valid = True
            except DurableExecutionError:
                artifact_valid = False
        record = {
            "stage": stage,
            "task_id": task_id,
            "status": status,
            "artifact_valid": artifact_valid,
        }
        inspected.append(record)
        if status == "terminal":
            rewound.append(record)
            if apply:
                task["status"] = "pending"
                task["terminal_receipt"] = None
    if apply and rewound:
        payload["journal_identity"] = DurableExecutionJournal._identity(payload)
        atomic_write_json(journal_path, payload)
    return {
        "schema_version": "atlas_lab_stage_rewind_report_v1",
        "run_root": str(resolved_run_root),
        "journal_path": str(journal_path),
        "execution_id": str(execution_id),
        "from_stage": from_stage,
        "apply": bool(apply),
        "inspected": inspected,
        "rewound": rewound,
        "next_resume_behavior": (
            "Rewound stages are pending. On --resume, each owning stage will quarantine "
            "existing artifacts under partial-stages before rebuilding."
            if apply
            else "Dry run only. Pass --apply to mark terminal stage receipts pending."
        ),
    }


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _stamp_historical_recipe_prior_lineage(
    recipe_priors_dir: Path,
    *,
    lineage: dict[str, str],
) -> None:
    """Bind the exact PlayHand seed surface to one historical Atlas lineage.

    Recipe-prior builders are intentionally reusable for exploratory work and
    do not know Level C.  Atlas owns the formal-run boundary, so it stamps and
    immediately revalidates the final artifacts after they are built.
    """
    artifacts = (
        recipe_priors_dir / "play-hand-seed-plan.json",
        recipe_priors_dir / "recipe-priors.json",
        recipe_priors_dir / "recipe-priors-summary.json",
    )
    for path in artifacts:
        try:
            payload = _load_json(path)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Historical Atlas missing or invalid recipe-prior artifact: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Historical Atlas recipe-prior artifact must be an object: {path}")
        existing = payload.get("historical_lineage")
        if existing is not None and existing != lineage:
            raise ValueError(f"Historical Atlas recipe-prior lineage conflicts: {path}")
        payload["historical_lineage"] = lineage
        _write_json(path, payload)
    for path in artifacts:
        observed = _load_json(path)
        if not isinstance(observed, dict) or observed.get("historical_lineage") != lineage:
            raise ValueError(f"Historical Atlas recipe-prior lineage was not persisted: {path}")
    lineage_path = recipe_priors_dir / "level-c-lineage.json"
    lineage_artifact = {
        "schema_version": "atlas_level_c_lineage_v1",
        "historical_lineage": lineage,
        "artifact_sha256": {
            path.name: _file_sha256(path) for path in artifacts
        },
    }
    existing_lineage = _load_json(lineage_path) if lineage_path.exists() else None
    if existing_lineage is not None and existing_lineage != lineage_artifact:
        raise ValueError(f"Historical Atlas recipe-prior lineage conflicts: {lineage_path}")
    _write_json(lineage_path, lineage_artifact)


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


_HISTORICAL_PROBE_RESULT_FIELDNAMES = (
    "evidence_plan_id",
    "observed_lake_manifest_sha256",
    "terminal_outcome",
    "terminal_reason",
)


def _probe_result_fieldnames(
    spec: ProbeRunSpec,
    rows: list[dict[str, Any]],
) -> list[str]:
    """Return the explicit CSV schema for ordinary or historical probe results.

    Historical gateway receipts add two known fields to the normal probe schema.
    Keep DictWriter's default strict handling for every other unknown field.
    """
    fieldnames = list(spec.result_fieldnames())
    if any(
        any(fieldname in row for fieldname in _HISTORICAL_PROBE_RESULT_FIELDNAMES)
        for row in rows
    ):
        fieldnames.extend(
            fieldname
            for fieldname in _HISTORICAL_PROBE_RESULT_FIELDNAMES
            if fieldname not in fieldnames
        )
    return fieldnames


def _append_event(paths: AtlasLabPaths, event: str, status: str, **payload: Any) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "status": status,
        **payload,
    }
    paths.events_path.parent.mkdir(parents=True, exist_ok=True)
    with paths.events_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n")


def _atlas_lab_runs_root(config: AppConfig) -> Path:
    return config.derived_root / ATLAS_LAB_RUNS_DIRNAME


def _run_id(value: str | None = None) -> str:
    token = _clean_token(value)
    if token and token.lower() != "auto":
        return re.sub(r"[^A-Za-z0-9_.-]+", "-", token)
    return f"{_utc_stamp()}-atlas-lab"


def build_atlas_lab_paths(config: AppConfig, *, run_id: str | None = None) -> AtlasLabPaths:
    resolved_run_id = _run_id(run_id)
    root = (_atlas_lab_runs_root(config) / resolved_run_id).resolve()
    return AtlasLabPaths(
        run_id=resolved_run_id,
        run_root=root,
        indicator_dir=root / DEFAULT_ATLAS_DIRNAME,
        signal_dir=root / DEFAULT_SIGNAL_ATLAS_DIRNAME,
        forward_dir=root / DEFAULT_FORWARD_RESPONSE_DIRNAME,
        anchor_pair_dir=root / DEFAULT_ANCHOR_PAIR_DIRNAME,
        anchor_pair_timing_dir=root / DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME,
        recipe_priors_dir=root / "recipe-priors-layer3",
        discovery_pair_dir=root / DEFAULT_DISCOVERY_PAIR_DIRNAME,
        discovery_cluster_dir=root / DEFAULT_DISCOVERY_CLUSTER_DIRNAME,
        discovery_validation_dir=root / DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME,
        discovery_scrutiny_dir=root / DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME,
        final_recipe_priors_dir=root / DEFAULT_RECIPE_PRIORS_DIRNAME,
        metadata_path=root / "atlas-lab-run.json",
        events_path=root / "atlas-lab-events.jsonl",
        summary_path=root / "atlas-lab-summary.json",
    )


def _profile_payload_from_doc(path: Path) -> dict[str, Any]:
    payload = _as_dict(_load_json(path))
    profile = payload.get("profile")
    if isinstance(profile, dict):
        return dict(profile)
    return payload


def _formal_task_profile_payload(
    profile_payload: dict[str, Any],
    *,
    runtime: AtlasLabRuntimeConfig,
) -> dict[str, Any]:
    """Return the exact profile payload formal tasks both hash and send.

    Evidence plans normalize profile snapshots before hashing.  Formal gateway
    tasks must send that same canonical payload, otherwise worker-side evidence
    validation correctly rejects the job.
    """
    payload = dict(profile_payload)
    if runtime.as_of_date:
        return normalize_evidence_profile_snapshot(payload)
    return payload


def _profile_direction_mode(profile_payload: dict[str, Any]) -> str:
    mode = str(profile_payload.get("directionMode") or profile_payload.get("direction_mode") or "both")
    return mode if mode in {"both", "long", "short"} else "both"


def _parse_sensitivity_args(args: list[Any]) -> dict[str, Any]:
    tokens = [str(value) for value in args]
    parsed: dict[str, Any] = {"instruments": []}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        next_value = tokens[index + 1] if index + 1 < len(tokens) else None
        if token == "--timeframe" and next_value:
            parsed["timeframe"] = _clean_upper(next_value)
            index += 2
            continue
        if token == "--lookback-months" and next_value:
            parsed["lookback_months"] = int(float(next_value))
            index += 2
            continue
        if token == "--as-of-date" and next_value:
            parsed["as_of_date"] = next_value
            index += 2
            continue
        if token == "--analysis-window-start" and next_value:
            parsed["analysis_window_start"] = next_value
            index += 2
            continue
        if token == "--analysis-window-end" and next_value:
            parsed["analysis_window_end"] = next_value
            index += 2
            continue
        if token == "--instrument" and next_value:
            parsed.setdefault("instruments", []).append(_clean_upper(next_value))
            index += 2
            continue
        if token == "--quality-score-preset" and next_value:
            parsed["quality_score_preset"] = _normalize_quality_score_preset(next_value)
            index += 2
            continue
        if token == "--execution-cost-mode" and next_value:
            parsed["execution_cost_mode"] = _clean_token(next_value).replace("-", "_")
            index += 2
            continue
        if token == "--output-dir" and next_value:
            parsed["output_dir"] = next_value
            index += 2
            continue
        index += 1
    if not parsed.get("instruments"):
        parsed["instruments"] = []
    return parsed


def _parse_utc_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("datetime value is required")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _subtract_calendar_months(value: datetime, months: int) -> datetime:
    total_month = value.month - int(months)
    year = value.year + (total_month - 1) // 12
    month = (total_month - 1) % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _analysis_window_from_as_of(as_of_date: str, lookback_months: int) -> tuple[str, str]:
    end = _parse_utc_datetime(as_of_date)
    start = _subtract_calendar_months(end, max(1, int(lookback_months)))
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def _normalize_quality_score_preset(value: Any) -> str:
    token = _clean_token(value).lower().replace("-", "_")
    if token == "profile_drop":
        return "profile_drop"
    return "default"


def _cost_model_payload(mode: str | None) -> dict[str, Any]:
    normalized = _clean_token(mode or DEFAULT_EXECUTION_COST_MODE).lower().replace("-", "_")
    if normalized == "none":
        return {"mode": "none", "spread_bps": 0.0, "slippage_bps": 0.0, "commission_bps": 0.0}
    if normalized == "fixed_bps":
        return {"mode": "fixed_bps", "spread_bps": 0.0, "slippage_bps": 0.0, "commission_bps": 0.0}
    return {
        "mode": "research_conservative",
        "spread_bps": 2.0,
        "slippage_bps": 1.0,
        "commission_bps": 0.5,
    }


def _resolve_trading_dashboard_root(
    *,
    config: AppConfig,
    runtime: AtlasLabRuntimeConfig,
) -> Path:
    if runtime.trading_dashboard_root is not None:
        return runtime.trading_dashboard_root.expanduser().resolve()
    configured = getattr(config.fuzzfolio, "workspace_root", None)
    if configured:
        return Path(configured).expanduser().resolve()
    return (config.repo_root.parent / "Trading-Dashboard").resolve()


def resolve_atlas_worker_contract_hash(
    *,
    config: AppConfig,
    runtime: AtlasLabRuntimeConfig,
) -> str:
    if runtime.worker_contract_hash:
        return runtime.worker_contract_hash
    root = _resolve_trading_dashboard_root(config=config, runtime=runtime)
    shared_python = root / "shared" / "python"
    if not shared_python.exists():
        raise RuntimeError(f"Trading-Dashboard shared python path not found: {shared_python}")
    for package_root in reversed(
        [
            shared_python / "fuzzfolio_core",
            shared_python / "fuzzfolio_data",
            shared_python,
        ]
    ):
        if package_root.exists() and str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
    try:
        from fuzzfolio_core.contracts.worker_contract import build_replay_worker_contract
    except Exception as exc:
        raise RuntimeError(f"Could not load FuzzFolio worker contract helpers from {shared_python}: {exc}") from exc
    return build_replay_worker_contract(repo_root=root).contract_hash


def _deep_replay_request_from_probe(
    *,
    probe_id: str,
    profile_payload: dict[str, Any],
    manifest_probe: dict[str, Any],
    row: dict[str, Any],
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
) -> dict[str, Any]:
    parsed = _parse_sensitivity_args(_as_list(manifest_probe.get("sensitivity_basket_args")))
    instruments = [
        _clean_upper(value)
        for value in (parsed.get("instruments") or _clean_token(row.get("instruments")).split(","))
        if _clean_upper(value)
    ]
    if not instruments:
        raise ValueError(f"Atlas probe {probe_id} has no instruments.")
    timeframe = _clean_upper(parsed.get("timeframe") or row.get("probe_timeframe"))
    if not timeframe:
        raise ValueError(f"Atlas probe {probe_id} has no timeframe.")
    lookback_months = int(parsed.get("lookback_months") or row.get("lookback_months") or 3)
    analysis_window_start = parsed.get("analysis_window_start")
    analysis_window_end = parsed.get("analysis_window_end")
    runtime_cutoff = _clean_token(runtime.as_of_date)
    manifest_cutoff = _clean_token(parsed.get("as_of_date"))
    if runtime_cutoff:
        expected_cutoff = _parse_utc_datetime(runtime_cutoff)
        if manifest_cutoff and _parse_utc_datetime(manifest_cutoff) != expected_cutoff:
            raise ValueError(
                f"Historical Atlas probe {probe_id} manifest cutoff {manifest_cutoff!r} "
                f"does not match runtime cutoff {runtime_cutoff!r}."
            )
        if analysis_window_end and _parse_utc_datetime(analysis_window_end) != expected_cutoff:
            raise ValueError(
                f"Historical Atlas probe {probe_id} manifest analysis window end "
                f"{analysis_window_end!r} does not match runtime cutoff {runtime_cutoff!r}."
            )
        as_of_date = runtime_cutoff
    else:
        as_of_date = manifest_cutoff
    if as_of_date and not (analysis_window_start or analysis_window_end):
        analysis_window_start, analysis_window_end = _analysis_window_from_as_of(
            str(as_of_date),
            lookback_months,
        )
    if bool(analysis_window_start) != bool(analysis_window_end):
        raise ValueError(
            f"Atlas probe {probe_id} must provide both analysis window bounds."
        )
    if as_of_date and not (analysis_window_start and analysis_window_end):
        raise ValueError(
            f"Historical Atlas probe {probe_id} did not resolve an explicit bounded analysis window."
        )
    evidence_plan = None
    if analysis_window_start and analysis_window_end:
        evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=f"atlas-probe:{probe_id}",
            evidence_role="training",
            selection_data_end=analysis_window_end,
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
            requested_horizon_months=lookback_months,
            profile_snapshot=profile_payload,
            lake_manifest_sha256=runtime.lake_manifest_sha256,
            data_availability_cutoff=analysis_window_end,
        )
    quality_score_preset = _normalize_quality_score_preset(
        parsed.get("quality_score_preset") or DEFAULT_QUALITY_SCORE_PRESET
    )
    cost_mode = _clean_token(parsed.get("execution_cost_mode") or DEFAULT_EXECUTION_COST_MODE)
    alert_threshold = _safe_float(profile_payload.get("notificationThreshold"))
    return {
        "job_id": f"{probe_id}-aggregate",
        "user_id": "autoresearch-atlas-lab",
        "profile_id": probe_id,
        "inline_profile_snapshot": profile_payload,
        "artifact_persistence": "ephemeral",
        "source_kind": "workspace_attempt",
        "client_origin": ATLAS_LAB_RUNNER,
        "retention_ttl_seconds": None,
        "retention_behavior": "ephemeral",
        "retention_reason": "autoresearch",
        "source_client_origin": ATLAS_LAB_RUNNER,
        "workspace_id": "atlas-lab",
        "workspace_attempt_id": probe_id,
        "instruments": instruments,
        "timeframe": timeframe,
        "market_data_source": "lake_bars",
        "lookback_months": None if evidence_plan else lookback_months,
        "analysis_window_start": analysis_window_start,
        "analysis_window_end": analysis_window_end,
        "evidence_plan": (
            evidence_plan.model_dump(mode="json") if evidence_plan else None
        ),
        "bar_limit": 5000,
        "alert_threshold": alert_threshold if alert_threshold is not None else 80.0,
        "view_mode": "overview",
        "direction_mode": _profile_direction_mode(profile_payload),
        "priority": "research",
        "work_class": "research_replay",
        "required_worker_contract_hash": worker_contract_hash,
        "required_worker_contract_schema": runtime.worker_contract_schema,
        "required_capabilities": ["deep_replay"],
        "options": {
            "include_entries": False,
            "include_per_instrument": True,
            "include_aggregate_matrix": True,
            "path_metrics_mode": "highlighted",
            "quality_score_preset": quality_score_preset,
            "cost_model": _cost_model_payload(cost_mode),
        },
    }


def make_deep_replay_task(
    *,
    state: ProbeState,
    runtime: AtlasLabRuntimeConfig,
) -> dict[str, Any]:
    return {
        "task_id": state.aggregate_task_id,
        "lane_id": state.probe_id,
        "attempt_id": state.probe_id,
        "task_kind": "deep_replay",
        "payload": state.replay_request,
        "required_worker_capabilities": [
            "deep_replay",
            PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
        ],
        "deadline_seconds": runtime.deadline_seconds,
        "max_attempts": runtime.max_attempts,
    }


def _best_cell_from_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    data = _as_dict(snapshot.get("data"))
    aggregate = _as_dict(data.get("aggregate")) or _as_dict(snapshot.get("aggregate"))
    best_cell = _as_dict(aggregate.get("best_cell"))
    if best_cell:
        return best_cell
    recommended_cell = _as_dict(aggregate.get("recommended_cell"))
    return recommended_cell or None


def make_deep_replay_detail_task(
    *,
    state: ProbeState,
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
) -> dict[str, Any] | None:
    cell = _best_cell_from_snapshot(state.snapshot)
    if not cell:
        return None
    stop_loss = _safe_float(cell.get("stop_loss_percent"))
    reward = _safe_float(cell.get("reward_multiple"))
    if stop_loss is None or reward is None:
        return None
    task_id = (
        f"{state.aggregate_task_id.removesuffix('-aggregate')}-detail"
        if state.aggregate_task_id
        else f"{state.probe_id}-detail"
    )
    state.detail_task_id = task_id
    request = {
        **state.replay_request,
        "job_id": task_id,
        "scope": "aggregate",
        "instrument": None,
        "stop_loss_percent": stop_loss,
        "reward_multiple": reward,
        "max_points": 1000,
        "include_visual_replay": False,
        "artifact_persistence": "ephemeral",
        "retention_behavior": "ephemeral",
        "retention_reason": "autoresearch",
        "source_client_origin": ATLAS_LAB_RUNNER,
    }
    payload = {
        "job_id": task_id,
        "parent_job_id": state.aggregate_task_id,
        "user_id": "autoresearch-atlas-lab",
        "profile_id": state.probe_id,
        "artifact_persistence": "ephemeral",
        "source_kind": "workspace_attempt",
        "client_origin": ATLAS_LAB_RUNNER,
        "retention_ttl_seconds": None,
        "retention_behavior": "ephemeral",
        "retention_reason": "autoresearch",
        "source_client_origin": ATLAS_LAB_RUNNER,
        "workspace_id": "atlas-lab",
        "workspace_attempt_id": state.probe_id,
        "request": request,
        "cache_key": f"atlas-lab:{state.probe_id}:{stop_loss:g}:{reward:g}",
        "inline_profile_snapshot": state.profile_payload,
        "required_worker_contract_hash": worker_contract_hash,
        "required_worker_contract_schema": runtime.worker_contract_schema,
        "required_capabilities": ["deep_replay_detail"],
    }
    return {
        "task_id": task_id,
        "lane_id": state.probe_id,
        "attempt_id": state.probe_id,
        "task_kind": "deep_replay_detail",
        "payload": payload,
        "required_worker_capabilities": [
            "deep_replay_detail",
            PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
        ],
        "deadline_seconds": runtime.deadline_seconds,
        "max_attempts": runtime.max_attempts,
    }


def _sensitivity_response_from_worker_result(
    worker_result: dict[str, Any],
    *,
    replay_request: dict[str, Any],
) -> dict[str, Any]:
    result = worker_result.get("result")
    data = result if isinstance(result, dict) else worker_result
    return {
        "status": str(worker_result.get("status") or "success"),
        "message": "Atlas lab deep replay completed via lab worker gateway.",
        "requested_timeframe": replay_request.get("timeframe"),
        "effective_timeframe": replay_request.get("timeframe"),
        "data": data,
    }


def _compact_sensitivity_snapshot_for_atlas(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    data = _as_dict(snapshot.get("data"))
    aggregate = _as_dict(data.get("aggregate")) or _as_dict(snapshot.get("aggregate"))
    if not aggregate:
        return {}
    keep_keys = {
        "best_cell",
        "recommended_cell",
        "behavior_summary",
        "score_lab",
        "scoreLab",
        "quality_score",
        "best_cell_path_metrics",
        "dsr",
    }
    compact_aggregate = {
        key: value for key, value in aggregate.items() if key in keep_keys
    }
    compact: dict[str, Any] = {"data": {"aggregate": compact_aggregate}}
    if "status" in snapshot:
        compact["status"] = snapshot.get("status")
    if "requested_timeframe" in snapshot:
        compact["requested_timeframe"] = snapshot.get("requested_timeframe")
    if "effective_timeframe" in snapshot:
        compact["effective_timeframe"] = snapshot.get("effective_timeframe")
    return compact


def _aggregate_from_sensitivity_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    payload = _as_dict(snapshot)
    aggregate = _as_dict(_as_dict(payload.get("data")).get("aggregate"))
    if aggregate:
        return aggregate
    return _as_dict(payload.get("aggregate"))


def _attempt_score_from_sensitivity_snapshot(snapshot: dict[str, Any] | None) -> AttemptScore:
    aggregate = _aggregate_from_sensitivity_snapshot(snapshot)
    if not aggregate:
        raise ValueError("sensitivity snapshot did not include an aggregate result")
    return build_attempt_score({"best": aggregate}, snapshot)


def _historical_execution_evidence_from_terminal(
    terminal: dict[str, Any],
    expected_plan: dict[str, Any],
) -> dict[str, Any] | None:
    if not expected_plan.get("lake_manifest_sha256"):
        return None
    receipt = terminal.get("execution_evidence")
    if not isinstance(receipt, dict):
        raise RuntimeError("Historical Atlas terminal result omitted execution_evidence")
    expected_receipt = {
        "plan_id": expected_plan.get("plan_id"),
        "profile_snapshot_sha256": expected_plan.get("profile_snapshot_sha256"),
        "execution_cell_sha256": expected_plan.get("execution_cell_sha256"),
        "observed_lake_manifest_sha256": expected_plan.get("lake_manifest_sha256"),
    }
    for key, value in expected_receipt.items():
        if receipt.get(key) != value:
            raise RuntimeError(
                f"Historical Atlas terminal receipt {key} mismatch: "
                f"expected {value!r}, observed {receipt.get(key)!r}"
            )
    return dict(receipt)


def _validated_no_valid_cell_terminal_for_probe(
    *,
    lab_result: dict[str, Any],
    state: ProbeState,
) -> dict[str, Any] | None:
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    nested = result_payload.get("result") if isinstance(result_payload.get("result"), dict) else {}
    terminal = result_payload.get("terminal_result") or nested.get("terminal_result")
    if not isinstance(terminal, dict):
        return None
    if terminal.get("outcome") != "no_valid_cell":
        raise RuntimeError(
            f"Historical Atlas probe {state.probe_id} returned unsupported terminal outcome: "
            f"{terminal.get('outcome')!r}"
        )
    if (
        terminal.get("schema") != "fuzzfolio-replay-terminal-result-v1"
        or terminal.get("status") != "nonviable"
    ):
        raise RuntimeError(
            f"Historical Atlas probe {state.probe_id} returned malformed no_valid_cell terminal result"
        )
    diagnostics = terminal.get("diagnostics")
    if not isinstance(diagnostics, dict):
        raise RuntimeError(
            f"Historical Atlas probe {state.probe_id} returned no_valid_cell without diagnostics"
        )
    expected_plan = state.replay_request.get("evidence_plan")
    expected_plan = expected_plan if isinstance(expected_plan, dict) else {}
    receipt = _historical_execution_evidence_from_terminal(terminal, expected_plan)
    if receipt is not None:
        state.execution_evidence = receipt
    state.terminal_outcome = {
        "schema": terminal.get("schema"),
        "status": terminal.get("status"),
        "outcome": terminal.get("outcome"),
        "diagnostics": diagnostics,
    }
    state.error = None
    return state.terminal_outcome


def _materialize_aggregate_result(
    *,
    state: ProbeState,
    lab_result: dict[str, Any],
    cli: FuzzfolioCli,
    strict: bool,
    compact_artifacts: bool,
) -> None:
    terminal_outcome = _validated_no_valid_cell_terminal_for_probe(
        lab_result=lab_result,
        state=state,
    )
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    expected_plan = state.replay_request.get("evidence_plan")
    expected_plan = expected_plan if isinstance(expected_plan, dict) else {}
    receipt = result_payload.get("execution_evidence")
    if expected_plan.get("lake_manifest_sha256") and terminal_outcome is None:
        if not isinstance(receipt, dict):
            raise RuntimeError("Historical Atlas probe omitted execution_evidence")
        expected_receipt = {
            "plan_id": expected_plan.get("plan_id"),
            "profile_snapshot_sha256": expected_plan.get("profile_snapshot_sha256"),
            "execution_cell_sha256": expected_plan.get("execution_cell_sha256"),
            "observed_lake_manifest_sha256": expected_plan.get("lake_manifest_sha256"),
        }
        for key, value in expected_receipt.items():
            if receipt.get(key) != value:
                raise RuntimeError(
                    f"Historical Atlas receipt {key} mismatch: expected {value!r}, observed {receipt.get(key)!r}"
                )
        state.execution_evidence = dict(receipt)
    state.output_dir.mkdir(parents=True, exist_ok=True)
    full_snapshot = _sensitivity_response_from_worker_result(
        result_payload,
        replay_request=state.replay_request,
    )
    _write_json(
        state.output_dir / "sensitivity-response.json",
        full_snapshot,
    )
    if state.execution_evidence is not None:
        _write_json(
            state.output_dir / "execution-evidence.json",
            state.execution_evidence,
        )
    request_payload = result_payload.get("request") if isinstance(result_payload.get("request"), dict) else None
    if isinstance(request_payload, dict) and not compact_artifacts:
        _write_json(state.output_dir / "deep-replay-job.json", {"request": request_payload, **result_payload})
    if terminal_outcome is not None:
        state.score = None
        state.snapshot = {
            "status": "nonviable",
            "terminal_outcome": state.terminal_outcome,
            "execution_evidence": state.execution_evidence,
        }
        if compact_artifacts:
            _write_json(state.output_dir / "sensitivity-response.json", state.snapshot)
            try:
                (state.output_dir / "deep-replay-job.json").unlink(missing_ok=True)
            except OSError:
                pass
        return
    try:
        snapshot = load_sensitivity_snapshot(state.output_dir)
        try:
            state.score = _attempt_score_from_sensitivity_snapshot(snapshot)
        except Exception:
            compare_payload = cli.score_artifact(state.output_dir)
            state.score = build_attempt_score(compare_payload, snapshot)
        state.snapshot = _compact_sensitivity_snapshot_for_atlas(snapshot)
    except Exception as exc:
        if strict:
            raise
        state.score = AttemptScore(
            primary_score=None,
            composite_score=None,
            score_basis="atlas_lab_scoring_failed",
            metrics={},
            best_summary={"error": str(exc)[:500]},
        )
        state.snapshot = _compact_sensitivity_snapshot_for_atlas(
            load_sensitivity_snapshot(state.output_dir)
        )
        state.error = str(exc)[:500]
    finally:
        if compact_artifacts:
            compact_snapshot = state.snapshot or _compact_sensitivity_snapshot_for_atlas(full_snapshot)
            if compact_snapshot is not None:
                _write_json(state.output_dir / "sensitivity-response.json", compact_snapshot)
            try:
                (state.output_dir / "deep-replay-job.json").unlink(missing_ok=True)
            except OSError:
                pass


def _materialize_detail_result(*, state: ProbeState, lab_result: dict[str, Any]) -> None:
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    cell_detail = result_payload.get("cell_detail")
    if not isinstance(cell_detail, dict):
        nested_result = result_payload.get("result")
        if isinstance(nested_result, dict):
            cell_detail = nested_result.get("cell_detail")
    if not isinstance(cell_detail, dict):
        raise RuntimeError("deep_replay_detail result did not include cell_detail")
    _write_json(state.output_dir / "best-cell-path-detail.json", cell_detail)


def _resolve_path(base: Path, value: Any) -> Path:
    path = Path(_clean_token(value))
    return path if path.is_absolute() else (base / path).resolve()


def _probe_id_for_row(row: dict[str, Any]) -> str:
    return _clean_token(row.get("probe_id") or row.get("timing_probe_id"))


def _task_namespace_for_spec(spec: ProbeRunSpec, runtime: AtlasLabRuntimeConfig) -> str:
    run_id = _clean_token(spec.source_dir.parent.name) or "atlas"
    kind = _clean_token(spec.kind) or "probe"
    digest = hashlib.sha1(str(spec.source_dir.resolve()).encode("utf-8")).hexdigest()[:8]
    attempt = _clean_token(runtime.task_attempt_id) or "attempt"
    return f"{run_id}-{kind}-{digest}-{attempt}"


def _task_id_for_probe(
    spec: ProbeRunSpec,
    probe_id: str,
    suffix: str,
    runtime: AtlasLabRuntimeConfig,
) -> str:
    return f"{_task_namespace_for_spec(spec, runtime)}-{probe_id}-{suffix}"


def _make_probe_state(
    *,
    config: AppConfig,
    spec: ProbeRunSpec,
    row: dict[str, Any],
    manifest_probe: dict[str, Any],
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
) -> ProbeState:
    probe_id = _probe_id_for_row(row)
    if not probe_id:
        raise ValueError("Atlas probe row is missing probe_id.")
    profile_path = _resolve_path(config.repo_root, manifest_probe.get("profile_path") or row.get("profile_path"))
    output_dir = _resolve_path(
        config.repo_root,
        manifest_probe.get("output_dir") or row.get("result_dir") or (spec.source_dir / "results" / probe_id),
    )
    profile_payload = _formal_task_profile_payload(
        _profile_payload_from_doc(profile_path),
        runtime=runtime,
    )
    replay_request = _deep_replay_request_from_probe(
        probe_id=probe_id,
        profile_payload=profile_payload,
        manifest_probe=manifest_probe,
        row=row,
        runtime=runtime,
        worker_contract_hash=worker_contract_hash,
    )
    aggregate_task_id = _task_id_for_probe(spec, probe_id, "aggregate", runtime)
    replay_request["job_id"] = aggregate_task_id
    return ProbeState(
        probe_id=probe_id,
        row=dict(row),
        manifest_probe=dict(manifest_probe),
        profile_path=profile_path,
        output_dir=output_dir,
        profile_payload=profile_payload,
        replay_request=replay_request,
        aggregate_task_id=aggregate_task_id,
    )


def _result_row(
    spec: ProbeRunSpec,
    state: ProbeState,
    *,
    status: str,
    error: str | None = None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "profile_id": state.probe_id,
        "output_dir": state.output_dir,
        "status": status,
        "score_payload": state.score,
        "sensitivity_snapshot": state.snapshot,
        "error": error or state.error,
    }
    if spec.kind in {"discovery_recipe_validation", "discovery_recipe_scrutiny"}:
        lookback = int(
            state.replay_request.get("lookback_months")
            or spec.default_lookback_months
            or state.row.get("lookback_months")
            or 12
        )
        kwargs["lookback_months"] = lookback
    row = spec.row_builder(state.row, **kwargs)
    if state.execution_evidence is not None:
        row["evidence_plan_id"] = state.execution_evidence.get("plan_id")
        row["observed_lake_manifest_sha256"] = state.execution_evidence.get(
            "observed_lake_manifest_sha256"
        )
    if state.terminal_outcome is not None:
        row["terminal_outcome"] = str(state.terminal_outcome.get("outcome") or "")
        diagnostics = state.terminal_outcome.get("diagnostics")
        row["terminal_reason"] = json.dumps(
            diagnostics if isinstance(diagnostics, dict) else {},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    return row


def _raise_historical_probe_failure(
    *,
    spec: ProbeRunSpec,
    task_id: str,
    error: str,
) -> None:
    raise RuntimeError(
        f"Historical {spec.kind} Atlas worker/result failed for {task_id}: {error}"
    )


def _validate_historical_probe_summary(
    *,
    spec: ProbeRunSpec,
    summary: dict[str, Any],
) -> None:
    counts = _as_dict(summary.get("result_counts"))
    selected = int(counts.get("selected") or 0)
    completed = int(counts.get("completed") or 0)
    scored = int(counts.get("scored") or 0)
    status_counts = _as_dict(counts.get("status_counts"))
    accepted_terminal_statuses = {"ok", "skipped_existing", "nonviable"}
    failed = sum(
        int(value or 0)
        for key, value in status_counts.items()
        if str(key).lower() not in accepted_terminal_statuses
    )
    accepted_terminal = sum(
        int(value or 0)
        for key, value in status_counts.items()
        if str(key).lower() in accepted_terminal_statuses
    )
    if selected and (completed != selected or failed or accepted_terminal != selected):
        raise RuntimeError(
            f"Historical {spec.kind} Atlas probe stage produced invalid accounting: "
            f"selected={selected}, completed={completed}, scored={scored}, "
            f"status_counts={status_counts}"
        )


def _validate_historical_probe_outcome(
    *,
    spec: ProbeRunSpec,
    outcome: ProbeRunOutcome | None,
) -> None:
    if outcome is None:
        return
    _validate_historical_probe_summary(spec=spec, summary=outcome.summary)


def _existing_result_row(
    *,
    spec: ProbeRunSpec,
    state: ProbeState,
    cli: FuzzfolioCli,
) -> dict[str, Any]:
    state.snapshot = load_sensitivity_snapshot(state.output_dir)
    try:
        state.score = _attempt_score_from_sensitivity_snapshot(state.snapshot)
    except Exception:
        compare_payload = cli.score_artifact(state.output_dir)
        state.score = build_attempt_score(compare_payload, state.snapshot)
    return _result_row(spec, state, status="skipped_existing")


def _retry_sleep_seconds(attempt: int, retry_base_seconds: float) -> float:
    return min(max(float(retry_base_seconds), 0.0) * max(int(attempt), 1), 30.0)


def _read_gateway_results(
    gateway: Any,
    *,
    limit: int,
    attempts: int = 5,
    retry_base_seconds: float = 0.5,
) -> list[dict[str, Any]]:
    reader = getattr(gateway, "read_results", None)
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            if callable(reader):
                return reader(limit=limit)
            return gateway.drain_results(limit=limit)
        except requests.RequestException:
            if attempt >= attempts:
                raise
            time.sleep(_retry_sleep_seconds(attempt, retry_base_seconds))
    return []


def _ack_gateway_results(
    gateway: Any,
    lease_ids: list[str],
    *,
    attempts: int = 5,
    retry_base_seconds: float = 0.5,
) -> None:
    acker = getattr(gateway, "ack_results", None)
    if callable(acker):
        clean_ids = [lease_id for lease_id in lease_ids if lease_id]
        for attempt in range(1, max(1, int(attempts)) + 1):
            try:
                acker(clean_ids)
                return
            except requests.RequestException:
                if attempt >= attempts:
                    raise
                time.sleep(_retry_sleep_seconds(attempt, retry_base_seconds))


def _gateway_snapshot_with_retries(
    gateway: Any,
    *,
    attempts: int = 5,
    retry_base_seconds: float = 0.5,
) -> dict[str, Any]:
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            snapshot = gateway.snapshot()
            return snapshot if isinstance(snapshot, dict) else {}
        except requests.RequestException:
            if attempt >= attempts:
                raise
            time.sleep(_retry_sleep_seconds(attempt, retry_base_seconds))
    return {}


def _enqueue_gateway_tasks_with_retries(
    gateway: Any,
    tasks: list[dict[str, Any]],
    *,
    attempts: int = 5,
    retry_base_seconds: float = 1.0,
) -> None:
    if not tasks:
        return
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            response = gateway.enqueue_tasks(tasks)
            if isinstance(response, dict):
                accepted = response.get("accepted", response.get("enqueued"))
                if accepted is not None and int(accepted) != len(tasks):
                    rejected = response.get("rejected")
                    raise RuntimeError(
                        "Atlas gateway accepted "
                        f"{accepted} of {len(tasks)} task(s)"
                        + (f"; rejected={rejected}" if rejected is not None else "")
                    )
            return
        except requests.RequestException:
            if attempt >= attempts:
                raise
            time.sleep(_retry_sleep_seconds(attempt, retry_base_seconds))


def _gateway_metrics(snapshot: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(snapshot, dict):
        return {}
    raw = snapshot.get("metrics")
    if not isinstance(raw, dict):
        return {}
    output: dict[str, int] = {}
    for key, value in raw.items():
        try:
            output[str(key)] = int(value or 0)
        except (TypeError, ValueError):
            continue
    return output


def _metric_delta(snapshot: dict[str, Any] | None, baseline: dict[str, int], key: str) -> int:
    metrics = _gateway_metrics(snapshot)
    return max(int(metrics.get(key, 0)) - int(baseline.get(key, 0)), 0)


def _print_probe_barrier(
    *,
    spec: ProbeRunSpec,
    completed: int,
    total: int,
    inflight: int,
    pending: int,
    snapshot: dict[str, Any] | None,
) -> None:
    if not isinstance(snapshot, dict):
        snapshot = {}
    worker_slots = int(snapshot.get("worker_slots") or snapshot.get("workers") or 0)
    busy_slots = int(snapshot.get("busy_slots") or 0)
    queued = int(snapshot.get("queued_tasks") or 0)
    backlog = int(snapshot.get("result_backlog") or 0)
    sat = f"{(busy_slots / worker_slots) * 100.0:.0f}%" if worker_slots else "0%"
    print(
        "[atlas-lab] "
        f"{spec.kind} completed={completed}/{total} pending={pending} inflight={inflight} "
        f"gateway queued={queued} result_backlog={backlog} workers={busy_slots}/{worker_slots} sat={sat}",
        flush=True,
    )


def run_probe_spec_via_gateway(
    config: AppConfig,
    *,
    spec: ProbeRunSpec,
    gateway: Any,
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ProbeRunOutcome:
    atlas_path = spec.source_dir / spec.atlas_filename
    if not atlas_path.exists():
        raise FileNotFoundError(f"Missing Atlas payload at {atlas_path}")
    payload = _as_dict(_load_json(atlas_path))
    queue_rows = [row for row in _as_list(payload.get(spec.queue_key)) if isinstance(row, dict)]
    if runtime.limit is not None:
        queue_rows = queue_rows[: max(int(runtime.limit), 0)]
    manifest = _as_dict(payload.get("run_manifest"))
    manifest_probes = {
        _probe_id_for_row(row): row
        for row in _as_list(manifest.get("probes"))
        if isinstance(row, dict) and _probe_id_for_row(row)
    }
    cli = FuzzfolioCli(config.fuzzfolio)
    results: list[dict[str, Any]] = []
    pending_rows = list(queue_rows)
    active: dict[str, ProbeState] = {}
    task_to_probe: dict[str, ProbeState] = {}
    completed = 0
    baseline_snapshot = _gateway_snapshot_with_retries(gateway)
    baseline_metrics = _gateway_metrics(baseline_snapshot)
    initial_gateway_id = str(baseline_snapshot.get("gateway_id") or "") if isinstance(baseline_snapshot, dict) else ""
    last_log = 0.0

    def release_state(state: ProbeState) -> None:
        active.pop(state.probe_id, None)
        task_to_probe.pop(state.aggregate_task_id, None)
        if state.detail_task_id:
            task_to_probe.pop(state.detail_task_id, None)
        state.row = {}
        state.manifest_probe = {}
        state.profile_payload = {}
        state.replay_request = {}
        state.snapshot = None
        state.score = None

    def release_aggregate_task(state: ProbeState) -> None:
        task_to_probe.pop(state.aggregate_task_id, None)
        state.manifest_probe = {}
        state.profile_payload = {}
        state.replay_request = {}

    def persist_result_row(state: ProbeState, row: dict[str, Any], *, status: str) -> None:
        nonlocal completed
        results.append(row)
        completed += 1
        if progress_callback:
            progress_callback(
                {
                    "kind": spec.kind,
                    "completed": completed,
                    "total": len(queue_rows),
                    "probe_id": state.probe_id,
                    "status": status,
                    "score": row.get("composite_score"),
                }
            )

    def persist_finished(state: ProbeState, *, status: str, error: str | None = None) -> None:
        row = _result_row(spec, state, status=status, error=error)
        persist_result_row(state, row, status=status)

    def enqueue_more() -> None:
        capacity = max(int(runtime.active_probes), 1) - len(active)
        if capacity <= 0 or not pending_rows:
            return
        chunk: list[dict[str, Any]] = []
        while pending_rows and len(chunk) < min(capacity, max(int(runtime.enqueue_chunk_size), 1)):
            row = pending_rows.pop(0)
            probe_id = _probe_id_for_row(row)
            manifest_probe = _as_dict(manifest_probes.get(probe_id))
            if not manifest_probe:
                dummy = ProbeState(
                    probe_id=probe_id or "unknown",
                    row=dict(row),
                    manifest_probe={},
                    profile_path=spec.source_dir,
                    output_dir=spec.source_dir / "missing-manifest" / (probe_id or "unknown"),
                    profile_payload={},
                    replay_request={},
                    aggregate_task_id=f"{probe_id or 'unknown'}-aggregate",
                    error="missing manifest probe",
                )
                persist_finished(dummy, status="failed", error="missing manifest probe")
                continue
            state = _make_probe_state(
                config=config,
                spec=spec,
                row=row,
                manifest_probe=manifest_probe,
                runtime=runtime,
                worker_contract_hash=worker_contract_hash,
            )
            if (state.output_dir / "sensitivity-response.json").exists() and not runtime.force:
                if state.replay_request.get("evidence_plan"):
                    raise RuntimeError(
                        f"Historical Atlas probe {state.probe_id} cannot reuse an existing result "
                        "without gateway receipt validation."
                    )
                try:
                    row = _existing_result_row(spec=spec, state=state, cli=cli)
                    persist_result_row(state, row, status=str(row.get("status") or "skipped_existing"))
                except Exception as exc:
                    state.error = str(exc)[:500]
                    persist_finished(state, status="skipped_existing_unscored", error=state.error)
                release_state(state)
                continue
            active[state.probe_id] = state
            task_to_probe[state.aggregate_task_id] = state
            chunk.append(make_deep_replay_task(state=state, runtime=runtime))
        _enqueue_gateway_tasks_with_retries(gateway, chunk)

    enqueue_more()
    while completed < len(queue_rows):
        cycle_started = time.monotonic()
        cycle_results = 0
        while cycle_results < max(int(runtime.max_results_per_cycle), int(runtime.result_batch_size)):
            limit = min(
                max(int(runtime.result_batch_size), 1),
                max(int(runtime.max_results_per_cycle), 1) - cycle_results,
            )
            result_batch = _read_gateway_results(gateway, limit=limit)
            if not result_batch:
                break
            ack_ids: list[str] = []
            for lab_result in result_batch:
                task_id = str(lab_result.get("task_id") or "")
                state = task_to_probe.get(task_id)
                lease_id = str(lab_result.get("lease_id") or "")
                if state is None:
                    ack_ids.append(lease_id)
                    continue
                try:
                    status = str(lab_result.get("status") or "").lower()
                    worker_result = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
                    worker_status = str(worker_result.get("status") or "").lower()
                    if status in {"failed", "error"} or worker_status in {"failed", "error"}:
                        terminal_outcome = _validated_no_valid_cell_terminal_for_probe(
                            lab_result=lab_result,
                            state=state,
                        )
                        if terminal_outcome is not None and task_id == state.aggregate_task_id:
                            _materialize_aggregate_result(
                                state=state,
                                lab_result=lab_result,
                                cli=cli,
                                strict=runtime.strict_parity,
                                compact_artifacts=runtime.compact_probe_artifacts,
                            )
                            persist_finished(state, status="nonviable")
                            release_state(state)
                            ack_ids.append(lease_id)
                            continue
                        error_text = str(worker_result.get("error") or "atlas lab worker failed")
                        if runtime.as_of_date:
                            _raise_historical_probe_failure(
                                spec=spec,
                                task_id=task_id,
                                error=error_text,
                            )
                        raise RuntimeError(error_text)
                    if task_id == state.aggregate_task_id:
                        _materialize_aggregate_result(
                            state=state,
                            lab_result=lab_result,
                            cli=cli,
                            strict=runtime.strict_parity,
                            compact_artifacts=runtime.compact_probe_artifacts,
                        )
                        detail_task = (
                            make_deep_replay_detail_task(
                                state=state,
                                runtime=runtime,
                                worker_contract_hash=worker_contract_hash,
                            )
                            if runtime.include_detail
                            else None
                        )
                        if detail_task is None:
                            if runtime.include_detail and runtime.strict_parity:
                                if state.terminal_outcome is not None:
                                    persist_finished(state, status="nonviable")
                                    release_state(state)
                                    ack_ids.append(lease_id)
                                    continue
                                raise RuntimeError("Atlas aggregate did not produce a detail-capable best cell.")
                            persist_finished(state, status="ok")
                            release_state(state)
                        else:
                            task_to_probe[detail_task["task_id"]] = state
                            _enqueue_gateway_tasks_with_retries(gateway, [detail_task])
                            release_aggregate_task(state)
                    elif task_id == state.detail_task_id:
                        _materialize_detail_result(state=state, lab_result=lab_result)
                        persist_finished(state, status="ok")
                        release_state(state)
                    ack_ids.append(lease_id)
                except Exception as exc:
                    if runtime.as_of_date:
                        raise
                    state.error = str(exc)[:500]
                    persist_finished(state, status="failed", error=state.error)
                    release_state(state)
                    ack_ids.append(lease_id)
            _ack_gateway_results(gateway, ack_ids)
            cycle_results += len(result_batch)
            if len(result_batch) < limit:
                break
            if runtime.max_drain_seconds > 0 and (time.monotonic() - cycle_started) >= runtime.max_drain_seconds:
                break
        enqueue_more()
        try:
            snapshot = _gateway_snapshot_with_retries(gateway)
        except requests.RequestException:
            snapshot = None
        if initial_gateway_id and isinstance(snapshot, dict):
            current_gateway_id = str(snapshot.get("gateway_id") or "")
            if current_gateway_id and current_gateway_id != initial_gateway_id:
                raise RuntimeError("Atlas lab gateway restarted during probe run.")
            if _metric_delta(snapshot, baseline_metrics, "results_dropped") > 0:
                raise RuntimeError("Atlas lab gateway dropped probe results.")
        now = time.monotonic()
        if now - last_log >= max(float(runtime.log_interval_seconds), 0.1):
            _print_probe_barrier(
                spec=spec,
                completed=completed,
                total=len(queue_rows),
                inflight=len(active),
                pending=len(pending_rows),
                snapshot=snapshot,
            )
            last_log = now
        if cycle_results <= 0:
            time.sleep(max(float(runtime.poll_interval_seconds), 0.0))

    results.sort(
        key=lambda row: int(float(row.get("queue_rank") or row.get("timing_rank") or 1_000_000))
        if str(row.get("queue_rank") or row.get("timing_rank") or "").replace(".", "", 1).isdigit()
        else 1_000_000
    )
    results_csv_path = spec.source_dir / spec.results_filename
    summary_path = spec.source_dir / spec.summary_filename
    fieldnames = _probe_result_fieldnames(spec, results)
    scored = [row for row in results if row.get("composite_score") not in (None, "")]
    scored.sort(key=lambda row: float(row.get("composite_score") or 0.0), reverse=True)
    status_counts: dict[str, int] = {}
    for row in results:
        status = _clean_token(row.get("status")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
    if runtime.as_of_date:
        accepted_terminal_statuses = {"ok", "skipped_existing", "nonviable"}
        failed = sum(
            count
            for status, count in status_counts.items()
            if str(status).lower() not in accepted_terminal_statuses
        )
        accepted_terminal = sum(
            count
            for status, count in status_counts.items()
            if str(status).lower() in accepted_terminal_statuses
        )
        if failed or accepted_terminal != len(queue_rows):
            raise RuntimeError(
                f"Historical {spec.kind} Atlas probe stage cannot complete with "
                f"failed={failed}, scored={len(scored)}, selected={len(queue_rows)}"
            )
    _write_csv(results_csv_path, results, fieldnames)
    summary = {
        "schema_version": f"{spec.kind}_atlas_lab_results_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {"atlas_path": str(atlas_path)},
        "selection": {
            "executor": "lab_gateway",
            "active_probes": int(runtime.active_probes),
            "force": bool(runtime.force),
            "limit": runtime.limit,
        },
        "result_counts": {
            "selected": len(queue_rows),
            "completed": len(results),
            "scored": len(scored),
            "status_counts": dict(sorted(status_counts.items())),
        },
        "top_scored": scored[:20],
    }
    _write_json(summary_path, summary)
    return ProbeRunOutcome(
        kind=spec.kind,
        results_csv_path=results_csv_path,
        summary_path=summary_path,
        summary=summary,
    )


def _probe_specs(paths: AtlasLabPaths) -> list[ProbeRunSpec]:
    return [
        ProbeRunSpec(
            kind="anchor_pair",
            source_dir=paths.anchor_pair_dir,
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        ProbeRunSpec(
            kind="anchor_pair_timing",
            source_dir=paths.anchor_pair_timing_dir,
            atlas_filename="anchor-pair-timing-atlas.json",
            results_filename="anchor-pair-timing-results.csv",
            summary_filename="anchor-pair-timing-summary.json",
            manifest_schema="anchor_pair_timing_run_manifest_v1",
            result_fieldnames=_timing_results_fieldnames,
            row_builder=_timing_result_row_from_score,
            queue_key="timing_queue_rows",
        ),
        ProbeRunSpec(
            kind="discovery_pair",
            source_dir=paths.discovery_pair_dir,
            atlas_filename="discovery-pair-atlas.json",
            results_filename="discovery-pair-probe-results.csv",
            summary_filename="discovery-pair-probe-summary.json",
            manifest_schema="discovery_pair_run_manifest_v1",
            result_fieldnames=_discovery_pair_result_fieldnames,
            row_builder=_result_row_from_discovery_score,
        ),
        ProbeRunSpec(
            kind="discovery_recipe_validation",
            source_dir=paths.discovery_validation_dir,
            atlas_filename="discovery-recipe-validation-atlas.json",
            results_filename="discovery-recipe-validation-results.csv",
            summary_filename="discovery-recipe-validation-results-summary.json",
            manifest_schema="discovery_recipe_validation_run_manifest_v1",
            result_fieldnames=_discovery_recipe_result_fieldnames,
            row_builder=_result_row_from_validation_score,
            default_lookback_months=12,
        ),
        ProbeRunSpec(
            kind="discovery_recipe_scrutiny",
            source_dir=paths.discovery_scrutiny_dir,
            atlas_filename="discovery-recipe-validation-atlas.json",
            results_filename="discovery-recipe-validation-results.csv",
            summary_filename="discovery-recipe-validation-results-summary.json",
            manifest_schema="discovery_recipe_scrutiny_run_manifest_v1",
            result_fieldnames=_discovery_recipe_result_fieldnames,
            row_builder=_result_row_from_validation_score,
            default_lookback_months=36,
        ),
    ]


def publish_atlas_lab_priors(config: AppConfig, *, paths: AtlasLabPaths) -> Path:
    source = paths.final_recipe_priors_dir
    if not source.exists():
        raise FileNotFoundError(f"Missing final recipe priors directory: {source}")
    target = config.derived_root / DEFAULT_RECIPE_PRIORS_DIRNAME
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        backup = config.derived_root / f"{DEFAULT_RECIPE_PRIORS_DIRNAME}.previous-{_utc_stamp()}"
        if backup.exists():
            shutil.rmtree(backup)
        shutil.move(str(target), str(backup))
    shutil.copytree(source, target)
    manifest_path = target / "atlas-lab-publish-manifest.json"
    _write_json(
        manifest_path,
        {
            "schema_version": "atlas_lab_publish_manifest_v1",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "run_id": paths.run_id,
            "source_dir": str(source),
            "target_dir": str(target),
        },
    )
    return manifest_path


def _signal_cell_task_id(
    *,
    runtime: AtlasLabRuntimeConfig,
    indicator_id: str,
    timeframe: str,
    instrument: str,
) -> str:
    digest = hashlib.sha1(
        f"{runtime.task_attempt_id}:{indicator_id}:{timeframe}:{instrument}".encode("utf-8")
    ).hexdigest()[:12]
    return (
        f"atlas-signal-{runtime.task_attempt_id}-"
        f"{indicator_id.lower()}-{timeframe.lower()}-{instrument.lower()}-{digest}"
    )


def _make_signal_atlas_cell_task(
    *,
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
    task_id: str,
    indicator_id: str,
    profile_id: str,
    profile_payload: dict[str, Any],
    instrument: str,
    timeframe: str,
    bar_limit: int,
) -> dict[str, Any]:
    analysis_window_start = None
    analysis_window_end = None
    evidence_plan = None
    task_profile_payload = dict(profile_payload)
    if runtime.as_of_date:
        task_profile_payload = normalize_evidence_profile_snapshot(task_profile_payload)
        analysis_window_start, analysis_window_end = _analysis_window_from_as_of(
            runtime.as_of_date,
            max(int(runtime.signal_lookback_months), 1),
        )
        evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=f"atlas-signal:{task_id}",
            evidence_role="training",
            selection_data_end=analysis_window_end,
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
            requested_horizon_months=max(int(runtime.signal_lookback_months), 1),
            profile_snapshot=task_profile_payload,
            lake_manifest_sha256=runtime.lake_manifest_sha256,
            data_availability_cutoff=analysis_window_end,
        )
    payload = {
        "task_id": task_id,
        "indicator_id": indicator_id,
        "profile_id": profile_id,
        "inline_profile_snapshot": task_profile_payload,
        "instrument": instrument,
        "timeframe": timeframe,
        "bar_limit": int(bar_limit),
        "market_data_source": "lake_bars",
        "lookback_months": None if evidence_plan else runtime.signal_lookback_months,
        "analysis_window_start": analysis_window_start,
        "analysis_window_end": analysis_window_end,
        "evidence_plan": evidence_plan.model_dump(mode="json") if evidence_plan else None,
        "alert_threshold": _safe_float(task_profile_payload.get("notificationThreshold")) or 80.0,
        "direction_mode": _clean_token(task_profile_payload.get("directionMode")) or "both",
        "required_worker_contract_hash": worker_contract_hash,
        "required_worker_contract_schema": runtime.worker_contract_schema,
        "required_capabilities": ["signal_atlas_cell"],
    }
    return {
        "task_id": task_id,
        "lane_id": f"signal:{indicator_id}:{timeframe}",
        "attempt_id": task_id,
        "task_kind": "signal_atlas_cell",
        "payload": payload,
        "resolved_profile_snapshot": task_profile_payload,
        "required_worker_capabilities": [
            "signal_atlas_cell",
            PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
        ],
        "deadline_seconds": runtime.deadline_seconds,
        "max_attempts": runtime.max_attempts,
    }


def _numeric_signal_series(values: Any) -> list[float]:
    output: list[float] = []
    for value in _as_list(values):
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = 0.0
        output.append(number if math.isfinite(number) else 0.0)
    return output


def _signal_result_payload(lab_result: dict[str, Any]) -> dict[str, Any]:
    worker_state = _as_dict(lab_result.get("result"))
    nested = _as_dict(worker_state.get("result"))
    return nested or worker_state


def _row_from_signal_result(
    *,
    base_row: dict[str, Any],
    lab_result: dict[str, Any],
    raw_path: Path,
) -> dict[str, Any]:
    payload = _signal_result_payload(lab_result)
    raw = _as_dict(payload.get("raw"))
    data = _as_dict(raw.get("data"))
    if not raw or not data:
        raise RuntimeError("signal_atlas_cell result did not include raw signal data")
    evidence_plan = _as_dict(base_row.get("evidence_plan"))
    execution_evidence = _as_dict(payload.get("execution_evidence"))
    if evidence_plan.get("lake_manifest_sha256"):
        expected = {
            "plan_id": evidence_plan.get("plan_id"),
            "profile_snapshot_sha256": evidence_plan.get("profile_snapshot_sha256"),
            "execution_cell_sha256": evidence_plan.get("execution_cell_sha256"),
            "observed_lake_manifest_sha256": evidence_plan.get("lake_manifest_sha256"),
        }
        for key, value in expected.items():
            if execution_evidence.get(key) != value:
                raise RuntimeError(
                    f"Historical signal receipt {key} mismatch: expected {value!r}, observed {execution_evidence.get(key)!r}"
                )
    _write_json(raw_path, raw)
    metrics = compute_signal_metrics(
        _numeric_signal_series(data.get("long_score")),
        _numeric_signal_series(data.get("short_score")),
        timestamps=_as_list(data.get("timestamp")),
    )
    row = dict(base_row)
    row.update(metrics)
    row["status"] = "ok"
    if payload.get("analysis_status"):
        row["analysis_status"] = payload.get("analysis_status")
    row["evidence_plan_id"] = execution_evidence.get("plan_id")
    row["observed_lake_manifest_sha256"] = execution_evidence.get(
        "observed_lake_manifest_sha256"
    )
    return row


def _print_signal_atlas_barrier(
    *,
    completed: int,
    total: int,
    inflight: int,
    pending: int,
    snapshot: dict[str, Any] | None,
) -> None:
    if not isinstance(snapshot, dict):
        snapshot = {}
    worker_slots = int(snapshot.get("worker_slots") or snapshot.get("workers") or 0)
    busy_slots = int(snapshot.get("busy_slots") or 0)
    queued = int(snapshot.get("queued_tasks") or 0)
    backlog = int(snapshot.get("result_backlog") or 0)
    sat = f"{(busy_slots / worker_slots) * 100.0:.0f}%" if worker_slots else "0%"
    print(
        "[atlas-lab] "
        f"signal_atlas completed={completed}/{total} pending={pending} inflight={inflight} "
        f"gateway queued={queued} result_backlog={backlog} workers={busy_slots}/{worker_slots} sat={sat}",
        flush=True,
    )


def build_signal_atlas_via_gateway(
    config: AppConfig,
    *,
    indicator_atlas_dir: Path,
    out_dir: Path,
    signal_role: str,
    instruments: list[str],
    timeframes: list[str],
    max_indicators: int | None,
    gateway: Any,
    runtime: AtlasLabRuntimeConfig,
    worker_contract_hash: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> SignalAtlasBuildResult:
    target_dir = out_dir.expanduser().resolve()
    raw_dir = target_dir / "raw"
    profile_dir = target_dir / "profiles"
    raw_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    static_atlas_path = indicator_atlas_dir.expanduser().resolve() / "indicator-atlas.json"
    if not static_atlas_path.exists():
        raise FileNotFoundError(f"Missing indicator atlas at {static_atlas_path}")
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
    )
    catalog_by_id = _signal_indicator_catalog_by_id(catalog_payload)
    rows_by_id = _signal_atlas_rows_by_id(static_atlas_path)
    selected_signal_roles = _normalize_signal_roles_for_atlas(signal_role)
    selected_indicator_ids = _select_signal_indicator_ids(
        rows_by_id,
        indicator_ids=None,
        signal_role=signal_role,
        max_indicators=max_indicators,
    )
    instrument_panel = _ordered_unique_tokens(tuple(instruments)) or list(DEFAULT_SIGNAL_INSTRUMENTS)
    timeframe_panel = _ordered_unique_tokens(tuple(timeframes)) or list(DEFAULT_SIGNAL_TIMEFRAMES)
    bar_limit = DEFAULT_SIGNAL_BAR_LIMIT
    total_calls = len(selected_indicator_ids) * len(timeframe_panel) * len(instrument_panel)
    run_started = datetime.now(timezone.utc).isoformat()

    rows: list[dict[str, Any]] = []
    profile_records: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    active: dict[str, dict[str, Any]] = {}
    completed = 0

    def persist_row(row: dict[str, Any]) -> None:
        nonlocal completed
        rows.append(row)
        completed += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "kind": "signal_atlas",
                    "completed": completed,
                    "total": total_calls,
                    "probe_id": row.get("indicator_id"),
                    "status": row.get("status"),
                    "score": row.get("active_percent"),
                }
            )

    for indicator_id in selected_indicator_ids:
        catalog_item = catalog_by_id.get(indicator_id)
        atlas_row = rows_by_id.get(indicator_id, {})
        if not catalog_item:
            for timeframe in timeframe_panel:
                for instrument in instrument_panel:
                    persist_row(
                        {
                            "indicator_id": indicator_id,
                            "signal_role": atlas_row.get("signal_role"),
                            "strategy_role": atlas_row.get("strategy_role"),
                            "instrument": instrument,
                            "timeframe": timeframe,
                            "status": "failed",
                            "error_type": "missing_catalog_item",
                            "error": "missing_catalog_item",
                        }
                    )
            continue
        for timeframe in timeframe_panel:
            profile_path = profile_dir / f"{indicator_id.lower()}-{timeframe.lower()}.json"
            profile_doc = _profile_document_for_indicator(
                catalog_item,
                indicator_id=indicator_id,
                timeframe=timeframe,
                instruments=instrument_panel,
                label_prefix="Signal Atlas",
            )
            _write_json(profile_path, profile_doc)
            profile_payload = _as_dict(profile_doc.get("profile"))
            profile_id = f"signal-atlas-{indicator_id.lower()}-{timeframe.lower()}"
            profile_records.append(
                {
                    "indicator_id": indicator_id,
                    "timeframe": timeframe,
                    "profile_id": profile_id,
                    "profile_path": str(profile_path),
                    "created": False,
                    "deleted": False,
                    "executor": "lab_gateway",
                }
            )
            for instrument in instrument_panel:
                raw_path = raw_dir / f"{indicator_id.lower()}-{timeframe.lower()}-{instrument.lower()}.json"
                base_row = {
                    "indicator_id": indicator_id,
                    "signal_role": atlas_row.get("signal_role"),
                    "strategy_role": atlas_row.get("strategy_role"),
                    "static_prior_score": atlas_row.get("static_prior_score"),
                    "static_prior_bucket": atlas_row.get("static_prior_bucket"),
                    "instrument": instrument,
                    "timeframe": timeframe,
                    "profile_id": profile_id,
                    "raw_path": str(raw_path),
                }
                pending.append(
                    {
                        "task_id": _signal_cell_task_id(
                            runtime=runtime,
                            indicator_id=indicator_id,
                            timeframe=timeframe,
                            instrument=instrument,
                        ),
                        "indicator_id": indicator_id,
                        "profile_id": profile_id,
                        "profile_payload": profile_payload,
                        "instrument": instrument,
                        "timeframe": timeframe,
                        "raw_path": raw_path,
                        "base_row": base_row,
                    }
                )

    baseline_snapshot = _gateway_snapshot_with_retries(gateway)
    baseline_metrics = _gateway_metrics(baseline_snapshot)
    initial_gateway_id = str(baseline_snapshot.get("gateway_id") or "") if isinstance(baseline_snapshot, dict) else ""
    last_log = 0.0

    def enqueue_more() -> None:
        capacity = max(int(runtime.active_probes), 1) - len(active)
        if capacity <= 0 or not pending:
            return
        chunk: list[dict[str, Any]] = []
        while pending and len(chunk) < min(capacity, max(int(runtime.enqueue_chunk_size), 1)):
            state = pending.pop(0)
            task = _make_signal_atlas_cell_task(
                runtime=runtime,
                worker_contract_hash=worker_contract_hash,
                task_id=str(state["task_id"]),
                indicator_id=str(state["indicator_id"]),
                profile_id=str(state["profile_id"]),
                profile_payload=dict(state["profile_payload"]),
                instrument=str(state["instrument"]),
                timeframe=str(state["timeframe"]),
                bar_limit=bar_limit,
            )
            state["base_row"]["evidence_plan"] = task["payload"].get(
                "evidence_plan"
            )
            active[str(state["task_id"])] = state
            chunk.append(task)
        if chunk:
            _enqueue_gateway_tasks_with_retries(gateway, chunk)

    enqueue_more()
    while completed < total_calls:
        cycle_started = time.monotonic()
        cycle_results = 0
        while cycle_results < max(int(runtime.max_results_per_cycle), int(runtime.result_batch_size)):
            limit = min(
                max(int(runtime.result_batch_size), 1),
                max(int(runtime.max_results_per_cycle), 1) - cycle_results,
            )
            result_batch = _read_gateway_results(gateway, limit=limit)
            if not result_batch:
                break
            ack_ids: list[str] = []
            for lab_result in result_batch:
                task_id = str(lab_result.get("task_id") or "")
                lease_id = str(lab_result.get("lease_id") or "")
                state = active.pop(task_id, None)
                if state is None:
                    ack_ids.append(lease_id)
                    continue
                row = dict(state["base_row"])
                try:
                    status = str(lab_result.get("status") or "").lower()
                    worker_state = _as_dict(lab_result.get("result"))
                    worker_status = str(worker_state.get("status") or "").lower()
                    if status in {"failed", "error"} or worker_status in {"failed", "error"}:
                        error_text = str(worker_state.get("error") or "signal atlas worker failed")
                        if runtime.as_of_date:
                            raise RuntimeError(
                                f"Historical signal atlas worker failed for {task_id}: {error_text}"
                            )
                        raise RuntimeError(error_text)
                    row = _row_from_signal_result(
                        base_row=row,
                        lab_result=lab_result,
                        raw_path=Path(state["raw_path"]),
                    )
                except Exception as exc:
                    if runtime.as_of_date:
                        raise RuntimeError(
                            f"Historical signal atlas result invalid for {task_id}: {exc}"
                        ) from exc
                    row["status"] = "failed"
                    row["error_type"] = _signal_error_type(str(exc))
                    row["error"] = str(exc)[:500]
                persist_row(row)
                ack_ids.append(lease_id)
            _ack_gateway_results(gateway, ack_ids)
            cycle_results += len(result_batch)
            if len(result_batch) < limit:
                break
            if runtime.max_drain_seconds > 0 and (time.monotonic() - cycle_started) >= runtime.max_drain_seconds:
                break
        enqueue_more()
        try:
            snapshot = _gateway_snapshot_with_retries(gateway)
        except requests.RequestException:
            snapshot = None
        if initial_gateway_id and isinstance(snapshot, dict):
            current_gateway_id = str(snapshot.get("gateway_id") or "")
            if current_gateway_id and current_gateway_id != initial_gateway_id:
                raise RuntimeError("Atlas lab gateway restarted during signal atlas build.")
            if _metric_delta(snapshot, baseline_metrics, "results_dropped") > 0:
                raise RuntimeError("Atlas lab gateway dropped signal atlas results.")
        now = time.monotonic()
        if now - last_log >= max(float(runtime.log_interval_seconds), 0.1):
            _print_signal_atlas_barrier(
                completed=completed,
                total=total_calls,
                inflight=len(active),
                pending=len(pending),
                snapshot=snapshot,
            )
            last_log = now
        if cycle_results <= 0:
            time.sleep(max(float(runtime.poll_interval_seconds), 0.0))

    by_indicator: dict[str, dict[str, Any]] = {}
    for indicator_id in selected_indicator_ids:
        indicator_rows = [row for row in rows if row.get("indicator_id") == indicator_id]
        aggregate = _aggregate_signal_indicator_rows(indicator_rows)
        atlas_row = rows_by_id.get(indicator_id, {})
        by_indicator[indicator_id] = {
            "indicator_id": indicator_id,
            "signal_role": atlas_row.get("signal_role"),
            "strategy_role": atlas_row.get("strategy_role"),
            "static_prior_score": atlas_row.get("static_prior_score"),
            "static_prior_bucket": atlas_row.get("static_prior_bucket"),
            **aggregate,
        }

    successful_rows = [row for row in rows if row.get("status") == "ok"]
    failed_rows = [row for row in rows if row.get("status") != "ok"]
    if total_calls > 0 and not successful_rows:
        sample_errors = [
            str(row.get("error") or row.get("error_type") or "unknown_error")[:200]
            for row in failed_rows[:3]
        ]
        detail = "; ".join(sample_errors) if sample_errors else "no successful signal atlas cells"
        raise RuntimeError(f"Gateway signal atlas build produced zero successful cells: {detail}")
    density_counts: dict[str, int] = {}
    balance_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    for row in successful_rows:
        density = str(row.get("density_bucket") or "unknown")
        balance = str(row.get("balance_bucket") or "unknown")
        role = str(row.get("signal_role") or "unknown")
        density_counts[density] = density_counts.get(density, 0) + 1
        balance_counts[balance] = balance_counts.get(balance, 0) + 1
        role_counts[role] = role_counts.get(role, 0) + 1

    summary = {
        "schema_version": SIGNAL_ATLAS_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_started_at": run_started,
        "source": {
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
            "static_atlas_path": str(static_atlas_path),
            "static_atlas_refreshed": False,
            "executor": "lab_gateway",
        },
        "selection": {
            "signal_role": signal_role,
            "signal_roles": selected_signal_roles,
            "signal_role_filter": "all" if not selected_signal_roles else ",".join(selected_signal_roles),
            "indicator_ids": selected_indicator_ids,
            "indicator_count": len(selected_indicator_ids),
            "instruments": instrument_panel,
            "timeframes": timeframe_panel,
            "bar_limit": bar_limit,
            "replay_source": "lake_bars",
            "as_of_date": runtime.as_of_date,
            "signal_lookback_months": runtime.signal_lookback_months,
            "lake_manifest_sha256": runtime.lake_manifest_sha256,
            "total_requested_calls": total_calls,
        },
        "result_counts": {
            "successful_calls": len(successful_rows),
            "failed_calls": len(failed_rows),
            "density_bucket_counts": dict(sorted(density_counts.items())),
            "balance_bucket_counts": dict(sorted(balance_counts.items())),
            "signal_role_counts": dict(sorted(role_counts.items())),
        },
        "indicator_rollups": by_indicator,
        "profiles": profile_records,
    }
    atlas_payload = {
        "schema_version": SIGNAL_ATLAS_SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "rows": rows,
    }
    atlas_path = target_dir / "signal-atlas.json"
    csv_path = target_dir / "signal-atlas.csv"
    summary_path = target_dir / "signal-atlas-summary.json"
    issues_path = target_dir / "signal-atlas-issues.csv"
    request_manifest_path = target_dir / "request-manifest.json"
    _write_json(atlas_path, atlas_payload)
    _write_json(summary_path, summary)
    _write_json(
        request_manifest_path,
        {
            "schema_version": "signal_atlas_request_manifest_v1",
            "signal_roles": selected_signal_roles,
            "signal_role_filter": "all" if not selected_signal_roles else ",".join(selected_signal_roles),
            "indicator_count": len(selected_indicator_ids),
            "instruments": instrument_panel,
            "timeframes": timeframe_panel,
            "profiles": profile_records,
            "replay_source": "lake_bars",
            "raw_dir": str(raw_dir),
            "profile_dir": str(profile_dir),
            "executor": "lab_gateway",
            "as_of_date": runtime.as_of_date,
            "signal_lookback_months": runtime.signal_lookback_months,
            "lake_manifest_sha256": runtime.lake_manifest_sha256,
        },
    )
    _write_signal_rows_csv(csv_path, rows)
    _write_signal_issues_csv(issues_path, rows)
    return SignalAtlasBuildResult(
        atlas_path=atlas_path,
        csv_path=csv_path,
        summary_path=summary_path,
        issues_path=issues_path,
        request_manifest_path=request_manifest_path,
        summary=summary,
    )


def run_atlas_lab(
    config: AppConfig,
    *,
    run_id: str | None = None,
    runtime: AtlasLabRuntimeConfig | None = None,
    phases: list[str] | None = None,
    gateway: Any | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> AtlasLabRunResult:
    runtime = runtime or AtlasLabRuntimeConfig()
    if runtime.as_of_date and (not _clean_token(run_id) or _clean_token(run_id).lower() == "auto"):
        raise ValueError("Historical Atlas requires an explicit protocol-bound run_id.")
    profile = atlas_profile_config(runtime.atlas_profile)
    build_profile = effective_atlas_build_profile(profile, runtime)
    paths = build_atlas_lab_paths(config, run_id=run_id)
    if runtime.as_of_date and phases is not None and not phases:
        raise ValueError("Historical Atlas phases may not be empty.")
    selected_phases = set(phases or ["full"])
    if runtime.as_of_date and (runtime.publish or "publish" in selected_phases):
        raise ValueError("Historical Atlas may not publish from a formal execution phase.")
    run_full = "full" in selected_phases
    historical_build = bool(runtime.as_of_date) and (run_full or "build" in selected_phases)
    historical_lineage = _historical_lineage(runtime)
    if historical_lineage is not None:
        runtime.task_attempt_id = str(historical_lineage["execution_plan_id"])[-16:]
    if historical_build and runtime.signal_atlas_executor != "gateway":
        raise ValueError(
            "Historical Atlas requires signal_atlas_executor='gateway'; the local signal path is not evidence-bounded."
        )
    existing_metadata: dict[str, Any] = {}
    if runtime.as_of_date and paths.run_root.exists() and any(paths.run_root.iterdir()):
        if not runtime.resume:
            raise FileExistsError(
                f"Historical Atlas run already exists; pass --resume to verify and continue: "
                f"{paths.run_root}"
            )
        if not paths.metadata_path.is_file():
            raise DurableExecutionError("Historical Atlas resume is missing run metadata")
        existing_metadata = _as_dict(_load_json(paths.metadata_path))
        if existing_metadata.get("historical_lineage") != historical_lineage:
            raise DurableExecutionError("Historical Atlas resume lineage does not match")
    if runtime.as_of_date:
        if runtime.execution_plan_path is None:
            raise ValueError("Historical Atlas requires one authoritative execution plan.")
        _expected_arguments, authoritative_plan = validate_executor_runtime_binding(
            runtime.execution_plan_path,
            executor="atlas",
            observed={
                **asdict(runtime),
                "run_id": run_id,
                "phases": sorted(selected_phases),
                "execution_plan_path": runtime.execution_plan_path,
            },
            config=config,
        )
        authoritative_root = Path(
            str((authoritative_plan.get("generation") or {}).get("active_runs_root") or "")
        ).resolve(strict=False)
        if config.runs_root.resolve(strict=False) != authoritative_root:
            raise ValueError("Historical Atlas config runs_root conflicts with authoritative execution plan.")
    paths.run_root.mkdir(parents=True, exist_ok=True)
    journal = (
        DurableExecutionJournal(
            paths.run_root / "execution-journal.json",
            execution_id=str(runtime.execution_plan_id),
            lineage=historical_lineage or {},
        )
        if historical_lineage is not None
        else None
    )
    if journal is not None:
        journal.load(create=not runtime.resume)
    started_at = str(existing_metadata.get("started_at") or datetime.now(timezone.utc).isoformat())
    _write_json(
        paths.metadata_path,
        {
            "schema_version": ATLAS_LAB_RUN_SCHEMA_VERSION,
            "run_id": paths.run_id,
            "started_at": started_at,
            "universe_contract": universe_provenance(),
            "status": "running",
            "runtime": asdict(runtime),
            "historical_lineage": historical_lineage,
            "atlas_profile": profile,
            "effective_build_profile": build_profile,
            "paths": {key: str(value) for key, value in asdict(paths).items() if isinstance(value, Path)},
        },
    )
    _append_event(paths, "run", "started")
    if runtime.as_of_date:
        worker_contract_hash = _require_exact_sha256(
            runtime.worker_contract_hash,
            label="worker_contract_hash",
        )
    else:
        worker_contract_hash = resolve_atlas_worker_contract_hash(config=config, runtime=runtime)
    gateway_client = gateway or LabGatewayClient(
        base_url=runtime.gateway_url,
        token=runtime.gateway_token or load_lab_gateway_token(),
    )
    probe_summaries: list[dict[str, Any]] = []
    pipeline_summaries: list[dict[str, Any]] = []
    status = "completed"
    published_manifest_path: Path | None = None
    semantic_runtime = asdict(runtime)
    for operational_field in (
        "gateway_url",
        "gateway_token",
        "active_probes",
        "enqueue_chunk_size",
        "result_batch_size",
        "max_results_per_cycle",
        "max_drain_seconds",
        "poll_interval_seconds",
        "deadline_seconds",
        "max_attempts",
        "log_interval_seconds",
        "json_output",
        "resume",
    ):
        semantic_runtime.pop(operational_field, None)
    stage_payload = {
        "historical_lineage": historical_lineage,
        "effective_build_profile": build_profile,
        "worker_contract_hash": worker_contract_hash,
        "semantic_runtime": _json_safe(semantic_runtime),
    }

    def run_stage(
        name: str,
        roots: tuple[Path, ...],
        action: Callable[[], Any],
    ) -> tuple[Any | None, bool]:
        return _run_durable_atlas_stage(
            journal=journal,
            run_root=paths.run_root,
            stage=name,
            payload=stage_payload,
            artifact_roots=roots,
            action=action,
        )

    def record_probe(outcome: ProbeRunOutcome | None, spec: ProbeRunSpec) -> None:
        if outcome is not None:
            if runtime.as_of_date:
                _validate_historical_probe_outcome(spec=spec, outcome=outcome)
            probe_summaries.append(outcome.summary)
            return
        summary_path = spec.source_dir / spec.summary_filename
        summary = _load_json(summary_path)
        if not isinstance(summary, dict):
            raise DurableExecutionError(f"Reused Atlas probe summary is invalid: {summary_path}")
        if runtime.as_of_date:
            _validate_historical_probe_summary(spec=spec, summary=summary)
        probe_summaries.append(summary)

    def build_final_priors() -> Any:
        result = build_recipe_priors(
            config,
            indicator_atlas_dir=paths.indicator_dir,
            signal_atlas_dir=paths.signal_dir,
            forward_response_dir=paths.forward_dir,
            anchor_pair_dir=paths.anchor_pair_dir,
            anchor_pair_timing_dir=paths.anchor_pair_timing_dir,
            discovery_recipe_validation_dir=paths.discovery_validation_dir,
            discovery_recipe_scrutiny_dir=paths.discovery_scrutiny_dir,
            include_playhand_outcome_priors=False,
            out_dir=paths.final_recipe_priors_dir,
        )
        if historical_lineage is not None:
            _stamp_historical_recipe_prior_lineage(
                paths.final_recipe_priors_dir,
                lineage=historical_lineage,
            )
        return result

    try:
        if run_full or "build" in selected_phases:
            run_stage(
                "01-indicator-atlas",
                (paths.indicator_dir,),
                lambda: build_indicator_atlas(config, out_dir=paths.indicator_dir),
            )
            if runtime.signal_atlas_executor == "gateway":
                run_stage(
                    "02-signal-atlas",
                    (paths.signal_dir,),
                    lambda: build_signal_atlas_via_gateway(
                        config,
                        indicator_atlas_dir=paths.indicator_dir,
                        out_dir=paths.signal_dir,
                        signal_role=build_profile["signal_role"],
                        instruments=build_profile["signal_instruments"],
                        timeframes=build_profile["signal_timeframes"],
                        max_indicators=build_profile["signal_max_indicators"],
                        gateway=gateway_client,
                        runtime=runtime,
                        worker_contract_hash=worker_contract_hash,
                        progress_callback=progress_callback,
                    ),
                )
            else:
                run_stage(
                    "02-signal-atlas",
                    (paths.signal_dir,),
                    lambda: build_signal_atlas(
                        config,
                        indicator_atlas_dir=paths.indicator_dir,
                        out_dir=paths.signal_dir,
                        signal_role=build_profile["signal_role"],
                        instruments=build_profile["signal_instruments"],
                        timeframes=build_profile["signal_timeframes"],
                        max_indicators=build_profile["signal_max_indicators"],
                    ),
                )
            run_stage(
                "03-forward-response-atlas",
                (paths.forward_dir,),
                lambda: build_forward_response_atlas(
                    config,
                    signal_atlas_dir=paths.signal_dir,
                    out_dir=paths.forward_dir,
                ),
            )
            run_stage(
                "04-anchor-pair-atlas",
                (paths.anchor_pair_dir,),
                lambda: build_anchor_pair_atlas(
                    config,
                    indicator_atlas_dir=paths.indicator_dir,
                    signal_atlas_dir=paths.signal_dir,
                    forward_response_dir=paths.forward_dir,
                    out_dir=paths.anchor_pair_dir,
                    as_of_date=runtime.as_of_date,
                ),
            )
        specs = _probe_specs(paths)
        if run_full or "probes" in selected_phases:
            for spec in specs[:1]:
                outcome, _reused = run_stage(
                    "05-anchor-pair-probes",
                    _probe_artifact_roots(spec),
                    lambda spec=spec: run_probe_spec_via_gateway(
                        config,
                        spec=spec,
                        gateway=gateway_client,
                        runtime=runtime,
                        worker_contract_hash=worker_contract_hash,
                        progress_callback=progress_callback,
                    ),
                )
                record_probe(outcome, spec)
            run_stage(
                "06-anchor-pair-timing-atlas",
                (paths.anchor_pair_timing_dir,),
                lambda: build_anchor_pair_timing_atlas(
                    config,
                    anchor_pair_atlas_dir=paths.anchor_pair_dir,
                    out_dir=paths.anchor_pair_timing_dir,
                    variant_sides=profile["timing_variant_sides"],
                    as_of_date=runtime.as_of_date,
                ),
            )
            outcome, _reused = run_stage(
                "07-anchor-pair-timing-probes",
                _probe_artifact_roots(specs[1]),
                lambda: run_probe_spec_via_gateway(
                    config,
                    spec=specs[1],
                    gateway=gateway_client,
                    runtime=runtime,
                    worker_contract_hash=worker_contract_hash,
                    progress_callback=progress_callback,
                ),
            )
            record_probe(outcome, specs[1])
            run_stage(
                "08-recipe-priors",
                (paths.recipe_priors_dir,),
                lambda: build_recipe_priors(
                    config,
                    indicator_atlas_dir=paths.indicator_dir,
                    signal_atlas_dir=paths.signal_dir,
                    forward_response_dir=paths.forward_dir,
                    anchor_pair_dir=paths.anchor_pair_dir,
                    anchor_pair_timing_dir=paths.anchor_pair_timing_dir,
                    discovery_recipe_validation_dir=paths.discovery_validation_dir,
                    discovery_recipe_scrutiny_dir=paths.discovery_scrutiny_dir,
                    include_playhand_outcome_priors=False,
                    out_dir=paths.recipe_priors_dir,
                ),
            )
            run_stage(
                "09-discovery-pair-atlas",
                (paths.discovery_pair_dir,),
                lambda: build_discovery_pair_atlas(
                    config,
                    indicator_atlas_dir=paths.indicator_dir,
                    signal_atlas_dir=paths.signal_dir,
                    forward_response_dir=paths.forward_dir,
                    recipe_priors_dir=paths.recipe_priors_dir,
                    out_dir=paths.discovery_pair_dir,
                    instruments=build_profile["discovery_instruments"],
                    timeframes=build_profile["discovery_timeframes"],
                    full_queue=runtime.full_discovery_queue,
                    as_of_date=runtime.as_of_date,
                ),
            )
            for spec in specs[2:3]:
                outcome, _reused = run_stage(
                    "10-discovery-pair-probes",
                    _probe_artifact_roots(spec),
                    lambda spec=spec: run_probe_spec_via_gateway(
                        config,
                        spec=spec,
                        gateway=gateway_client,
                        runtime=runtime,
                        worker_contract_hash=worker_contract_hash,
                        progress_callback=progress_callback,
                    ),
                )
                record_probe(outcome, spec)
            discovery_cluster_result, discovery_cluster_reused = run_stage(
                "11-discovery-cluster-atlas",
                (paths.discovery_cluster_dir,),
                lambda: build_discovery_cluster_atlas(
                    config,
                    discovery_pair_dir=paths.discovery_pair_dir,
                    out_dir=paths.discovery_cluster_dir,
                    min_similarity=runtime.discovery_cluster_min_similarity,
                    min_shared_partners=runtime.discovery_cluster_min_shared_partners,
                    max_recipes=runtime.discovery_cluster_max_recipes,
                ),
            )
            pipeline_summaries.append(
                {
                    **_safe_stage_summary("discovery_cluster", discovery_cluster_result),
                    "status": "reused" if discovery_cluster_reused else "completed",
                }
            )
            validation_result, validation_reused = run_stage(
                "12-discovery-recipe-validation-atlas",
                (paths.discovery_validation_dir,),
                lambda: build_discovery_recipe_validation_atlas(
                    config,
                    cluster_atlas_dir=paths.discovery_cluster_dir,
                    recipe_priors_dir=paths.recipe_priors_dir,
                    out_dir=paths.discovery_validation_dir,
                    as_of_date=runtime.as_of_date,
                    included_confidence=runtime.discovery_validation_included_confidence,
                    instruments=runtime.discovery_validation_instruments,
                    timeframes=runtime.discovery_validation_timeframes,
                    max_recipes=runtime.discovery_validation_max_recipes,
                    max_pairs_per_recipe=runtime.discovery_validation_max_pairs_per_recipe,
                    first_member_limit=runtime.discovery_validation_first_member_limit,
                    second_member_limit=runtime.discovery_validation_second_member_limit,
                    diversity_penalty_scale=runtime.discovery_validation_diversity_penalty_scale,
                ),
            )
            pipeline_summaries.append(
                {
                    **_safe_stage_summary("discovery_recipe_validation", validation_result),
                    "status": "reused" if validation_reused else "completed",
                }
            )
            outcome, _reused = run_stage(
                "13-discovery-recipe-validation-probes",
                _probe_artifact_roots(specs[3]),
                lambda: run_probe_spec_via_gateway(
                    config,
                    spec=specs[3],
                    gateway=gateway_client,
                    runtime=runtime,
                    worker_contract_hash=worker_contract_hash,
                    progress_callback=progress_callback,
                ),
            )
            record_probe(outcome, specs[3])
            run_stage(
                "14-discovery-recipe-scrutiny-atlas",
                (paths.discovery_scrutiny_dir,),
                lambda: build_discovery_recipe_scrutiny_atlas(
                    config,
                    validation_atlas_dir=paths.discovery_validation_dir,
                    out_dir=paths.discovery_scrutiny_dir,
                    as_of_date=runtime.as_of_date,
                ),
            )
            outcome, _reused = run_stage(
                "15-discovery-recipe-scrutiny-probes",
                _probe_artifact_roots(specs[4]),
                lambda: run_probe_spec_via_gateway(
                    config,
                    spec=specs[4],
                    gateway=gateway_client,
                    runtime=runtime,
                    worker_contract_hash=worker_contract_hash,
                    progress_callback=progress_callback,
                ),
            )
            record_probe(outcome, specs[4])
            run_stage(
                "16-final-recipe-priors",
                (paths.final_recipe_priors_dir,),
                build_final_priors,
            )
            if runtime.publish or "publish" in selected_phases:
                published_manifest_path = publish_atlas_lab_priors(config, paths=paths)
            _append_event(paths, "run", "completed")
    except Exception as exc:
        status = "failed"
        _append_event(paths, "run", "failed", error=str(exc)[:1000])
        raise
    finally:
        completed_at = datetime.now(timezone.utc).isoformat()
        summary = {
            "schema_version": ATLAS_LAB_RUN_SCHEMA_VERSION,
            "run_id": paths.run_id,
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "run_root": str(paths.run_root),
            "atlas_profile": profile,
            "effective_build_profile": build_profile,
            "historical_lineage": historical_lineage,
            "published_manifest_path": str(published_manifest_path) if published_manifest_path else None,
            "probe_summaries": probe_summaries,
            "pipeline_summaries": pipeline_summaries,
        }
        _write_json(paths.summary_path, summary)
        metadata = _as_dict(_load_json(paths.metadata_path)) if paths.metadata_path.exists() else {}
        metadata.update(summary)
        _write_json(paths.metadata_path, metadata)
    return AtlasLabRunResult(
        run_id=paths.run_id,
        run_root=paths.run_root,
        status=status,
        summary_path=paths.summary_path,
        published_manifest_path=published_manifest_path,
        probe_summaries=probe_summaries,
        pipeline_summaries=pipeline_summaries,
    )


__all__ = [
    "ATLAS_LAB_RUNS_DIRNAME",
    "ATLAS_LAB_RUN_SCHEMA_VERSION",
    "ATLAS_LAB_RUNNER",
    "AtlasLabRuntimeConfig",
    "AtlasLabRunResult",
    "ProbeRunSpec",
    "build_atlas_lab_paths",
    "build_signal_atlas_via_gateway",
    "effective_atlas_build_profile",
    "make_deep_replay_detail_task",
    "make_deep_replay_task",
    "publish_atlas_lab_priors",
    "resolve_atlas_worker_contract_hash",
    "run_atlas_lab",
    "run_probe_spec_via_gateway",
]
