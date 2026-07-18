from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import random
import shutil
import statistics
from collections import Counter
from dataclasses import asdict, fields, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Callable, Iterable

from .evidence_plan import validate_replay_evidence_plan
from .nested_evidence import FrozenExecutionCellReceipt
from .portfolio_optimizer import (
    OptimizerCandidate,
    PortfolioOptimizerSpec,
    PortfolioSearch,
    analyze_behavioral_similarity,
    build_optimizer_candidates,
    instrument_asset_class,
    optimizer_candidate_payload,
    pearson_corr,
    run_optimizer_backend,
)


CAMPAIGN_SCHEMA_VERSION = 1
ANALYSIS_SCHEMA_VERSION = 2
STAGES = (
    "resolve",
    "corpus_health",
    "candidate_snapshot",
    "experiments",
    "temporal_validation",
    "analysis",
    "report",
)


class PortfolioResearchError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def slug(value: str) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "-" for char in str(value or "")
    )
    return "-".join(part for part in normalized.split("-") if part) or "campaign"


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def payload_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _validate_formal_nested_receipt(
    *,
    fold_id: str,
    fold: dict[str, Any],
    attempt_id: str,
    receipt_payload: dict[str, Any],
    train_plan_payload: dict[str, Any],
    outer_plan_payload: dict[str, Any],
    formal_binding: dict[str, Any],
) -> dict[str, Any]:
    """Verify the frozen cell binds the exact train and outer replay plans."""
    try:
        receipt = FrozenExecutionCellReceipt.model_validate(receipt_payload)
        train_plan = validate_replay_evidence_plan(train_plan_payload)
        outer_plan = validate_replay_evidence_plan(outer_plan_payload)
    except (TypeError, ValueError) as exc:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} has an invalid frozen cell receipt for {attempt_id}: {exc}"
        ) from exc
    for field in ("stop_loss_percent", "reward_multiple"):
        try:
            value = float(receipt.execution_cell[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise PortfolioResearchError(
                f"Nested fold {fold_id} frozen cell receipt lacks {field} for {attempt_id}"
            ) from exc
        if not math.isfinite(value) or value <= 0.0:
            raise PortfolioResearchError(
                f"Nested fold {fold_id} frozen cell receipt has invalid {field} for {attempt_id}"
            )
    if receipt.fold_id != fold_id:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} frozen cell receipt binds another fold for {attempt_id}"
        )
    if receipt.campaign_plan_id != train_plan.campaign_plan_id:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} frozen cell receipt campaign differs from train plan for {attempt_id}"
        )
    if outer_plan.campaign_plan_id != train_plan.campaign_plan_id:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} outer plan campaign differs from train plan for {attempt_id}"
        )
    if train_plan.evidence_role != "cell_selection" or outer_plan.evidence_role != "outer_test":
        raise PortfolioResearchError(
            f"Nested fold {fold_id} has incorrect train/outer evidence roles for {attempt_id}"
        )
    if receipt.train_evidence_plan_id != train_plan.plan_id:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} frozen cell receipt differs from train plan for {attempt_id}"
        )
    if receipt.profile_snapshot_sha256 != train_plan.profile_snapshot_sha256:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} frozen cell receipt profile differs from train plan for {attempt_id}"
        )
    if outer_plan.profile_snapshot_sha256 != train_plan.profile_snapshot_sha256:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} outer plan profile differs from train plan for {attempt_id}"
        )
    if (
        receipt.lake_manifest_sha256 != train_plan.lake_manifest_sha256
        or outer_plan.lake_manifest_sha256 != train_plan.lake_manifest_sha256
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} frozen cell receipt lake differs from replay plans for {attempt_id}"
        )
    if outer_plan.execution_cell_sha256 != receipt.execution_cell_sha256:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} outer plan is not bound to the frozen cell for {attempt_id}"
        )
    expected_campaign = str(formal_binding.get("campaign_plan_id") or "")
    expected_lake = str(formal_binding.get("lake_manifest_sha256") or "")
    formal_campaign_id = str(formal_binding.get("campaign_id") or "")
    formal_cohort_manifest_id = str(formal_binding.get("cohort_manifest_id") or "")
    formal_execution_plan_id = str(formal_binding.get("execution_plan_id") or "")
    expected_profiles = formal_binding.get("profile_snapshot_sha256_by_attempt_id")
    expected_profile = (
        str(expected_profiles.get(attempt_id) or "")
        if isinstance(expected_profiles, dict)
        else ""
    )
    if (
        not expected_campaign
        or not expected_lake
        or not expected_profile
        or not formal_campaign_id
        or not formal_cohort_manifest_id
        or not formal_execution_plan_id
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} formal binding is incomplete for {attempt_id}"
        )
    expected_composed_campaign = (
        f"{formal_campaign_id}:attempt-cohort:{formal_cohort_manifest_id}:"
        f"execution-plan:{formal_execution_plan_id}"
    )
    if expected_campaign != expected_composed_campaign:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} formal campaign binding is inconsistent for {attempt_id}"
        )
    if (
        receipt.campaign_plan_id != expected_campaign
        or train_plan.campaign_plan_id != expected_campaign
        or outer_plan.campaign_plan_id != expected_campaign
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} replay plans differ from the formal campaign for {attempt_id}"
        )
    if (
        receipt.profile_snapshot_sha256 != expected_profile
        or train_plan.profile_snapshot_sha256 != expected_profile
        or outer_plan.profile_snapshot_sha256 != expected_profile
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} replay plans differ from the formal profile for {attempt_id}"
        )
    if (
        receipt.lake_manifest_sha256 != expected_lake
        or train_plan.lake_manifest_sha256 != expected_lake
        or outer_plan.lake_manifest_sha256 != expected_lake
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} replay plans differ from the formal lake for {attempt_id}"
        )
    try:
        train_start = date.fromisoformat(str(fold["train_start"])[:10])
        train_end = date.fromisoformat(str(fold["train_end"])[:10]) + timedelta(days=1)
        test_start = date.fromisoformat(str(fold["test_start"])[:10])
        test_end = date.fromisoformat(str(fold["test_end"])[:10]) + timedelta(days=1)
        train_months = int(formal_binding["train_months"])
        test_months = int(formal_binding["test_months"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} formal geometry is invalid for {attempt_id}"
        ) from exc
    utc = lambda value: f"{value.isoformat()}T00:00:00Z"
    if (
        train_plan.analysis_window_start != utc(train_start)
        or train_plan.analysis_window_end != utc(train_end)
        or train_plan.selection_data_end != utc(train_end)
        or train_plan.requested_horizon_months != train_months
        or outer_plan.analysis_window_start != utc(test_start)
        or outer_plan.analysis_window_end != utc(test_end)
        or outer_plan.selection_data_end != utc(train_end)
        or outer_plan.requested_horizon_months != test_months
    ):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} replay plan geometry differs from the formal fold for {attempt_id}"
        )
    return receipt.model_dump(mode="json")


def _validate_formal_nested_train_evidence(
    *,
    fold_id: str,
    attempt_id: str,
    train_result_payload: dict[str, Any],
    train_curve_path: Path,
) -> tuple[dict[str, Any], float]:
    """Reject malformed train evidence before policy eligibility can filter it."""
    aggregate = (train_result_payload.get("data") or {}).get("aggregate")
    if not isinstance(aggregate, dict):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train result lacks an aggregate for {attempt_id}"
        )
    score_lab = aggregate.get("score_lab")
    try:
        score = float((score_lab or {}).get("score"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train result lacks a bounded score for {attempt_id}"
        ) from exc
    if not math.isfinite(score):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train result has a non-finite score for {attempt_id}"
        )
    metrics = aggregate.get("best_cell_path_metrics")
    try:
        avg_hold = float((metrics or {}).get("avg_holding_hours"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train result lacks holding metrics for {attempt_id}"
        ) from exc
    if not math.isfinite(avg_hold):
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train result has non-finite holding metrics for {attempt_id}"
        )
    for field in ("p90_holding_hours", "max_holding_hours", "path_quality"):
        value = (metrics or {}).get(field)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise PortfolioResearchError(
                f"Nested fold {fold_id} train result has malformed {field} for {attempt_id}"
            ) from exc
        if not math.isfinite(numeric):
            raise PortfolioResearchError(
                f"Nested fold {fold_id} train result has non-finite {field} for {attempt_id}"
            )
    try:
        curve_payload = json.loads(train_curve_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} cannot read bounded train curve for {attempt_id}: {exc}"
        ) from exc
    points = ((curve_payload.get("curve") or {}).get("points") or [])
    if not isinstance(points, list) or not points:
        raise PortfolioResearchError(
            f"Nested fold {fold_id} train evidence has an empty calendar curve for {attempt_id}"
        )
    for point in points:
        if not isinstance(point, dict):
            raise PortfolioResearchError(
                f"Nested fold {fold_id} train evidence has a malformed calendar curve for {attempt_id}"
            )
        try:
            date.fromisoformat(str(point.get("date") or "")[:10])
            equity = float(point.get("equity_r"))
            open_trades = float(point.get("open_trade_count"))
            closed_trades = float(point.get("closed_trade_count"))
        except (TypeError, ValueError) as exc:
            raise PortfolioResearchError(
                f"Nested fold {fold_id} train evidence has a malformed calendar curve for {attempt_id}"
            ) from exc
        if (
            not math.isfinite(equity)
            or not math.isfinite(open_trades)
            or not math.isfinite(closed_trades)
            or open_trades < 0
            or closed_trades < 0
            or not open_trades.is_integer()
            or not closed_trades.is_integer()
        ):
            raise PortfolioResearchError(
                f"Nested fold {fold_id} train evidence has a non-finite calendar curve for {attempt_id}"
            )
    return aggregate, score


def validate_artifact_manifest(
    manifest_path: Path,
    *,
    campaign_root: Path,
    required_paths: Iterable[Path] = (),
) -> dict[str, Any]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioResearchError(
            f"Missing or invalid immutable artifact manifest: {manifest_path}"
        ) from exc
    recorded = manifest.get("artifact_sha256")
    if not isinstance(recorded, dict):
        raise PortfolioResearchError(
            f"Immutable artifact manifest has no artifact hashes: {manifest_path}"
        )
    for path in required_paths:
        try:
            key = str(path.relative_to(campaign_root)).replace("\\", "/")
        except ValueError:
            key = str(path)
        expected = str(recorded.get(key) or "")
        observed = file_sha256(path)
        if not expected or observed != expected:
            raise PortfolioResearchError(
                f"Immutable campaign artifact changed or is missing: {path}"
            )
    return manifest


def write_artifact_manifest(
    manifest_path: Path,
    *,
    campaign_root: Path,
    paths: Iterable[Path],
    fields_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_sha256: dict[str, str | None] = {}
    for path in paths:
        try:
            key = str(path.relative_to(campaign_root)).replace("\\", "/")
        except ValueError:
            key = str(path)
        artifact_sha256[key] = file_sha256(path)
    payload = {
        "schema_version": 1,
        **(fields_payload or {}),
        "artifact_sha256": artifact_sha256,
    }
    write_json_immutable(manifest_path, payload)
    return payload


def process_rss_bytes() -> int | None:
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return int(counters.WorkingSetSize)
        except (AttributeError, OSError):
            return None
        return None
    try:
        statm = Path("/proc/self/statm").read_text(encoding="ascii").split()
        return int(statm[1]) * int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def write_json_atomic(path: Path, payload: Any, *, compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        (
            canonical_json(payload)
            if compact
            else json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
        ),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def write_json_immutable(path: Path, payload: Any) -> None:
    serialized = canonical_json(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            existing = canonical_json(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            existing = path.read_text(encoding="utf-8")
        if existing != serialized:
            raise PortfolioResearchError(
                f"Immutable portfolio decision already exists with different content: {path}"
            )
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)


def write_text_immutable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing != content:
            raise PortfolioResearchError(
                f"Immutable artifact already exists with different content: {path}"
            )
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def load_research_suite(path: Path, suite_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioResearchError(f"Unable to load research suite file {path}: {exc}") from exc
    suites = document.get("research_suites") or document.get("suites") or {}
    suite = suites.get(suite_name)
    if not isinstance(suite, dict):
        names = ", ".join(sorted(str(item) for item in suites)) or "none"
        raise PortfolioResearchError(
            f"Unknown portfolio research suite '{suite_name}'. Available: {names}"
        )
    resolved = json.loads(canonical_json(suite))
    validate_suite(resolved, suite_name=suite_name)
    return document, resolved


def validate_suite(suite: dict[str, Any], *, suite_name: str) -> None:
    portfolio = suite.get("portfolio") or {}
    sizes = portfolio.get("sizes") or []
    objectives = portfolio.get("objectives") or []
    if not sizes or any(int(item) <= 0 for item in sizes):
        raise PortfolioResearchError(f"Suite {suite_name} must declare positive portfolio.sizes")
    if not objectives:
        raise PortfolioResearchError(f"Suite {suite_name} must declare portfolio.objectives")
    robustness = suite.get("robustness") or {}
    if not robustness.get("seeds"):
        raise PortfolioResearchError(f"Suite {suite_name} must declare robustness.seeds")
    temporal = suite.get("temporal_validation") or {}
    if temporal.get("enabled", True):
        for key in ("train_months", "test_months", "step_months"):
            if int(temporal.get(key) or 0) <= 0:
                raise PortfolioResearchError(
                    f"Suite {suite_name} temporal_validation.{key} must be positive"
                )
        if temporal.get("nested_cell_selection", False):
            raise PortfolioResearchError(
                f"Suite {suite_name} requests temporal_validation.nested_cell_selection, "
                "but train-only execution-cell selection is not implemented"
            )
    selection_policy = suite.get("selection_policy") or {}
    if "require_zero_invalid_artifacts" in selection_policy:
        raise PortfolioResearchError(
            f"Suite {suite_name} uses retired selection_policy.require_zero_invalid_artifacts; "
            "use require_zero_unresolved_candidate_artifacts"
        )
    if "require_zero_unresolved_candidate_artifacts" in selection_policy and not isinstance(
        selection_policy["require_zero_unresolved_candidate_artifacts"], bool
    ):
        raise PortfolioResearchError(
            f"Suite {suite_name} selection_policy.require_zero_unresolved_candidate_artifacts "
            "must be boolean"
        )
    for key in (
        "minimum_consensus_selection_frequency",
        "minimum_consensus_fold_frequency",
        "minimum_consensus_core_share",
        "maximum_median_adjacent_fold_churn",
    ):
        if key not in selection_policy:
            continue
        value = float(selection_policy[key])
        if value < 0.0 or value > 1.0:
            raise PortfolioResearchError(
                f"Suite {suite_name} selection_policy.{key} must be between 0 and 1"
            )
    for key in ("minimum_consensus_core_count", "minimum_conditional_selection_count"):
        if key in selection_policy and int(selection_policy[key]) < 0:
            raise PortfolioResearchError(
                f"Suite {suite_name} selection_policy.{key} must be nonnegative"
            )
    behavioral = suite.get("behavioral_clustering") or {}
    if behavioral.get("enabled", False):
        for key in ("worst_quantile", "cluster_threshold"):
            value = float(behavioral.get(key, 0.0))
            if value < 0.0 or value > 1.0:
                raise PortfolioResearchError(
                    f"Suite {suite_name} behavioral_clustering.{key} must be between 0 and 1"
                )
        if int(behavioral.get("min_observations", 0)) <= 0:
            raise PortfolioResearchError(
                f"Suite {suite_name} behavioral_clustering.min_observations must be positive"
            )
        weights = behavioral.get("weights") or {}
        resolved_weights = [
            float(weights.get(key, 0.0))
            for key in (
                "active_overlap",
                "return_correlation",
                "downside_correlation",
                "worst_decile_correlation",
            )
        ]
        if any(value < 0.0 for value in resolved_weights) or sum(resolved_weights) <= 0.0:
            raise PortfolioResearchError(
                f"Suite {suite_name} behavioral_clustering.weights must be nonnegative with a positive total"
            )


def make_campaign_id(suite_name: str, campaign_name: str | None = None) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = slug(campaign_name or suite_name)
    return f"{timestamp}-{suffix}"


def latest_campaign_id(
    root: Path, *, suite_name: str | None = None, include_complete: bool = False
) -> str | None:
    if not root.exists():
        return None
    matches: list[Path] = []
    for item in root.iterdir():
        campaign_path = item / "campaign.json"
        if not item.is_dir() or not campaign_path.exists():
            continue
        try:
            payload = json.loads(campaign_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if suite_name and str(payload.get("suite_name")) != suite_name:
            continue
        if not include_complete and str(payload.get("status")) == "complete":
            continue
        matches.append(item)
    return max(matches, key=lambda item: item.name).name if matches else None


class CampaignLedger:
    def __init__(self, root: Path, *, campaign_id: str, suite_name: str) -> None:
        self.root = root
        self.campaign_id = campaign_id
        self.suite_name = suite_name
        self.status_path = root / "status.json"
        self.progress_path = root / "progress.jsonl"
        self.lock_path = root / ".campaign.lock"
        self._lock_fd: int | None = None
        root.mkdir(parents=True, exist_ok=True)

    def acquire(self, *, resume: bool = False) -> None:
        if resume and self.lock_path.exists():
            try:
                lock_text = self.lock_path.read_text(encoding="ascii")
                pid_text = lock_text.split("pid=", 1)[1].split()[0]
                pid = int(pid_text)
            except (OSError, ValueError, IndexError):
                pid = -1
            if pid <= 0 or not _pid_running(pid):
                self.lock_path.unlink(missing_ok=True)
        try:
            self._lock_fd = os.open(
                self.lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            os.write(self._lock_fd, f"pid={os.getpid()} started={utc_now()}\n".encode("ascii"))
        except FileExistsError as exc:
            raise PortfolioResearchError(
                f"Campaign is already locked: {self.lock_path}. Remove only after confirming no coordinator is running."
            ) from exc

    def release(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _status(self) -> dict[str, Any]:
        if self.status_path.exists():
            try:
                return json.loads(self.status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        return {
            "schema_version": CAMPAIGN_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "suite_name": self.suite_name,
            "status": "pending",
            "stages": {},
        }

    def event(self, event: str, **fields_payload: Any) -> None:
        completed = fields_payload.get("completed")
        total = fields_payload.get("total")
        telemetry: dict[str, Any] = {"memory_rss_bytes": process_rss_bytes()}
        if isinstance(completed, int) and isinstance(total, int):
            telemetry["remaining_units"] = max(total - completed, 0)
            telemetry["pending_work"] = max(total - completed, 0)
        if event in {
            "stage_start",
            "stage_done",
            "stage_skip",
            "experiment_done",
            "temporal_unit_done",
            "campaign_complete",
            "campaign_failed",
        }:
            telemetry["artifact_file_count"] = sum(
                1 for path in self.root.rglob("*") if path.is_file()
            )
        payload = {
            "generated_at": utc_now(),
            "event": event,
            **telemetry,
            **fields_payload,
        }
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)
        with self.progress_path.open("a", encoding="utf-8") as handle:
            handle.write(canonical_json(payload) + "\n")

    def stage_complete(self, stage: str, input_hash: str) -> bool:
        row = (self._status().get("stages") or {}).get(stage) or {}
        return row.get("status") == "complete" and row.get("input_hash") == input_hash

    def stage_outputs_valid(
        self,
        stage: str,
        input_hash: str,
        output_paths: Iterable[Path],
    ) -> bool:
        if not self.stage_complete(stage, input_hash):
            return False
        row = (self._status().get("stages") or {}).get(stage) or {}
        recorded = (row.get("outputs") or {}).get("artifact_sha256") or {}
        for path in output_paths:
            try:
                relative = str(path.relative_to(self.root)).replace("\\", "/")
            except ValueError:
                relative = str(path)
            if not path.is_file() or recorded.get(relative) != file_sha256(path):
                return False
        return True

    def stage_start(self, stage: str, input_payload: Any) -> str:
        input_digest = payload_hash(input_payload)
        status = self._status()
        status["status"] = "running"
        status["current_stage"] = stage
        status["updated_at"] = utc_now()
        status.setdefault("stages", {})[stage] = {
            "status": "running",
            "input_hash": input_digest,
            "started_at": utc_now(),
        }
        write_json_atomic(self.status_path, status)
        self.event("stage_start", stage=stage, input_hash=input_digest)
        return input_digest

    def stage_done(
        self,
        stage: str,
        input_hash: str,
        *,
        outputs: dict[str, Any],
        output_paths: Iterable[Path] = (),
    ) -> None:
        status = self._status()
        stage_row = status.setdefault("stages", {}).setdefault(stage, {})
        artifact_sha256: dict[str, str | None] = {}
        for path in output_paths:
            try:
                relative = str(path.relative_to(self.root)).replace("\\", "/")
            except ValueError:
                relative = str(path)
            artifact_sha256[relative] = file_sha256(path)
        stage_row.update(
            {
                "status": "complete",
                "input_hash": input_hash,
                "completed_at": utc_now(),
                "outputs": {**outputs, "artifact_sha256": artifact_sha256},
            }
        )
        status["updated_at"] = utc_now()
        status["current_stage"] = None
        write_json_atomic(self.status_path, status)
        self.event("stage_done", stage=stage, input_hash=input_hash, outputs=outputs)

    def stage_skip(self, stage: str, input_hash: str) -> None:
        self.event("stage_skip", stage=stage, input_hash=input_hash, reason="validated_complete")

    def fail(self, error: Exception) -> None:
        status = self._status()
        status["status"] = "failed"
        status["updated_at"] = utc_now()
        status["error"] = str(error)
        write_json_atomic(self.status_path, status)
        self.event("campaign_failed", error=str(error), stage=status.get("current_stage"))

    def complete(self, outputs: dict[str, Any]) -> None:
        status = self._status()
        status["status"] = "complete"
        status["updated_at"] = utc_now()
        status["completed_at"] = utc_now()
        status["current_stage"] = None
        status["outputs"] = outputs
        write_json_atomic(self.status_path, status)
        self.event("campaign_complete", outputs=outputs)


def candidate_snapshot_payload(candidate: OptimizerCandidate) -> dict[str, Any]:
    source_paths = {
        key: str(candidate.row.get(key) or "")
        for key in (
            "profile_path",
            "full_backtest_result_path_36m",
            "full_backtest_curve_path_36m",
            "full_backtest_calendar_curve_path_36m",
        )
        if candidate.row.get(key)
    }
    return {
        **optimizer_candidate_payload(candidate),
        "asset_classes": sorted(candidate.asset_classes),
        "primary_asset_class": candidate.primary_asset_class,
        "source": {
            "profile_ref": candidate.row.get("profile_ref"),
            "profile_fingerprint": candidate.row.get("profile_fingerprint"),
            "strategy_key_36m": candidate.row.get("strategy_key_36m"),
            "selected_cell_fingerprint_36m": candidate.row.get(
                "selected_cell_fingerprint_36m"
            ),
            "score_12m": candidate.row.get("score_12m"),
            "score_36m": candidate.row.get("score_36m"),
            "full_backtest_effective_start_36m": candidate.row.get(
                "full_backtest_effective_start_36m"
            ),
            "full_backtest_effective_end_36m": candidate.row.get(
                "full_backtest_effective_end_36m"
            ),
            "paths": source_paths,
            "sha256": {
                key: file_sha256(Path(value)) for key, value in source_paths.items()
            },
        },
    }


def candidate_from_snapshot(payload: dict[str, Any]) -> OptimizerCandidate:
    source = payload.get("source") or {}
    row = {
        "attempt_id": payload.get("attempt_id"),
        "candidate_name": payload.get("candidate_name"),
        "run_id": payload.get("run_id"),
        "profile_ref": source.get("profile_ref"),
        "profile_path": (source.get("paths") or {}).get("profile_path"),
    }
    return OptimizerCandidate(
        attempt_id=str(payload.get("attempt_id") or ""),
        row=row,
        instruments=[str(item) for item in payload.get("instruments") or []],
        asset_classes=set(str(item) for item in payload.get("asset_classes") or []),
        primary_asset_class=str(payload.get("primary_asset_class") or "other"),
        family=str(payload.get("family") or "unknown"),
        family_source=str(payload.get("family_source") or "snapshot_legacy"),
        lineage_id=str(payload.get("lineage_id") or "") or None,
        behavior_fingerprint=str(payload.get("behavior_fingerprint") or "") or None,
        structural_family_signature=(
            dict(payload["structural_family_signature"])
            if isinstance(payload.get("structural_family_signature"), dict)
            else None
        ),
        score=float(payload.get("score") or 0.0),
        created_at=str(payload.get("created_at") or "") or None,
        avg_hold_hours=float(payload.get("avg_hold_hours") or 0.0),
        p90_hold_hours=_optional_float(payload.get("p90_hold_hours")),
        max_hold_hours=_optional_float(payload.get("max_hold_hours")),
        path_quality=_optional_float(payload.get("path_quality")),
        stop_loss_percent=_optional_float(payload.get("stop_loss_percent")),
        trade_count=int(payload.get("trade_count") or 0),
        trades_per_month=float(payload.get("trades_per_month") or 0.0),
        dates=[str(item) for item in payload.get("dates") or []],
        daily_r=[float(item) for item in payload.get("daily_r") or []],
        open_counts=[int(item) for item in payload.get("open_counts") or []],
        closed_counts=[int(item) for item in payload.get("closed_counts") or []],
    )


def _optional_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def intersect_candidate_calendar(
    candidates: list[OptimizerCandidate],
) -> tuple[list[OptimizerCandidate], dict[str, Any]]:
    candidates = [candidate for candidate in candidates if candidate.dates]
    if not candidates:
        raise PortfolioResearchError("No candidates have source-calendar data")
    common_start = max(min(candidate.dates) for candidate in candidates)
    common_end = min(max(candidate.dates) for candidate in candidates)
    if common_start > common_end:
        raise PortfolioResearchError("Candidate calendar curves have no common date range")
    sliced = [
        slice_candidate(candidate, start=common_start, end=common_end)
        for candidate in candidates
    ]
    sliced = [candidate for candidate in sliced if candidate.dates]
    return sliced, {
        "common_effective_start": common_start,
        "common_effective_end": common_end,
        "calendar_day_count": len(
            sorted({item for candidate in sliced for item in candidate.dates})
        ),
    }


def slice_candidate(
    candidate: OptimizerCandidate,
    *,
    start: str,
    end: str,
) -> OptimizerCandidate:
    indexes = [
        index
        for index, date_text in enumerate(candidate.dates)
        if start <= date_text <= end
    ]
    return replace(
        candidate,
        dates=[candidate.dates[index] for index in indexes],
        daily_r=[candidate.daily_r[index] for index in indexes],
        open_counts=[candidate.open_counts[index] for index in indexes],
        closed_counts=[candidate.closed_counts[index] for index in indexes],
        vector=[],
        open_vector=[],
        closed_vector=[],
        month_vector=[],
        week_vector=[],
    )


def slice_candidates(
    candidates: list[OptimizerCandidate], *, start: str, end: str
) -> list[OptimizerCandidate]:
    return [
        sliced
        for candidate in candidates
        if (sliced := slice_candidate(candidate, start=start, end=end)).dates
    ]


def add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    month_days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                  31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return date(year, month, min(value.day, month_days[month - 1]))


def temporal_folds(
    *,
    start: str,
    end: str,
    train_months: int,
    test_months: int,
    step_months: int,
    embargo_days: int,
) -> list[dict[str, Any]]:
    overall_start = date.fromisoformat(start[:10])
    overall_end = date.fromisoformat(end[:10])
    embargo = max(0, embargo_days)
    train_start = overall_start
    test_start: date | None = None
    rows: list[dict[str, Any]] = []
    while True:
        if test_start is None:
            train_end = add_months(train_start, train_months) - timedelta(days=1)
            test_start = train_end + timedelta(days=embargo + 1)
        else:
            test_start = add_months(test_start, step_months)
            train_end = test_start - timedelta(days=embargo + 1)
            train_start = add_months(train_end + timedelta(days=1), -train_months)
        test_end = add_months(test_start, test_months) - timedelta(days=1)
        if test_end > overall_end:
            break
        rows.append(
            {
                "fold_id": f"fold-{len(rows) + 1:02d}",
                "train_start": train_start.isoformat(),
                "train_end": train_end.isoformat(),
                "test_start": test_start.isoformat(),
                "test_end": test_end.isoformat(),
                "embargo_days": embargo,
            }
        )
    return rows


def _spec_kwargs(base_args: dict[str, Any]) -> dict[str, Any]:
    allowed = {field.name for field in fields(PortfolioOptimizerSpec)}
    aliases = {
        "allowed_asset_class": "allowed_asset_classes",
        "allowed_instrument": "allowed_instruments",
        "blocked_instrument": "blocked_instruments",
        "objective": "objective_names",
    }
    output: dict[str, Any] = {}
    tuple_fields = {
        "allowed_asset_classes",
        "allowed_instruments",
        "blocked_instruments",
        "objective_names",
        "baseline_attempt_ids",
        "required_attempt_ids",
    }
    for key, value in base_args.items():
        normalized = aliases.get(key, key)
        if normalized not in allowed:
            raise PortfolioResearchError(f"Unsupported optimizer suite key: {key}")
        output[normalized] = tuple(value) if normalized in tuple_fields else value
    return output


def expand_experiments(suite: dict[str, Any]) -> list[dict[str, Any]]:
    portfolio = suite.get("portfolio") or {}
    robustness = suite.get("robustness") or {}
    sizes = [int(item) for item in portfolio.get("sizes") or []]
    objectives = [str(item) for item in portfolio.get("objectives") or []]
    seeds = [int(item) for item in robustness.get("seeds") or []]
    candidate_limits = [
        int(item)
        for item in robustness.get("candidate_limits")
        or [(portfolio.get("base_optimizer_args") or {}).get("candidate_limit", 120)]
    ]
    risk_multipliers = [
        float(item) for item in robustness.get("risk_weight_multipliers") or [1.0]
    ]
    diversification_profiles = robustness.get("diversification_profiles") or [
        {"name": "base"}
    ]
    rows: list[dict[str, Any]] = []
    for size in sizes:
        for objective in objectives:
            for seed in seeds:
                for candidate_limit in candidate_limits:
                    for risk_multiplier in risk_multipliers:
                        for profile in diversification_profiles:
                            payload = {
                                "portfolio_size": size,
                                "objective": objective,
                                "random_seed": seed,
                                "candidate_limit": candidate_limit,
                                "risk_weight_multiplier": risk_multiplier,
                                "diversification_profile": profile,
                            }
                            rows.append(
                                {
                                    "experiment_id": payload_hash(payload)[:16],
                                    **payload,
                                }
                            )
    return rows


def build_experiment_spec(
    suite: dict[str, Any],
    experiment: dict[str, Any],
    *,
    account: dict[str, Any],
    name_prefix: str,
) -> PortfolioOptimizerSpec:
    portfolio = suite.get("portfolio") or {}
    kwargs = _spec_kwargs(portfolio.get("base_optimizer_args") or {})
    profile = experiment.get("diversification_profile") or {}
    kwargs.update(
        {
            key: value
            for key, value in profile.items()
            if key
            in {
                "correlation_penalty_weight",
                "diversification_mode",
                "portfolio_sharpe_weight",
                "max_instrument_share",
                "max_per_family",
            }
        }
    )
    kwargs.update(
        {
            "portfolio_name": f"{name_prefix}-{experiment['experiment_id']}",
            "portfolio_size": int(experiment["portfolio_size"]),
            "objective_names": (str(experiment["objective"]),),
            "random_seed": int(experiment["random_seed"]),
            "candidate_limit": int(experiment["candidate_limit"]),
            "risk_weight_multiplier": float(experiment["risk_weight_multiplier"]),
            "account": dict(account),
        }
    )
    return PortfolioOptimizerSpec(**kwargs)


def run_atomic_experiment(
    *,
    candidates: list[OptimizerCandidate],
    spec: PortfolioOptimizerSpec,
    experiment: dict[str, Any],
    output_dir: Path,
    backend: str,
    progress: Callable[[dict[str, Any]], None] | None,
    rank_on_slice: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    spec_payload = {**asdict(spec), "objective_names": list(spec.objective_names)}
    input_payload = {
        "experiment": experiment,
        "spec": spec_payload,
        "candidate_snapshot_hash": payload_hash(
            [candidate.attempt_id for candidate in candidates]
        ),
    }
    input_digest = payload_hash(input_payload)
    status_path = output_dir / "status.json"
    result_path = output_dir / "result.json"
    if status_path.exists() and result_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
            if status.get("status") == "complete" and status.get("input_hash") == input_digest:
                return json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    write_json_atomic(output_dir / "spec.json", input_payload)
    write_json_atomic(
        status_path,
        {"status": "running", "input_hash": input_digest, "started_at": utc_now()},
    )

    def emit(event: dict[str, Any]) -> None:
        if progress is not None:
            progress({"experiment_id": experiment["experiment_id"], **event})

    ranked_candidates = [
        candidate
        for candidate in candidates
        if (
            (rank_on_slice or candidate.score >= spec.min_score)
            and (
                not spec.require_positive_source_return
                or candidate.final_r > 0.0
            )
        )
    ]
    if rank_on_slice:
        ranked_candidates.sort(
            key=lambda candidate: (
                candidate.final_r / max(candidate.maxdd_r, 0.25),
                candidate.final_r,
                -candidate.maxdd_r,
            ),
            reverse=True,
        )
    if spec.candidate_limit > 0:
        ranked_candidates = ranked_candidates[: spec.candidate_limit]
    search, variants, pareto_front, used_backend = run_optimizer_backend(
        ranked_candidates,
        spec,
        backend=backend,
        progress_callback=emit,
    )
    objective = str(experiment["objective"])
    variant = variants.get(objective)
    if not variant:
        raise PortfolioResearchError(
            f"Experiment {experiment['experiment_id']} produced no {objective} variant"
        )
    selected_ids = [str(item) for item in variant.get("selected_attempt_ids") or []]
    result = {
        "experiment_id": experiment["experiment_id"],
        "experiment": experiment,
        "backend": used_backend,
        "selected_attempt_ids": selected_ids,
        "objective_score": variant.get("objective_score"),
        "metrics": search.metrics(selected_ids, include_correlation=True),
        "selected": variant.get("selected") or [],
        "pareto_front_count": len(pareto_front),
        "completed_at": utc_now(),
    }
    write_json_atomic(result_path, result, compact=True)
    write_json_atomic(
        status_path,
        {
            "status": "complete",
            "input_hash": input_digest,
            "completed_at": utc_now(),
            "result": str(result_path),
        },
    )
    return result


def run_experiment_matrix(
    *,
    candidates: list[OptimizerCandidate],
    suite: dict[str, Any],
    account: dict[str, Any],
    root: Path,
    backend: str,
    experiment_limit: int | None,
    ledger: CampaignLedger,
) -> list[dict[str, Any]]:
    matrix = expand_experiments(suite)
    if experiment_limit is not None and int(experiment_limit) >= 0:
        matrix = matrix[: int(experiment_limit)]
    results: list[dict[str, Any]] = []
    total = len(matrix)
    for index, experiment in enumerate(matrix, start=1):
        ledger.event(
            "experiment_start",
            experiment_id=experiment["experiment_id"],
            completed=index - 1,
            total=total,
        )
        spec = build_experiment_spec(
            suite,
            experiment,
            account=account,
            name_prefix=ledger.campaign_id,
        )
        result = run_atomic_experiment(
            candidates=candidates,
            spec=spec,
            experiment=experiment,
            output_dir=root / experiment["experiment_id"],
            backend=backend,
            progress=lambda event: ledger.event(
                "optimizer_progress",
                optimizer_event=event.get("event"),
                **{key: value for key, value in event.items() if key != "event"},
            ),
        )
        results.append(result)
        ledger.event(
            "experiment_done",
            experiment_id=experiment["experiment_id"],
            completed=index,
            total=total,
        )
    write_json_atomic(root / "experiment-index.json", results, compact=True)
    return results


def run_temporal_validation(
    *,
    candidates: list[OptimizerCandidate],
    suite: dict[str, Any],
    account: dict[str, Any],
    root: Path,
    backend: str,
    ledger: CampaignLedger,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    temporal = suite.get("temporal_validation") or {}
    if not temporal.get("enabled", True):
        write_json_atomic(root / "fold-index.json", [])
        write_json_atomic(root / "temporal-results.json", [], compact=True)
        return [], []
    all_dates = sorted({item for candidate in candidates for item in candidate.dates})
    if not all_dates:
        raise PortfolioResearchError("Temporal validation has no candidate dates")
    folds = temporal_folds(
        start=all_dates[0],
        end=all_dates[-1],
        train_months=int(temporal["train_months"]),
        test_months=int(temporal["test_months"]),
        step_months=int(temporal["step_months"]),
        embargo_days=int(temporal.get("embargo_days") or 0),
    )
    if not folds:
        raise PortfolioResearchError(
            "Configured temporal fold geometry does not fit the available calendar range"
        )
    portfolio = suite.get("portfolio") or {}
    objectives = [str(item) for item in portfolio.get("objectives") or []]
    sizes = [int(item) for item in portfolio.get("sizes") or []]
    temporal_seeds = [
        int(item)
        for item in temporal.get("seeds")
        or [(suite.get("robustness") or {}).get("seeds", [17])[0]]
    ]
    base_limit = int(
        (portfolio.get("base_optimizer_args") or {}).get("candidate_limit", 120)
    )
    results: list[dict[str, Any]] = []
    units = len(folds) * len(sizes) * len(objectives) * len(temporal_seeds)
    completed = 0
    for fold in folds:
        train_candidates = slice_candidates(
            candidates, start=fold["train_start"], end=fold["train_end"]
        )
        test_candidates = slice_candidates(
            candidates, start=fold["test_start"], end=fold["test_end"]
        )
        test_by_id = {candidate.attempt_id: candidate for candidate in test_candidates}
        fold_root = root / fold["fold_id"]
        write_json_atomic(fold_root / "fold.json", fold)
        for size in sizes:
            for objective in objectives:
                for seed in temporal_seeds:
                    experiment = {
                        "fold_id": fold["fold_id"],
                        "portfolio_size": size,
                        "objective": objective,
                        "random_seed": seed,
                        "candidate_limit": base_limit,
                        "risk_weight_multiplier": 1.0,
                        "diversification_profile": {"name": "temporal-base"},
                    }
                    experiment["experiment_id"] = payload_hash(experiment)[:16]
                    completed += 1
                    ledger.event(
                        "temporal_unit_start",
                        fold_id=fold["fold_id"],
                        experiment_id=experiment["experiment_id"],
                        completed=completed - 1,
                        total=units,
                    )
                    spec = build_experiment_spec(
                        suite,
                        experiment,
                        account=account,
                        name_prefix=f"{ledger.campaign_id}-{fold['fold_id']}",
                    )
                    train_result = run_atomic_experiment(
                        candidates=train_candidates,
                        spec=spec,
                        experiment=experiment,
                        output_dir=fold_root / experiment["experiment_id"],
                        backend=backend,
                        progress=lambda event: ledger.event(
                            "temporal_optimizer_progress",
                            fold_id=fold["fold_id"],
                            optimizer_event=event.get("event"),
                            **{
                                key: value
                                for key, value in event.items()
                                if key not in {"event", "fold_id"}
                            },
                        ),
                        rank_on_slice=True,
                    )
                    selected_ids = [
                        item
                        for item in train_result["selected_attempt_ids"]
                        if item in test_by_id
                    ]
                    test_search = PortfolioSearch(test_candidates, spec)
                    test_metrics = (
                        test_search.metrics(selected_ids, include_correlation=True)
                        if selected_ids
                        else {}
                    )
                    result = {
                        "fold": fold,
                        "experiment_id": experiment["experiment_id"],
                        "portfolio_size": size,
                        "objective": objective,
                        "random_seed": seed,
                        "selected_attempt_ids": selected_ids,
                        "train_metrics": train_result["metrics"],
                        "test_metrics": test_metrics,
                    }
                    write_json_atomic(
                        fold_root / experiment["experiment_id"] / "test-result.json",
                        result,
                        compact=True,
                    )
                    results.append(result)
                    ledger.event(
                        "temporal_unit_done",
                        fold_id=fold["fold_id"],
                        experiment_id=experiment["experiment_id"],
                        completed=completed,
                        total=units,
                    )
    write_json_atomic(root / "fold-index.json", folds)
    write_json_atomic(root / "temporal-results.json", results, compact=True)
    return folds, results


def _flat_outer_no_signal_curve_path(
    *,
    root: Path,
    fold_id: str,
    attempt_id: str,
    fold: dict[str, Any],
    terminal_outcome: dict[str, Any],
) -> str:
    """Persist a deterministic zero-return OOS curve for selected no-signal members."""
    test_start = str(fold.get("test_start") or "").strip()
    test_end = str(fold.get("test_end") or test_start).strip() or test_start
    points = [
        {
            "date": test_start,
            "equity_r": 0.0,
            "open_trade_count": 0,
            "closed_trade_count": 0,
        }
    ]
    if test_end and test_end != test_start:
        points.append(
            {
                "date": test_end,
                "equity_r": 0.0,
                "open_trade_count": 0,
                "closed_trade_count": 0,
            }
        )
    path = (
        root
        / str(fold_id)
        / "outer-terminal-flat-curves"
        / f"{slug(attempt_id)}-{payload_hash(terminal_outcome)[:12]}.json"
    )
    write_json_atomic(
        path,
        {
            "schema": "autoresearch-nested-flat-outer-no-signal-curve-v1",
            "attempt_id": attempt_id,
            "fold_id": fold_id,
            "terminal_outcome": terminal_outcome,
            "curve": {
                "period_granularity": "day",
                "downsampled": False,
                "point_count": len(points),
                "returned_point_count": len(points),
                "points": points,
            },
        },
        compact=True,
    )
    return str(path)


def run_nested_cell_temporal_validation(
    *,
    rows: list[dict[str, Any]],
    fold_reports: list[dict[str, Any]],
    suite: dict[str, Any],
    account: dict[str, Any],
    root: Path,
    backend: str,
    freeze_only: bool = False,
    formal_binding: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build train-only inner consensus, then score its frozen portfolio on outer curves.

    ``formal_binding`` is supplied only by the formal Level C freeze boundary.
    It turns a fully receipt-backed training set that the optimizer rejects in
    its entirety into a typed non-promotable result. Missing or malformed
    evidence remains a hard error.
    """
    root.mkdir(parents=True, exist_ok=True)
    base_by_id = {
        str(row.get("attempt_id") or ""): row
        for row in rows
        if str(row.get("attempt_id") or "")
    }
    temporal = suite.get("temporal_validation") or {}
    portfolio = suite.get("portfolio") or {}
    sizes = [int(item) for item in portfolio.get("sizes") or []]
    objectives = [str(item) for item in portfolio.get("objectives") or []]
    seeds = [int(item) for item in temporal.get("seeds") or [29]]
    policy = dict(suite.get("selection_policy") or {})
    inner_config = dict(temporal.get("inner_validation") or {})
    formal_terminal = formal_binding is not None
    if formal_terminal and not freeze_only:
        raise PortfolioResearchError(
            "formal nested terminal handling is only valid at the frozen-portfolio boundary"
        )
    results: list[dict[str, Any]] = []
    for fold_report in fold_reports:
        fold = dict(fold_report.get("fold") or {})
        fold_id = str(fold.get("fold_id") or "")
        train_rows: list[dict[str, Any]] = []
        outer_rows: list[dict[str, Any]] = []
        receipt_by_id: dict[str, dict[str, Any]] = {}
        evidence_identity_by_id: dict[str, dict[str, Any]] = {}
        for record in fold_report.get("records") or []:
            attempt_id = str(record.get("attempt_id") or "")
            base = base_by_id.get(attempt_id)
            if base is None:
                continue
            train_stage_status = str(record.get("train_validation_status") or "valid")
            if train_stage_status == "nonviable":
                continue
            outer_stage_status = str(record.get("outer_validation_status") or "")
            allowed_outer_statuses = (
                {"pending_selection", "not_selected", "valid", "nonviable"}
                if freeze_only
                else {"not_selected", "valid", "nonviable"}
            )
            if outer_stage_status not in allowed_outer_statuses:
                raise PortfolioResearchError(
                    f"Nested fold {fold_id} has non-terminal outer evidence "
                    f"for {attempt_id}: {outer_stage_status or 'missing'}"
                )
            outer_plan = dict(record.get("outer_test_plan") or {})
            train_plan = dict(record.get("train_plan") or {})
            receipt_payload = dict(record.get("cell_receipt") or {})
            receipt = (
                _validate_formal_nested_receipt(
                    fold_id=fold_id,
                    fold=fold,
                    attempt_id=attempt_id,
                    receipt_payload=receipt_payload,
                    train_plan_payload=train_plan,
                    outer_plan_payload=outer_plan,
                    formal_binding=formal_binding,
                )
                if formal_terminal
                else receipt_payload
            )
            receipt_by_id[attempt_id] = receipt
            evidence_identity_by_id[attempt_id] = {
                "train_evidence_plan_id": train_plan.get("plan_id"),
                "outer_evidence_plan_id": outer_plan.get("plan_id"),
                "lake_manifest_sha256": outer_plan.get("lake_manifest_sha256"),
            }
            train_result_path = Path(str(record.get("train_result_path") or ""))
            try:
                train_result_payload = json.loads(train_result_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise PortfolioResearchError(
                    f"Nested fold {fold_id} cannot read bounded train result for {attempt_id}: {exc}"
                ) from exc
            if not isinstance(train_result_payload, dict):
                raise PortfolioResearchError(
                    f"Nested fold {fold_id} train result is malformed for {attempt_id}"
                )
            if formal_terminal:
                train_aggregate, bounded_train_score = _validate_formal_nested_train_evidence(
                    fold_id=fold_id,
                    attempt_id=attempt_id,
                    train_result_payload=train_result_payload,
                    train_curve_path=Path(str(record.get("train_curve_path") or "")),
                )
            else:
                train_aggregate = (train_result_payload.get("data") or {}).get("aggregate")
                train_score = (
                    (train_aggregate.get("score_lab") or {}).get("score")
                    if isinstance(train_aggregate, dict)
                    else None
                )
                try:
                    bounded_train_score = float(train_score)
                except (TypeError, ValueError) as exc:
                    raise PortfolioResearchError(
                        f"Nested fold {fold_id} train result lacks a bounded score for {attempt_id}"
                    ) from exc
            bounded_trade_count = int(
                (train_aggregate or {}).get("resolved_trade_count_max")
                or (train_aggregate or {}).get("signal_count")
                or 0
            )
            train_month_count = max(
                1,
                int(
                    (date.fromisoformat(str(fold["train_end"])[:10])
                    - date.fromisoformat(str(fold["train_start"])[:10])).days
                    / 30.4375
                ),
            )
            common = {
                **base,
                "full_backtest_validation_status_36m": "valid",
                "has_full_backtest_36m": True,
                "score_36m": bounded_train_score,
                "score_lab_score_36m": bounded_train_score,
                "trade_count_36m": bounded_trade_count,
                "trades_per_month_36m": bounded_trade_count / train_month_count,
                "selected_stop_loss_percent_36m": (
                    receipt.get("execution_cell") or {}
                ).get("stop_loss_percent"),
            }
            train_rows.append(
                {
                    **common,
                    "full_backtest_result_path_36m": record.get("train_result_path"),
                    "full_backtest_calendar_curve_path_36m": record.get("train_curve_path"),
                }
            )
            # Holding/risk metadata stays train-only; only the return curve comes from OOS.
            if outer_stage_status in {"valid", "nonviable"}:
                outer_rows.append(
                    {
                        **common,
                        "full_backtest_result_path_36m": record.get("train_result_path"),
                        "full_backtest_calendar_curve_path_36m": (
                            _flat_outer_no_signal_curve_path(
                                root=root,
                                fold_id=fold_id,
                                attempt_id=attempt_id,
                                fold=fold,
                                terminal_outcome=dict(record.get("outer_terminal_outcome") or {}),
                            )
                            if outer_stage_status == "nonviable"
                            else record.get("outer_curve_path")
                        ),
                        "outer_validation_status": outer_stage_status,
                    }
                )
        if not train_rows:
            raise PortfolioResearchError(f"Nested fold {fold_id} has no valid evidence rows")
        inner_base_spec = build_experiment_spec(
            suite,
            {
                "experiment_id": "nested-inner-base",
                "portfolio_size": max(sizes),
                "objective": objectives[0],
                "random_seed": seeds[0],
                "candidate_limit": -1,
                "risk_weight_multiplier": 1.0,
                "diversification_profile": {"name": "nested-inner-base"},
            },
            account=account,
            name_prefix=f"nested-{fold_id}",
        )
        train_candidates, train_rejections = build_optimizer_candidates(
            train_rows,
            inner_base_spec,
        )
        outer_candidates, outer_rejections = (
            ([], [])
            if freeze_only
            else build_optimizer_candidates(
                outer_rows,
                replace(
                    build_experiment_spec(
                        suite,
                        {
                            "experiment_id": "nested-outer-base",
                            "portfolio_size": max(sizes),
                            "objective": objectives[0],
                            "random_seed": seeds[0],
                            "candidate_limit": -1,
                            "risk_weight_multiplier": 1.0,
                            "diversification_profile": {"name": "nested-outer-base"},
                        },
                        account=account,
                        name_prefix=f"nested-{fold_id}",
                    ),
                    min_score=float("-inf"),
                    candidate_limit=-1,
                    require_positive_source_return=False,
                ),
            )
        )
        candidate_by_id = {candidate.attempt_id: candidate for candidate in train_candidates}
        total_months = max(
            1,
            (date.fromisoformat(str(fold["train_end"])[:10]).year - date.fromisoformat(str(fold["train_start"])[:10]).year) * 12
            + date.fromisoformat(str(fold["train_end"])[:10]).month
            - date.fromisoformat(str(fold["train_start"])[:10]).month,
        )
        inner_train_months = int(inner_config.get("train_months") or max(1, total_months // 2))
        inner_test_months = int(inner_config.get("test_months") or max(1, total_months // 6))
        inner_step_months = int(inner_config.get("step_months") or inner_test_months)
        inner_embargo_days = int(inner_config.get("embargo_days", fold.get("embargo_days") or 0))
        inner_folds = temporal_folds(
            start=str(fold["train_start"]),
            end=str(fold["train_end"]),
            train_months=inner_train_months,
            test_months=inner_test_months,
            step_months=inner_step_months,
            embargo_days=inner_embargo_days,
        )
        minimum_inner_units = int(inner_config.get("minimum_units", 2))
        if len(inner_folds) < minimum_inner_units:
            raise PortfolioResearchError(
                f"Nested fold {fold_id} produced {len(inner_folds)} inner folds; "
                f"at least {minimum_inner_units} are required"
            )
        if not train_candidates:
            evidence_failures = sorted(
                reason
                for reason in train_rejections
                if reason in {
                    "invalid_or_missing_full_backtest",
                    "missing_source_calendar_curve",
                    "empty_calendar_curve",
                    "missing_hold_metrics",
                }
                or reason.startswith("full_backtest_")
            )
            if evidence_failures:
                raise PortfolioResearchError(
                    f"Nested fold {fold_id} has malformed training evidence: "
                    + ", ".join(evidence_failures)
                )
            if not formal_terminal or not train_rejections:
                raise PortfolioResearchError(
                    f"Nested fold {fold_id} has no optimizer-eligible training candidates"
                )
            terminal = {
                "fold": fold,
                "experiment_id": payload_hash(
                    {
                        "fold_id": fold_id,
                        "kind": "no_eligible_training_candidates",
                        "train_evidence_plan_ids": {
                            attempt_id: identity.get("train_evidence_plan_id")
                            for attempt_id, identity in sorted(evidence_identity_by_id.items())
                        },
                    }
                )[:16],
                "evidence_level": "level_b_train_selected_cell",
                "status": "no_eligible_training_candidates",
                "outcome": "no_candidate",
                "selected_attempt_ids": [],
                "training_valid_evidence_count": len(train_rows),
                "optimizer_candidate_count": 0,
                "optimizer_min_score": inner_base_spec.min_score,
                "inner_fold_count": len(inner_folds),
                "train_rejections": train_rejections,
            }
            terminal_path = root / fold_id / terminal["experiment_id"] / "freeze-result.json"
            write_json_atomic(terminal_path, terminal, compact=True)
            results.append(terminal)
            continue
        inner_results: list[dict[str, Any]] = []
        for inner_fold in inner_folds:
            inner_train_candidates = slice_candidates(
                train_candidates,
                start=str(inner_fold["train_start"]),
                end=str(inner_fold["train_end"]),
            )
            inner_test_candidates = slice_candidates(
                train_candidates,
                start=str(inner_fold["test_start"]),
                end=str(inner_fold["test_end"]),
            )
            inner_test_by_id = {
                candidate.attempt_id: candidate for candidate in inner_test_candidates
            }
            for size in sizes:
                if len(inner_train_candidates) < size:
                    raise PortfolioResearchError(
                        f"Nested fold {fold_id}/{inner_fold['fold_id']} has "
                        f"{len(inner_train_candidates)} candidates for requested size {size}"
                    )
                for objective in objectives:
                    for seed in seeds:
                        inner_experiment = {
                            "outer_fold_id": fold_id,
                            "inner_fold_id": inner_fold["fold_id"],
                            "portfolio_size": size,
                            "objective": objective,
                            "random_seed": seed,
                            "candidate_limit": -1,
                            "risk_weight_multiplier": 1.0,
                            "diversification_profile": {"name": "nested-inner"},
                            "evidence_level": "level_a_inner_train_validation",
                        }
                        inner_experiment["experiment_id"] = payload_hash(inner_experiment)[:16]
                        inner_spec = build_experiment_spec(
                            suite,
                            inner_experiment,
                            account=account,
                            name_prefix=f"nested-inner-{fold_id}",
                        )
                        selection = run_atomic_experiment(
                            candidates=inner_train_candidates,
                            spec=inner_spec,
                            experiment=inner_experiment,
                            output_dir=(
                                root
                                / fold_id
                                / "inner"
                                / str(inner_fold["fold_id"])
                                / inner_experiment["experiment_id"]
                            ),
                            backend=backend,
                            progress=None,
                            rank_on_slice=True,
                        )
                        selected_ids = [
                            attempt_id
                            for attempt_id in selection["selected_attempt_ids"]
                            if attempt_id in inner_test_by_id
                        ]
                        inner_metrics = PortfolioSearch(
                            inner_test_candidates,
                            replace(
                                inner_spec,
                                min_score=float("-inf"),
                                require_positive_source_return=False,
                            ),
                        ).metrics(selected_ids, include_correlation=True)
                        inner_results.append(
                            {
                                "fold": inner_fold,
                                "experiment_id": inner_experiment["experiment_id"],
                                "portfolio_size": size,
                                "objective": objective,
                                "random_seed": seed,
                                "selected_attempt_ids": selected_ids,
                                "train_metrics": selection["metrics"],
                                "test_metrics": inner_metrics,
                                "evidence_level": "level_a_inner_train_validation",
                            }
                        )
        consensus_policy = {**policy, **dict(inner_config.get("selection_policy") or {})}
        consensus = build_consensus_evidence(
            inner_results,
            candidate_by_id=candidate_by_id,
            policy=consensus_policy,
        )
        write_json_atomic(
            root / fold_id / "inner-consensus.json",
            {
                "outer_fold": fold,
                "inner_folds": inner_folds,
                "inner_results": inner_results,
                "consensus": consensus,
            },
            compact=True,
        )
        core_rank = {
            str(row["attempt_id"]): index
            for index, row in enumerate(consensus.get("strategy_rows") or [])
        }
        ranked_core = sorted(
            (str(item) for item in consensus.get("core_attempt_ids") or []),
            key=lambda attempt_id: core_rank.get(attempt_id, 10**9),
        )
        for size in sizes:
            for objective in objectives:
                for seed in seeds:
                    experiment = {
                        "fold_id": fold_id,
                        "portfolio_size": size,
                        "objective": objective,
                        "random_seed": seed,
                        "candidate_limit": -1,
                        "risk_weight_multiplier": 1.0,
                        "diversification_profile": {"name": "nested-cell"},
                        "evidence_level": "level_b_train_selected_cell",
                    }
                    experiment["experiment_id"] = payload_hash(experiment)[:16]
                    spec = build_experiment_spec(
                        suite,
                        experiment,
                        account=account,
                        name_prefix=f"nested-{fold_id}",
                    )
                    if len(train_candidates) < size:
                        raise PortfolioResearchError(
                            f"Nested fold {fold_id} has {len(train_candidates)} train "
                            f"candidates for requested size {size}"
                        )
                    frozen_core = ranked_core[:size]
                    minimum_core_count = int(consensus_policy.get("minimum_consensus_core_count", 0))
                    minimum_core_share = float(consensus_policy.get("minimum_consensus_core_share", 0.0))
                    if (
                        len(frozen_core) < minimum_core_count
                        or (len(frozen_core) / size if size else 0.0) < minimum_core_share
                    ):
                        result = {
                            "fold": fold,
                            "experiment_id": experiment["experiment_id"],
                            "portfolio_size": size,
                            "objective": objective,
                            "random_seed": seed,
                            "evidence_level": "level_ab_inner_consensus_frozen_cell_outer_test",
                            "status": "no_defensible_consensus",
                            "selected_attempt_ids": [],
                            "inner_fold_count": len(inner_folds),
                            "inner_unit_count": len(inner_results),
                            "consensus_core_attempt_ids": frozen_core,
                            "consensus_core_count": len(frozen_core),
                            "train_rejections": train_rejections,
                            "outer_rejections": outer_rejections,
                        }
                        write_json_atomic(
                            root / fold_id / experiment["experiment_id"] / "test-result.json",
                            result,
                            compact=True,
                        )
                        results.append(result)
                        continue
                    consensus_spec = replace(
                        spec,
                        required_attempt_ids=tuple(frozen_core),
                    )
                    train_result = run_atomic_experiment(
                        candidates=train_candidates,
                        spec=consensus_spec,
                        experiment=experiment,
                        output_dir=root / fold_id / experiment["experiment_id"],
                        backend=backend,
                        progress=None,
                        rank_on_slice=True,
                    )
                    train_selected_ids = [
                        str(attempt_id)
                        for attempt_id in train_result["selected_attempt_ids"]
                    ]
                    if len(train_selected_ids) != size:
                        raise PortfolioResearchError(
                            f"Nested fold {fold_id} selected {len(train_selected_ids)} "
                            f"members for requested size {size}"
                        )
                    if not set(frozen_core) <= set(train_selected_ids):
                        raise PortfolioResearchError(
                            f"Nested fold {fold_id} optimizer omitted mandatory consensus core"
                        )
                    selected_ids = train_selected_ids
                    frozen_portfolio = {
                        "schema": "autoresearch-frozen-nested-portfolio-v1",
                        "outer_fold": fold,
                        "experiment_id": experiment["experiment_id"],
                        "evidence_level": "level_ab_inner_consensus_frozen_cell_outer_test",
                        "selected_attempt_ids": selected_ids,
                        "consensus_core_attempt_ids": frozen_core,
                        "inner_consensus_sha256": "sha256:" + payload_hash(consensus),
                        "cell_receipts": {
                            attempt_id: receipt_by_id[attempt_id]
                            for attempt_id in selected_ids
                        },
                        "evidence_identities": {
                            attempt_id: evidence_identity_by_id[attempt_id]
                            for attempt_id in selected_ids
                        },
                    }
                    frozen_portfolio_path = (
                        root
                        / fold_id
                        / experiment["experiment_id"]
                        / "frozen-portfolio.json"
                    )
                    write_json_immutable(frozen_portfolio_path, frozen_portfolio)
                    if freeze_only:
                        result = {
                            "fold": fold,
                            "experiment_id": experiment["experiment_id"],
                            "portfolio_size": size,
                            "objective": objective,
                            "random_seed": seed,
                            "evidence_level": "level_b_train_selected_cell",
                            "status": "frozen",
                            "selected_attempt_ids": selected_ids,
                            "train_metrics": train_result["metrics"],
                            "cell_receipts": {
                                attempt_id: receipt_by_id[attempt_id]
                                for attempt_id in selected_ids
                            },
                            "inner_fold_count": len(inner_folds),
                            "inner_unit_count": len(inner_results),
                            "consensus_core_attempt_ids": frozen_core,
                            "consensus_core_count": len(frozen_core),
                            "consensus_core_share": len(frozen_core) / size if size else 0.0,
                            "frozen_portfolio_path": str(frozen_portfolio_path),
                            "frozen_portfolio_sha256": "sha256:" + payload_hash(frozen_portfolio),
                            "train_rejections": train_rejections,
                        }
                        write_json_atomic(
                            root / fold_id / experiment["experiment_id"] / "freeze-result.json",
                            result,
                            compact=True,
                        )
                        results.append(result)
                        continue
                    outer_by_id = {
                        candidate.attempt_id: candidate for candidate in outer_candidates
                    }
                    missing_outer_ids = sorted(
                        set(train_selected_ids) - set(outer_by_id)
                    )
                    if missing_outer_ids:
                        raise PortfolioResearchError(
                            f"Nested fold {fold_id} outer evidence is missing frozen "
                            f"portfolio members: {', '.join(missing_outer_ids)}"
                        )
                    outer_search = PortfolioSearch(
                        outer_candidates,
                        replace(
                            consensus_spec,
                            min_score=float("-inf"),
                            candidate_limit=-1,
                            require_positive_source_return=False,
                        ),
                    )
                    outer_metrics = outer_search.metrics(
                        selected_ids, include_correlation=True
                    )
                    result = {
                        "fold": fold,
                        "experiment_id": experiment["experiment_id"],
                        "portfolio_size": size,
                        "objective": objective,
                        "random_seed": seed,
                        "evidence_level": "level_ab_inner_consensus_frozen_cell_outer_test",
                        "status": "complete",
                        "selected_attempt_ids": selected_ids,
                        "train_metrics": train_result["metrics"],
                        "test_metrics": outer_metrics,
                        "cell_receipts": {
                            attempt_id: receipt_by_id[attempt_id]
                            for attempt_id in selected_ids
                        },
                        "inner_fold_count": len(inner_folds),
                        "inner_unit_count": len(inner_results),
                        "consensus_core_attempt_ids": frozen_core,
                        "consensus_core_count": len(frozen_core),
                        "consensus_core_share": len(frozen_core) / size if size else 0.0,
                        "frozen_portfolio_path": str(frozen_portfolio_path),
                        "frozen_portfolio_sha256": "sha256:" + payload_hash(frozen_portfolio),
                        "train_rejections": train_rejections,
                        "outer_rejections": outer_rejections,
                    }
                    write_json_atomic(
                        root / fold_id / experiment["experiment_id"] / "test-result.json",
                        result,
                        compact=True,
                    )
                    results.append(result)
    write_json_atomic(root / "nested-temporal-results.json", results, compact=True)
    return results


def median(values: Iterable[float]) -> float:
    resolved = [float(item) for item in values]
    return statistics.median(resolved) if resolved else 0.0


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    position = max(0.0, min(1.0, quantile)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _result_identity(result: dict[str, Any]) -> str:
    fold_id = str((result.get("fold") or {}).get("fold_id") or "").strip()
    experiment_id = str(result.get("experiment_id") or "unknown").strip()
    return f"{fold_id}:{experiment_id}" if fold_id else experiment_id


def selection_frequency(
    results: list[dict[str, Any]],
    *,
    domain: str = "unspecified",
    candidate_by_id: dict[str, OptimizerCandidate] | None = None,
) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    total = len(results)
    for result in results:
        counts.update(set(result.get("selected_attempt_ids") or []))
    rows: list[dict[str, Any]] = []
    for attempt_id, count in counts.most_common():
        candidate = (candidate_by_id or {}).get(attempt_id)
        rows.append({
            "domain": domain,
            "attempt_id": attempt_id,
            "selection_count": count,
            "experiment_count": total,
            "selection_frequency": count / total if total else 0.0,
            "structural_family_id": candidate.family if candidate else None,
            "instruments": ",".join(candidate.instruments) if candidate else None,
            "primary_asset_class": candidate.primary_asset_class if candidate else None,
        })
    return rows


def family_selection_frequency(
    results: list[dict[str, Any]],
    *,
    domain: str,
    candidate_by_id: dict[str, OptimizerCandidate],
) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    member_counts: dict[str, Counter[str]] = {}
    total = len(results)
    for result in results:
        selected_families: set[str] = set()
        for attempt_id in set(result.get("selected_attempt_ids") or []):
            candidate = candidate_by_id.get(str(attempt_id))
            if candidate is None:
                continue
            selected_families.add(candidate.family)
            member_counts.setdefault(candidate.family, Counter())[candidate.attempt_id] += 1
        counts.update(selected_families)
    return [
        {
            "domain": domain,
            "structural_family_id": family_id,
            "selection_count": count,
            "experiment_count": total,
            "selection_frequency": count / total if total else 0.0,
            "selected_member_count": len(member_counts.get(family_id) or {}),
            "selected_attempt_ids": ",".join(
                attempt_id
                for attempt_id, _member_count in (member_counts.get(family_id) or Counter()).most_common()
            ),
        }
        for family_id, count in counts.most_common()
    ]


def portfolio_similarity(
    results: list[dict[str, Any]], *, domain: str = "unspecified"
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for left_index, left in enumerate(results):
        left_ids = set(left.get("selected_attempt_ids") or [])
        for right in results[left_index + 1 :]:
            right_ids = set(right.get("selected_attempt_ids") or [])
            union = left_ids | right_ids
            rows.append(
                {
                    "domain": domain,
                    "left_id": _result_identity(left),
                    "right_id": _result_identity(right),
                    "intersection": len(left_ids & right_ids),
                    "union": len(union),
                    "jaccard": len(left_ids & right_ids) / len(union) if union else 1.0,
                }
            )
    return rows


def similarity_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "comparison_count": len(rows),
        "jaccard": {
            "minimum": min((float(row["jaccard"]) for row in rows), default=None),
            "median": median(float(row["jaccard"]) for row in rows),
            "mean": (
                statistics.mean(float(row["jaccard"]) for row in rows)
                if rows
                else None
            ),
            "maximum": max((float(row["jaccard"]) for row in rows), default=None),
        },
        "shared_member_count": {
            "minimum": min((int(row["intersection"]) for row in rows), default=None),
            "median": median(int(row["intersection"]) for row in rows),
            "maximum": max((int(row["intersection"]) for row in rows), default=None),
        },
    }


def temporal_adjacent_churn(
    temporal_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for result in temporal_results:
        signature = (
            str(result.get("objective") or "unknown"),
            int(result.get("portfolio_size") or 0),
            int(result.get("random_seed") or 0),
        )
        grouped.setdefault(signature, []).append(result)
    rows: list[dict[str, Any]] = []
    for (objective, portfolio_size, random_seed), results in sorted(grouped.items()):
        ordered = sorted(
            results,
            key=lambda row: (
                str((row.get("fold") or {}).get("train_start") or ""),
                str((row.get("fold") or {}).get("fold_id") or ""),
            ),
        )
        for previous, current in zip(ordered, ordered[1:]):
            previous_ids = set(previous.get("selected_attempt_ids") or [])
            current_ids = set(current.get("selected_attempt_ids") or [])
            union = previous_ids | current_ids
            intersection = previous_ids & current_ids
            rows.append(
                {
                    "domain": "temporal",
                    "objective": objective,
                    "portfolio_size": portfolio_size,
                    "random_seed": random_seed,
                    "previous_fold_id": (previous.get("fold") or {}).get("fold_id"),
                    "current_fold_id": (current.get("fold") or {}).get("fold_id"),
                    "previous_count": len(previous_ids),
                    "current_count": len(current_ids),
                    "retained_count": len(intersection),
                    "added_count": len(current_ids - previous_ids),
                    "removed_count": len(previous_ids - current_ids),
                    "jaccard": len(intersection) / len(union) if union else 1.0,
                    "churn": 1.0 - (len(intersection) / len(union) if union else 1.0),
                }
            )
    return rows


def build_consensus_evidence(
    temporal_results: list[dict[str, Any]],
    *,
    candidate_by_id: dict[str, OptimizerCandidate],
    policy: dict[str, Any],
) -> dict[str, Any]:
    total_units = len(temporal_results)
    fold_ids = sorted(
        {
            str((result.get("fold") or {}).get("fold_id") or "").strip()
            for result in temporal_results
            if str((result.get("fold") or {}).get("fold_id") or "").strip()
        }
    )
    total_folds = len(fold_ids)
    strategy_units: Counter[str] = Counter()
    strategy_folds: dict[str, set[str]] = {}
    family_units: Counter[str] = Counter()
    family_folds: dict[str, set[str]] = {}
    for result in temporal_results:
        fold_id = str((result.get("fold") or {}).get("fold_id") or "").strip()
        selected_ids = {
            str(item)
            for item in result.get("selected_attempt_ids") or []
            if str(item) in candidate_by_id
        }
        strategy_units.update(selected_ids)
        selected_families: set[str] = set()
        for attempt_id in selected_ids:
            strategy_folds.setdefault(attempt_id, set()).add(fold_id)
            selected_families.add(candidate_by_id[attempt_id].family)
        family_units.update(selected_families)
        for family_id in selected_families:
            family_folds.setdefault(family_id, set()).add(fold_id)

    minimum_unit_frequency = float(
        policy.get("minimum_consensus_selection_frequency", 0.5)
    )
    minimum_fold_frequency = float(policy.get("minimum_consensus_fold_frequency", 1.0))
    minimum_conditional_count = max(
        1, int(policy.get("minimum_conditional_selection_count", 2))
    )
    raw_core_ids = {
        attempt_id
        for attempt_id, count in strategy_units.items()
        if (count / total_units if total_units else 0.0) >= minimum_unit_frequency
        and (
            len(strategy_folds.get(attempt_id) or set()) / total_folds
            if total_folds
            else 0.0
        )
        >= minimum_fold_frequency
    }

    def representative_rank(attempt_id: str) -> tuple[int, int, float, float, str]:
        candidate = candidate_by_id[attempt_id]
        return (
            strategy_units[attempt_id],
            len(strategy_folds.get(attempt_id) or set()),
            candidate.score,
            candidate.final_r,
            attempt_id,
        )

    excluded_duplicate_of: dict[str, str] = {}
    exact_groups: dict[str, list[str]] = {}
    for attempt_id in sorted(raw_core_ids):
        candidate = candidate_by_id[attempt_id]
        key = candidate.behavior_fingerprint or f"unique:{attempt_id}"
        exact_groups.setdefault(key, []).append(attempt_id)
    exact_representatives: set[str] = set()
    for members in exact_groups.values():
        representative = max(members, key=representative_rank)
        exact_representatives.add(representative)
        for attempt_id in members:
            if attempt_id != representative:
                excluded_duplicate_of[attempt_id] = representative

    family_groups: dict[str, list[str]] = {}
    for attempt_id in sorted(exact_representatives):
        family_groups.setdefault(candidate_by_id[attempt_id].family, []).append(attempt_id)
    core_ids: set[str] = set()
    for members in family_groups.values():
        representative = max(members, key=representative_rank)
        core_ids.add(representative)
        for attempt_id in members:
            if attempt_id != representative:
                excluded_duplicate_of[attempt_id] = representative

    strategy_rows: list[dict[str, Any]] = []
    for attempt_id, count in strategy_units.most_common():
        candidate = candidate_by_id[attempt_id]
        selected_fold_count = len(strategy_folds.get(attempt_id) or set())
        if attempt_id in core_ids:
            category = "stable_core"
        elif attempt_id in excluded_duplicate_of:
            category = "excluded_duplicate"
        elif count >= minimum_conditional_count:
            category = "conditional_sleeve"
        else:
            category = "one_off_selection"
        strategy_rows.append(
            {
                "attempt_id": attempt_id,
                "category": category,
                "selection_count": count,
                "temporal_unit_count": total_units,
                "selection_frequency": count / total_units if total_units else 0.0,
                "selected_fold_count": selected_fold_count,
                "temporal_fold_count": total_folds,
                "fold_frequency": selected_fold_count / total_folds if total_folds else 0.0,
                "structural_family_id": candidate.family,
                "structural_family_source": candidate.family_source,
                "lineage_id": candidate.lineage_id,
                "behavior_fingerprint": candidate.behavior_fingerprint,
                "duplicate_of_attempt_id": excluded_duplicate_of.get(attempt_id),
                "instruments": ",".join(candidate.instruments),
                "score": candidate.score,
                "full_window_return_r": candidate.final_r,
            }
        )
    family_rows = [
        {
            "structural_family_id": family_id,
            "selection_count": count,
            "temporal_unit_count": total_units,
            "selection_frequency": count / total_units if total_units else 0.0,
            "selected_fold_count": len(family_folds.get(family_id) or set()),
            "temporal_fold_count": total_folds,
            "fold_frequency": (
                len(family_folds.get(family_id) or set()) / total_folds
                if total_folds
                else 0.0
            ),
            "representative_attempt_id": max(
                (
                    attempt_id
                    for attempt_id in strategy_units
                    if candidate_by_id[attempt_id].family == family_id
                ),
                key=representative_rank,
            ),
        }
        for family_id, count in family_units.most_common()
    ]
    return {
        "schema_version": 1,
        "temporal_unit_count": total_units,
        "temporal_fold_count": total_folds,
        "policy": {
            "minimum_consensus_selection_frequency": minimum_unit_frequency,
            "minimum_consensus_fold_frequency": minimum_fold_frequency,
            "minimum_conditional_selection_count": minimum_conditional_count,
        },
        "raw_core_attempt_ids": sorted(raw_core_ids),
        "core_attempt_ids": sorted(core_ids),
        "raw_core_count": len(raw_core_ids),
        "core_count": len(core_ids),
        "strategy_rows": strategy_rows,
        "family_rows": family_rows,
    }


def behavioral_cluster_summary(
    output: dict[str, Any],
    *,
    candidate_by_id: dict[str, OptimizerCandidate],
) -> dict[str, Any]:
    attempt_ids = [str(item) for item in output.get("attempt_ids") or []]
    index_by_id = {attempt_id: index for index, attempt_id in enumerate(attempt_ids)}
    matrices = {
        "active_overlap": output.get("active_overlap_matrix") or [],
        "return_correlation": output.get("return_correlation_matrix") or [],
        "downside_correlation": output.get("downside_correlation_matrix") or [],
        "worst_decile_correlation": output.get("worst_decile_correlation_matrix") or [],
        "behavioral_similarity": output.get("similarity_matrix") or [],
    }
    assignments: dict[str, str] = {}
    cluster_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    for cluster in output.get("clusters") or []:
        cluster_id = str(cluster.get("id") or "")
        members = sorted(str(item) for item in cluster.get("members") or [])
        for attempt_id in members:
            assignments[attempt_id] = cluster_id
            candidate = candidate_by_id.get(attempt_id)
            cluster_rows.append(
                {
                    "behavioral_cluster_id": cluster_id,
                    "cluster_size": len(members),
                    "attempt_id": attempt_id,
                    "structural_family_id": candidate.family if candidate else None,
                    "behavior_fingerprint": (
                        candidate.behavior_fingerprint if candidate else None
                    ),
                }
            )
        for left_index, left_id in enumerate(members):
            for right_id in members[left_index + 1 :]:
                left = index_by_id[left_id]
                right = index_by_id[right_id]
                pair_rows.append(
                    {
                        "behavioral_cluster_id": cluster_id,
                        "left_attempt_id": left_id,
                        "right_attempt_id": right_id,
                        **{
                            metric: float(matrix[left][right])
                            for metric, matrix in matrices.items()
                        },
                    }
                )
    cluster_sizes = Counter(assignments.values())
    return {
        "schema_version": 1,
        "backend": "rust_pyo3",
        "candidate_count": len(attempt_ids),
        "cluster_count": len(cluster_sizes),
        "singleton_cluster_count": sum(size == 1 for size in cluster_sizes.values()),
        "largest_cluster_size": max(cluster_sizes.values(), default=0),
        "reference": output.get("reference") or {},
        "assignments": assignments,
        "cluster_rows": cluster_rows,
        "pair_rows": pair_rows,
    }


def apply_behavioral_cluster_deduplication(
    consensus: dict[str, Any],
    *,
    assignments: dict[str, str],
    attempt_ids: list[str],
    similarity_matrix: list[list[float]],
    threshold: float,
) -> dict[str, Any]:
    rows_by_id = {
        str(row.get("attempt_id")): row
        for row in consensus.get("strategy_rows") or []
    }
    current_core = set(consensus.get("core_attempt_ids") or [])
    index_by_id = {attempt_id: index for index, attempt_id in enumerate(attempt_ids)}

    def rank(attempt_id: str) -> tuple[int, int, float, float, str]:
        row = rows_by_id[attempt_id]
        return (
            int(row.get("selection_count") or 0),
            int(row.get("selected_fold_count") or 0),
            float(row.get("score") or 0.0),
            float(row.get("full_window_return_r") or 0.0),
            attempt_id,
        )

    representatives: list[str] = []
    for attempt_id in sorted(current_core, key=rank, reverse=True):
        duplicate_of = next(
            (
                representative
                for representative in representatives
                if float(
                    similarity_matrix[index_by_id[attempt_id]][
                        index_by_id[representative]
                    ]
                )
                >= threshold
            ),
            None,
        )
        row = rows_by_id[attempt_id]
        row["behavioral_cluster_id"] = assignments.get(attempt_id)
        if duplicate_of is None:
            representatives.append(attempt_id)
        else:
            row["category"] = "excluded_behavioral_substitute"
            row["behavioral_duplicate_of_attempt_id"] = duplicate_of
    for attempt_id, row in rows_by_id.items():
        row.setdefault("behavioral_cluster_id", assignments.get(attempt_id))
        row.setdefault("behavioral_duplicate_of_attempt_id", None)
    consensus["pre_behavioral_core_attempt_ids"] = sorted(current_core)
    consensus["pre_behavioral_core_count"] = len(current_core)
    consensus["core_attempt_ids"] = sorted(representatives)
    consensus["core_count"] = len(representatives)
    return consensus


def build_consensus_portfolios(
    *,
    candidates: list[OptimizerCandidate],
    folds: list[dict[str, Any]],
    suite: dict[str, Any],
    account: dict[str, Any],
    consensus: dict[str, Any],
    base_spec: PortfolioOptimizerSpec,
) -> list[dict[str, Any]]:
    candidate_by_id = {candidate.attempt_id: candidate for candidate in candidates}
    strategy_rows = list(consensus.get("strategy_rows") or [])
    row_by_id = {str(row.get("attempt_id")): row for row in strategy_rows}

    def rank(attempt_id: str) -> tuple[int, int, float, float, str]:
        row = row_by_id[attempt_id]
        return (
            int(row.get("selection_count") or 0),
            int(row.get("selected_fold_count") or 0),
            float(row.get("score") or 0.0),
            float(row.get("full_window_return_r") or 0.0),
            attempt_id,
        )

    core_ids = sorted(
        (
            str(item)
            for item in consensus.get("core_attempt_ids") or []
            if str(item) in candidate_by_id and str(item) in row_by_id
        ),
        key=rank,
        reverse=True,
    )
    sleeve_ids = sorted(
        (
            str(row["attempt_id"])
            for row in strategy_rows
            if row.get("category") == "conditional_sleeve"
            and str(row.get("attempt_id")) in candidate_by_id
        ),
        key=rank,
        reverse=True,
    )
    objective = str(
        (suite.get("selection_policy") or {}).get("consensus_objective")
        or "stability"
    )
    optimizer_backend = str((suite.get("execution") or {}).get("optimizer_backend") or "auto")
    results: list[dict[str, Any]] = []
    for portfolio_size in sorted(
        {int(item) for item in (suite.get("portfolio") or {}).get("sizes") or []}
    ):
        frozen_core = core_ids[:portfolio_size]
        pool_ids = list(dict.fromkeys([*frozen_core, *sleeve_ids]))
        if len(pool_ids) < portfolio_size:
            results.append(
                {
                    "portfolio_size": portfolio_size,
                    "objective": objective,
                    "status": "insufficient_consensus_pool",
                    "available_candidate_count": len(pool_ids),
                    "required_candidate_count": portfolio_size,
                    "core_attempt_ids": frozen_core,
                    "promotion_eligible": False,
                }
            )
            continue
        pool = [candidate_by_id[attempt_id] for attempt_id in pool_ids]
        spec = replace(
            base_spec,
            portfolio_name=f"consensus-{portfolio_size}",
            portfolio_size=portfolio_size,
            candidate_limit=-1,
            objective_names=(objective,),
            baseline_attempt_ids=tuple(frozen_core),
            required_attempt_ids=tuple(frozen_core),
        )
        search, variants, _pareto, used_backend = run_optimizer_backend(
            pool,
            spec,
            backend=optimizer_backend,
        )
        variant = variants.get(objective)
        if not isinstance(variant, dict):
            raise PortfolioResearchError(
                f"Consensus optimizer did not return objective {objective}."
            )
        selected_ids = [str(item) for item in variant.get("selected_attempt_ids") or []]
        missing_core = sorted(set(frozen_core) - set(selected_ids))
        if missing_core:
            raise PortfolioResearchError(
                "Consensus optimizer violated required core membership: "
                + ", ".join(missing_core)
            )
        full_metrics = search.metrics(selected_ids, include_account=True)
        scenarios: list[dict[str, Any]] = []
        for fold in folds:
            test_candidates = slice_candidates(
                pool,
                start=str(fold["test_start"]),
                end=str(fold["test_end"]),
            )
            test_search = PortfolioSearch(test_candidates, spec)
            scenarios.append(
                {
                    "fold_id": fold.get("fold_id"),
                    "test_start": fold.get("test_start"),
                    "test_end": fold.get("test_end"),
                    "metrics": test_search.metrics(selected_ids, include_account=True),
                }
            )
        test_returns = [
            float((scenario.get("metrics") or {}).get("final_r") or 0.0)
            for scenario in scenarios
        ]
        test_drawdowns = [
            float((scenario.get("metrics") or {}).get("maxdd_r") or 0.0)
            for scenario in scenarios
        ]
        selected_core = sorted(set(selected_ids) & set(frozen_core))
        results.append(
            {
                "portfolio_id": payload_hash(tuple(sorted(selected_ids)))[:16],
                "portfolio_size": portfolio_size,
                "objective": objective,
                "status": "complete",
                "optimizer_backend": used_backend,
                "evidence_level": "cross_fold_consensus_diagnostic",
                "selected_attempt_ids": selected_ids,
                "core_attempt_ids": frozen_core,
                "selected_core_attempt_ids": selected_core,
                "selected_core_count": len(selected_core),
                "selected_core_share": (
                    len(selected_core) / len(selected_ids) if selected_ids else 0.0
                ),
                "full_metrics": full_metrics,
                "test_scenarios": scenarios,
                "median_test_return_r": median(test_returns),
                "worst_test_return_r": min(test_returns or [0.0]),
                "worst_test_drawdown_r": max(test_drawdowns or [0.0]),
                "negative_test_count": sum(value < 0.0 for value in test_returns),
                "robust_utility": _robust_utility(
                    full_metrics,
                    scenarios,
                    len(folds),
                ),
                "promotion_eligible": False,
                "promotion_block_reason": "requires_untouched_outer_test",
            }
        )
    return results


def _unique_portfolios(
    rows: list[dict[str, Any]], *, evidence_level: str
) -> dict[tuple[str, ...], dict[str, Any]]:
    portfolios: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(sorted(set(str(item) for item in row.get("selected_attempt_ids") or [])))
        if not key:
            continue
        entry = portfolios.setdefault(
            key,
            {
                "portfolio_id": payload_hash(key)[:16],
                "selected_attempt_ids": list(key),
                "support": 0,
                "evidence_level": evidence_level,
                "sources": [],
            },
        )
        entry["support"] += 1
        entry["sources"].append(
            {
                "id": row.get("experiment_id"),
                "fold": row.get("fold"),
                "test_metrics": row.get("test_metrics"),
            }
        )
    return portfolios


def _portfolio_scenario_metrics(
    *,
    ids: list[str],
    candidates: list[OptimizerCandidate],
    base_spec: PortfolioOptimizerSpec,
    sources: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    full_search = PortfolioSearch(candidates, replace(base_spec, portfolio_size=len(ids)))
    full_metrics = full_search.metrics(ids, include_correlation=True)
    tests: list[dict[str, Any]] = []
    for source in sources:
        fold = source.get("fold") or {}
        metrics = source.get("test_metrics")
        if not isinstance(metrics, dict) or not metrics:
            continue
        tests.append(
            {
                "fold_id": fold.get("fold_id"),
                "test_start": fold.get("test_start"),
                "test_end": fold.get("test_end"),
                "metrics": metrics,
            }
        )
    return full_metrics, tests


def _robust_utility(
    full_metrics: dict[str, Any], tests: list[dict[str, Any]], support: int
) -> float:
    test_returns = [float((item.get("metrics") or {}).get("final_r") or 0.0) for item in tests]
    test_drawdowns = [float((item.get("metrics") or {}).get("maxdd_r") or 0.0) for item in tests]
    negative_tests = sum(value < 0 for value in test_returns)
    return (
        median(test_returns) * 3.0
        + min(test_returns or [0.0]) * 2.0
        + float(full_metrics.get("return_to_dd") or 0.0) * 4.0
        - max(test_drawdowns or [0.0]) * 2.0
        - negative_tests * 10.0
        + math.log1p(max(0, support))
        - float(full_metrics.get("avg_positive_pair_corr") or 0.0) * 5.0
    )


def _bootstrap_total_return_ci(
    daily: list[float], *, seed: int = 731, samples: int = 500, block_size: int = 10
) -> dict[str, Any]:
    if not daily:
        return {"samples": 0, "lower": None, "median": None, "upper": None}
    rng = random.Random(seed)
    block = max(1, min(block_size, len(daily)))
    totals: list[float] = []
    for _ in range(samples):
        resampled: list[float] = []
        while len(resampled) < len(daily):
            start = rng.randrange(0, max(1, len(daily) - block + 1))
            resampled.extend(daily[start : start + block])
        totals.append(sum(resampled[: len(daily)]))
    return {
        "method": "moving_block_bootstrap",
        "samples": samples,
        "block_size_days": block,
        "lower": percentile(totals, 0.025),
        "median": percentile(totals, 0.5),
        "upper": percentile(totals, 0.975),
    }


def _deflated_sharpe(daily: list[float], *, trial_count: int) -> dict[str, Any]:
    if len(daily) < 3 or statistics.pstdev(daily) <= 1e-12:
        return {"supported": False, "reason": "insufficient_nonconstant_daily_returns"}
    mean_value = statistics.mean(daily)
    std_value = statistics.pstdev(daily)
    observed = mean_value / std_value
    effective_trials = max(1, int(trial_count))
    benchmark = NormalDist().inv_cdf(1.0 - 1.0 / (effective_trials + 1.0)) / math.sqrt(
        len(daily)
    )
    standard_error = math.sqrt(max(1e-12, (1.0 + 0.5 * observed * observed) / len(daily)))
    probability = NormalDist().cdf((observed - benchmark) / standard_error)
    return {
        "supported": True,
        "daily_sharpe": observed,
        "effective_trial_count": effective_trials,
        "expected_max_sharpe_benchmark": benchmark,
        "deflated_sharpe_probability": probability,
        "note": "Analytic trial-count adjustment; interpret with the campaign's fold and bootstrap evidence.",
    }


def _champion_diagnostics(
    *,
    champion: dict[str, Any],
    candidates: list[OptimizerCandidate],
    search: PortfolioSearch,
) -> dict[str, Any]:
    ids = [item for item in champion.get("selected_attempt_ids") or [] if item in search.by_id]
    daily, open_counts, closed_counts = search.combine_vectors(ids)
    worst_cutoff = percentile(daily, 0.1)
    downside_indexes = [index for index, value in enumerate(daily) if value < 0.0]
    worst_indexes = [index for index, value in enumerate(daily) if value <= worst_cutoff]
    correlation_rows: list[dict[str, Any]] = []
    for left_index, left_id in enumerate(ids):
        left = search.by_id[left_id].vector
        for right_id in ids[left_index + 1 :]:
            right = search.by_id[right_id].vector
            correlation_rows.append(
                {
                    "left_attempt_id": left_id,
                    "right_attempt_id": right_id,
                    "unconditional_correlation": pearson_corr(left, right),
                    "downside_correlation": pearson_corr(
                        [left[index] for index in downside_indexes],
                        [right[index] for index in downside_indexes],
                    ),
                    "worst_decile_correlation": pearson_corr(
                        [left[index] for index in worst_indexes],
                        [right[index] for index in worst_indexes],
                    ),
                }
            )

    instrument_counts = (champion.get("full_metrics") or {}).get("instrument_counts") or {}
    total_instrument_share = sum(float(value) for value in instrument_counts.values())
    concentration_rows = [
        {
            "instrument": instrument,
            "asset_class": instrument_asset_class(instrument),
            "fractional_strategy_share": float(value),
            "portfolio_share": (
                float(value) / total_instrument_share if total_instrument_share else 0.0
            ),
        }
        for instrument, value in sorted(
            instrument_counts.items(), key=lambda item: float(item[1]), reverse=True
        )
    ]
    instrument_hhi = sum(row["portfolio_share"] ** 2 for row in concentration_rows)

    recent_start_index = max(0, len(search.dates) - 90)
    regime_rows: list[dict[str, Any]] = []
    for attempt_id in ids:
        candidate = search.by_id[attempt_id]
        recent = candidate.vector[recent_start_index:]
        prior = candidate.vector[:recent_start_index]
        recent_closed = candidate.closed_vector[recent_start_index:]
        recent_return = sum(recent)
        prior_return = sum(prior)
        if not recent_closed or sum(recent_closed) == 0:
            role = "recent_inactive"
        elif recent_return > 0 and prior_return > 0:
            role = "persistent_positive"
        elif recent_return > 0 >= prior_return:
            role = "recent_regime_hedge"
        elif recent_return < 0 < prior_return:
            role = "recent_deterioration"
        else:
            role = "persistent_weakness"
        regime_rows.append(
            {
                "attempt_id": attempt_id,
                "candidate_name": candidate.row.get("candidate_name"),
                "instruments": "|".join(candidate.instruments),
                "role": role,
                "recent_window_days": len(recent),
                "recent_return_r": recent_return,
                "prior_return_r": prior_return,
                "recent_closed_trades": sum(recent_closed),
                "recent_active_days": sum(1 for value in recent if abs(value) > 1e-12),
                "recent_negative_days": sum(1 for value in recent if value < 0.0),
            }
        )
    return {
        "downside_correlation": correlation_rows,
        "instrument_concentration": concentration_rows,
        "instrument_hhi": instrument_hhi,
        "regime_roles": regime_rows,
        "simultaneous_loss_pressure": {
            "negative_portfolio_days": len(downside_indexes),
            "worst_decile_day_count": len(worst_indexes),
            "worst_decile_cutoff_r": worst_cutoff,
            "peak_open_positions_on_loss_days": max(
                (open_counts[index] for index in downside_indexes), default=0
            ),
            "closed_trades_on_loss_days": sum(
                closed_counts[index] for index in downside_indexes
            ),
            "days_with_three_or_more_losing_strategies": sum(
                1
                for index in range(len(daily))
                if sum(search.by_id[item].vector[index] < 0.0 for item in ids) >= 3
            ),
        },
    }


def analyze_campaign(
    *,
    candidates: list[OptimizerCandidate],
    suite: dict[str, Any],
    account: dict[str, Any],
    experiments: list[dict[str, Any]],
    folds: list[dict[str, Any]],
    temporal_results: list[dict[str, Any]],
    root: Path,
    promotable: bool,
) -> dict[str, Any]:
    analysis_root = root / "analysis"
    candidate_by_id = {candidate.attempt_id: candidate for candidate in candidates}
    full_window_frequency = selection_frequency(
        experiments,
        domain="full_window",
        candidate_by_id=candidate_by_id,
    )
    temporal_frequency = selection_frequency(
        temporal_results,
        domain="temporal",
        candidate_by_id=candidate_by_id,
    )
    combined_frequency = selection_frequency(
        [*experiments, *temporal_results],
        domain="combined_legacy",
        candidate_by_id=candidate_by_id,
    )
    full_window_family_frequency = family_selection_frequency(
        experiments,
        domain="full_window",
        candidate_by_id=candidate_by_id,
    )
    temporal_family_frequency = family_selection_frequency(
        temporal_results,
        domain="temporal",
        candidate_by_id=candidate_by_id,
    )
    full_window_similarity = portfolio_similarity(experiments, domain="full_window")
    temporal_similarity = portfolio_similarity(temporal_results, domain="temporal")
    adjacent_churn = temporal_adjacent_churn(temporal_results)
    policy = suite.get("selection_policy") or {}
    consensus = build_consensus_evidence(
        temporal_results,
        candidate_by_id=candidate_by_id,
        policy=policy,
    )
    behavioral_config = suite.get("behavioral_clustering") or {}
    behavioral_summary: dict[str, Any] = {
        "enabled": bool(behavioral_config.get("enabled", False)),
        "backend": None,
    }
    if behavioral_summary["enabled"] and candidates:
        reference_ids = list(consensus.get("core_attempt_ids") or [])
        if not reference_ids:
            maximum_size = max(
                (int(item) for item in (suite.get("portfolio") or {}).get("sizes") or [1]),
                default=1,
            )
            reference_ids = [
                str(row["attempt_id"])
                for row in (consensus.get("strategy_rows") or [])[:maximum_size]
            ]
        if not reference_ids:
            reference_ids = [candidates[0].attempt_id]
        cluster_threshold = float(behavioral_config.get("cluster_threshold", 0.82))
        behavioral_output = analyze_behavioral_similarity(
            candidates,
            reference_attempt_ids=reference_ids,
            active_epsilon=float(behavioral_config.get("active_epsilon", 1e-9)),
            worst_quantile=float(behavioral_config.get("worst_quantile", 0.1)),
            min_observations=int(behavioral_config.get("min_observations", 3)),
            behavioral_weights=dict(behavioral_config.get("weights") or {}),
            cluster_threshold=cluster_threshold,
        )
        write_json_atomic(
            analysis_root / "behavioral-similarity-matrices.json",
            behavioral_output,
            compact=True,
        )
        behavioral_summary = {
            "enabled": True,
            "cluster_threshold": cluster_threshold,
            **behavioral_cluster_summary(
                behavioral_output,
                candidate_by_id=candidate_by_id,
            ),
        }
        consensus = apply_behavioral_cluster_deduplication(
            consensus,
            assignments=dict(behavioral_summary.get("assignments") or {}),
            attempt_ids=[str(item) for item in behavioral_output["attempt_ids"]],
            similarity_matrix=behavioral_output["similarity_matrix"],
            threshold=cluster_threshold,
        )
        write_csv(
            analysis_root / "behavioral-clusters.csv",
            behavioral_summary["cluster_rows"],
        )
        write_csv(
            analysis_root / "behavioral-cluster-pairs.csv",
            behavioral_summary["pair_rows"],
        )
    write_csv(
        analysis_root / "selection-frequency-full-window.csv",
        full_window_frequency,
    )
    write_csv(
        analysis_root / "selection-frequency-temporal.csv",
        temporal_frequency,
    )
    write_csv(
        analysis_root / "family-selection-frequency-full-window.csv",
        full_window_family_frequency,
    )
    write_csv(
        analysis_root / "family-selection-frequency-temporal.csv",
        temporal_family_frequency,
    )
    write_csv(
        analysis_root / "portfolio-similarity-full-window.csv",
        full_window_similarity,
    )
    write_csv(
        analysis_root / "portfolio-similarity-temporal.csv",
        temporal_similarity,
    )
    write_csv(analysis_root / "temporal-adjacent-churn.csv", adjacent_churn)
    write_csv(
        analysis_root / "consensus-strategies.csv",
        consensus["strategy_rows"],
    )
    write_csv(
        analysis_root / "consensus-families.csv",
        consensus["family_rows"],
    )
    write_json_atomic(analysis_root / "consensus-evidence.json", consensus)
    # Preserve the original aggregate filenames for older report consumers.
    write_csv(analysis_root / "selection-frequency.csv", combined_frequency)
    write_csv(analysis_root / "portfolio-similarity.csv", full_window_similarity)
    fold_rows = [
        {
            "fold_id": (row.get("fold") or {}).get("fold_id"),
            "experiment_id": row.get("experiment_id"),
            "portfolio_size": row.get("portfolio_size"),
            "objective": row.get("objective"),
            "random_seed": row.get("random_seed"),
            "test_final_r": (row.get("test_metrics") or {}).get("final_r"),
            "test_maxdd_r": (row.get("test_metrics") or {}).get("maxdd_r"),
            "test_return_to_dd": (row.get("test_metrics") or {}).get("return_to_dd"),
            "test_neg_weeks": (row.get("test_metrics") or {}).get("neg_weeks"),
        }
        for row in temporal_results
    ]
    write_csv(analysis_root / "fold-results.csv", fold_rows)

    base_experiment = (experiments or temporal_results)[0]
    base_spec = build_experiment_spec(
        suite,
        {
            "experiment_id": "analysis",
            "portfolio_size": int(base_experiment.get("portfolio_size") or 1),
            "objective": str(
                (base_experiment.get("experiment") or {}).get("objective")
                or base_experiment.get("objective")
                or "stability"
            ),
            "random_seed": 17,
            "candidate_limit": -1,
            "risk_weight_multiplier": 1.0,
            "diversification_profile": {"name": "analysis"},
        },
        account=account,
        name_prefix="analysis",
    )
    consensus_portfolios = build_consensus_portfolios(
        candidates=candidates,
        folds=folds,
        suite=suite,
        account=account,
        consensus=consensus,
        base_spec=base_spec,
    )
    consensus_diagnostic_rows: list[dict[str, Any]] = []
    for row in consensus_portfolios:
        gate_reasons: list[str] = []
        if row.get("status") != "complete":
            gate_reasons.append(str(row.get("status") or "incomplete"))
        else:
            if int(row.get("selected_core_count") or 0) < int(
                policy.get("minimum_consensus_core_count", 0)
            ):
                gate_reasons.append("consensus_core_count_below_minimum")
            if float(row.get("selected_core_share") or 0.0) < float(
                policy.get("minimum_consensus_core_share", 0.0)
            ):
                gate_reasons.append("consensus_core_share_below_minimum")
            if int(row.get("negative_test_count") or 0) > int(
                policy.get("max_negative_test_folds", len(folds))
            ):
                gate_reasons.append("too_many_negative_test_folds")
            if float(row.get("worst_test_return_r") or 0.0) < float(
                policy.get("minimum_worst_test_return_r", -math.inf)
            ):
                gate_reasons.append("worst_test_return_below_floor")
            if (row.get("full_metrics") or {}).get("constraint_violations"):
                gate_reasons.append("portfolio_constraint_violation")
            account_initial = (row.get("full_metrics") or {}).get("account_initial") or {}
            if policy.get("require_no_account_failure", True) and account_initial.get(
                "blown"
            ):
                gate_reasons.append("account_failure")
        row["diagnostic_gate_reasons"] = gate_reasons
        row["passes_diagnostic_gates"] = not gate_reasons
        metrics = row.get("full_metrics") or {}
        consensus_diagnostic_rows.append(
            {
                "portfolio_id": row.get("portfolio_id"),
                "portfolio_size": row.get("portfolio_size"),
                "status": row.get("status"),
                "optimizer_backend": row.get("optimizer_backend"),
                "selected_core_count": row.get("selected_core_count"),
                "selected_core_share": row.get("selected_core_share"),
                "full_return_r": metrics.get("final_r"),
                "full_maxdd_r": metrics.get("maxdd_r"),
                "median_test_return_r": row.get("median_test_return_r"),
                "worst_test_return_r": row.get("worst_test_return_r"),
                "worst_test_drawdown_r": row.get("worst_test_drawdown_r"),
                "passes_diagnostic_gates": row.get("passes_diagnostic_gates"),
                "diagnostic_gate_reasons": ",".join(gate_reasons),
            }
        )
    passing_consensus_diagnostics = [
        row
        for row in consensus_portfolios
        if row.get("passes_diagnostic_gates")
    ]
    consensus_diagnostic = (
        max(
            passing_consensus_diagnostics,
            key=lambda row: float(row.get("robust_utility") or -math.inf),
        )
        if passing_consensus_diagnostics
        else None
    )
    write_json_atomic(
        analysis_root / "consensus-portfolios.json",
        consensus_portfolios,
        compact=True,
    )
    write_csv(
        analysis_root / "consensus-portfolios.csv",
        consensus_diagnostic_rows,
    )
    temporal_enabled = bool((suite.get("temporal_validation") or {}).get("enabled", True))
    if temporal_results:
        finalist_rows = temporal_results
        evidence_level = "walk_forward_out_of_sample"
    elif not temporal_enabled:
        finalist_rows = experiments
        evidence_level = "full_window_only"
    else:
        finalist_rows = []
        evidence_level = "missing_temporal_evidence"
    unique = _unique_portfolios(finalist_rows, evidence_level=evidence_level)
    portfolios: list[dict[str, Any]] = []
    consensus_core_ids = set(consensus.get("core_attempt_ids") or [])
    for entry in unique.values():
        full_metrics, test_scenarios = _portfolio_scenario_metrics(
            ids=entry["selected_attempt_ids"],
            candidates=candidates,
            base_spec=base_spec,
            sources=entry["sources"],
        )
        test_returns = [
            float((row.get("metrics") or {}).get("final_r") or 0.0)
            for row in test_scenarios
        ]
        test_drawdowns = [
            float((row.get("metrics") or {}).get("maxdd_r") or 0.0)
            for row in test_scenarios
        ]
        portfolios.append(
            {
                **entry,
                "consensus_core_attempt_ids": sorted(
                    set(entry["selected_attempt_ids"]) & consensus_core_ids
                ),
                "consensus_core_count": len(
                    set(entry["selected_attempt_ids"]) & consensus_core_ids
                ),
                "consensus_core_share": (
                    len(set(entry["selected_attempt_ids"]) & consensus_core_ids)
                    / len(entry["selected_attempt_ids"])
                    if entry["selected_attempt_ids"]
                    else 0.0
                ),
                "non_core_attempt_ids": sorted(
                    set(entry["selected_attempt_ids"]) - consensus_core_ids
                ),
                "full_metrics": full_metrics,
                "test_scenarios": test_scenarios,
                "median_test_return_r": median(test_returns),
                "worst_test_return_r": min(test_returns or [0.0]),
                "worst_test_drawdown_r": max(test_drawdowns or [0.0]),
                "negative_test_count": sum(value < 0 for value in test_returns),
                "robust_utility": _robust_utility(
                    full_metrics, test_scenarios, int(entry["support"])
                ),
            }
        )
    max_negative_tests = int(policy.get("max_negative_test_folds", len(folds)))
    minimum_test_return = float(policy.get("minimum_worst_test_return_r", -math.inf))
    minimum_selection_support = max(
        1, int(policy.get("minimum_selection_support", 1))
    )
    minimum_oos_scenarios = max(0, int(policy.get("minimum_oos_scenarios", 0)))
    minimum_consensus_core_count = max(
        0, int(policy.get("minimum_consensus_core_count", 0))
    )
    minimum_consensus_core_share = max(
        0.0, float(policy.get("minimum_consensus_core_share", 0.0))
    )
    maximum_median_adjacent_fold_churn = float(
        policy.get("maximum_median_adjacent_fold_churn", 1.0)
    )
    median_adjacent_fold_churn = median(row["churn"] for row in adjacent_churn)
    for row in portfolios:
        gate_reasons: list[str] = []
        if int(row.get("support") or 0) < minimum_selection_support:
            gate_reasons.append("selection_support_below_minimum")
        if len(row.get("test_scenarios") or []) < minimum_oos_scenarios:
            gate_reasons.append("oos_scenario_count_below_minimum")
        if int(row.get("consensus_core_count") or 0) < minimum_consensus_core_count:
            gate_reasons.append("consensus_core_count_below_minimum")
        if float(row.get("consensus_core_share") or 0.0) < minimum_consensus_core_share:
            gate_reasons.append("consensus_core_share_below_minimum")
        if median_adjacent_fold_churn > maximum_median_adjacent_fold_churn:
            gate_reasons.append("temporal_churn_above_maximum")
        if row["negative_test_count"] > max_negative_tests:
            gate_reasons.append("too_many_negative_test_folds")
        if row["worst_test_return_r"] < minimum_test_return:
            gate_reasons.append("worst_test_return_below_floor")
        if (row.get("full_metrics") or {}).get("constraint_violations"):
            gate_reasons.append("portfolio_constraint_violation")
        account_initial = (row.get("full_metrics") or {}).get("account_initial") or {}
        if policy.get("require_no_account_failure", True) and account_initial.get("blown"):
            gate_reasons.append("account_failure")
        row["gate_reasons"] = gate_reasons
        row["passes_gates"] = not gate_reasons
    passing = [row for row in portfolios if row["passes_gates"]]
    gates_relaxed = bool(
        portfolios
        and not passing
        and policy.get("allow_gate_relaxation", False)
    )
    if gates_relaxed:
        passing = portfolios
    passing.sort(key=lambda row: float(row["robust_utility"]), reverse=True)
    champion = passing[0] if passing else None
    conservative = (
        min(
            passing,
            key=lambda row: (
                float(row["worst_test_drawdown_r"]),
                -float(row["robust_utility"]),
            ),
        )
        if passing
        else None
    )
    return_alternate = (
        max(
            passing,
            key=lambda row: float((row.get("full_metrics") or {}).get("final_r") or 0.0),
        )
        if passing
        else None
    )
    finalists = {
        "champion": champion,
        "conservative": conservative,
        "return_alternate": return_alternate,
    }
    finalist_root = root / "finalists"
    for name, payload in finalists.items():
        finalist_path = finalist_root / name / "portfolio.json"
        if payload is not None:
            payload["promotion_eligible"] = bool(
                promotable
                and payload.get("passes_gates")
                and payload.get("evidence_level")
                == "level_ab_inner_consensus_frozen_cell_outer_test"
            )
            write_json_atomic(finalist_path, payload)
        else:
            finalist_path.unlink(missing_ok=True)

    statistics_payload: dict[str, Any] = {
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "portfolio_count": len(portfolios),
        "experiment_count": len(experiments),
        "temporal_result_count": len(temporal_results),
        "fold_count": len(folds),
        "stability_domains": {
            "full_window": similarity_summary(full_window_similarity),
            "temporal": similarity_summary(temporal_similarity),
            "temporal_adjacent_churn": {
                "comparison_count": len(adjacent_churn),
                "median_churn": median(row["churn"] for row in adjacent_churn),
                "maximum_churn": max(
                    (float(row["churn"]) for row in adjacent_churn),
                    default=None,
                ),
            },
        },
        "consensus": {
            "raw_core_count": consensus["raw_core_count"],
            "core_count": consensus["core_count"],
            "minimum_consensus_core_count": minimum_consensus_core_count,
            "minimum_consensus_core_share": minimum_consensus_core_share,
            "diagnostic_portfolio_count": len(consensus_portfolios),
            "passing_diagnostic_portfolio_count": len(
                passing_consensus_diagnostics
            ),
        },
        "behavioral_clustering": {
            key: value
            for key, value in behavioral_summary.items()
            if key not in {"assignments", "cluster_rows", "pair_rows"}
        },
        "probability_of_backtest_overfitting": {
            "supported": False,
            "reason": "Rolling folds are not a symmetric combinatorially purged cross-validation design.",
        },
    }
    diagnostic_target = champion or (
        max(portfolios, key=lambda row: float(row.get("robust_utility") or -math.inf))
        if portfolios
        else None
    )
    if champion:
        champion_search = PortfolioSearch(
            candidates, replace(base_spec, portfolio_size=len(champion["selected_attempt_ids"]))
        )
        daily, _open, _closed = champion_search.combine_vectors(
            champion["selected_attempt_ids"]
        )
        statistics_payload["champion_block_bootstrap"] = _bootstrap_total_return_ci(daily)
        statistics_payload["champion_deflated_sharpe"] = _deflated_sharpe(
            daily, trial_count=len(experiments)
        )
        statistics_payload["champion_leave_one_out"] = [
            {
                "removed_attempt_id": attempt_id,
                "final_r_without": champion_search.metrics(
                    [item for item in champion["selected_attempt_ids"] if item != attempt_id],
                    include_account=False,
                ).get("final_r"),
                "maxdd_r_without": champion_search.metrics(
                    [item for item in champion["selected_attempt_ids"] if item != attempt_id],
                    include_account=False,
                ).get("maxdd_r"),
            }
            for attempt_id in champion["selected_attempt_ids"]
        ]
    if diagnostic_target:
        diagnostic_search = PortfolioSearch(
            candidates,
            replace(
                base_spec,
                portfolio_size=len(diagnostic_target["selected_attempt_ids"]),
            ),
        )
        diagnostics = _champion_diagnostics(
            champion=diagnostic_target,
            candidates=candidates,
            search=diagnostic_search,
        )
        statistics_payload["diagnostic_portfolio"] = {
            "portfolio_id": diagnostic_target.get("portfolio_id"),
            "selection": "champion" if champion else "best_rejected_candidate",
            "gate_reasons": diagnostic_target.get("gate_reasons") or [],
            "instrument_hhi": diagnostics["instrument_hhi"],
            "simultaneous_loss_pressure": diagnostics["simultaneous_loss_pressure"],
        }
        if champion:
            statistics_payload["champion_instrument_hhi"] = diagnostics["instrument_hhi"]
            statistics_payload["champion_simultaneous_loss_pressure"] = diagnostics[
                "simultaneous_loss_pressure"
            ]
        write_csv(
            analysis_root / "downside-correlation.csv",
            diagnostics["downside_correlation"],
        )
        write_csv(
            analysis_root / "instrument-concentration.csv",
            diagnostics["instrument_concentration"],
        )
        write_csv(
            analysis_root / "regime-roles.csv",
            diagnostics["regime_roles"],
        )
    else:
        write_csv(analysis_root / "downside-correlation.csv", [])
        write_csv(analysis_root / "instrument-concentration.csv", [])
        write_csv(analysis_root / "regime-roles.csv", [])
    write_json_atomic(analysis_root / "statistical-tests.json", statistics_payload)
    write_json_atomic(analysis_root / "portfolio-evaluations.json", portfolios, compact=True)
    return {
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "selection_frequency": combined_frequency,
        "similarity": full_window_similarity,
        "stability_domains": {
            "full_window": {
                "selection_frequency": full_window_frequency,
                "family_selection_frequency": full_window_family_frequency,
                "similarity": full_window_similarity,
                "summary": similarity_summary(full_window_similarity),
            },
            "temporal": {
                "selection_frequency": temporal_frequency,
                "family_selection_frequency": temporal_family_frequency,
                "similarity": temporal_similarity,
                "summary": similarity_summary(temporal_similarity),
                "adjacent_churn": adjacent_churn,
            },
        },
        "consensus": consensus,
        "consensus_portfolios": consensus_portfolios,
        "consensus_diagnostic": consensus_diagnostic,
        "behavioral_clustering": {
            key: value
            for key, value in behavioral_summary.items()
            if key != "pair_rows"
        },
        "fold_rows": fold_rows,
        "portfolio_evaluations": portfolios,
        "statistics": statistics_payload,
        "finalists": finalists,
        "gates_relaxed_for_ranking": gates_relaxed,
        "finalist_evidence_level": evidence_level,
    }


def render_markdown_report(payload: dict[str, Any]) -> str:
    campaign = payload.get("campaign") or {}
    health = payload.get("corpus_health") or {}
    analysis = payload.get("analysis") or {}
    finalists = analysis.get("finalists") or {}
    stability = analysis.get("stability_domains") or {}
    full_window_stability = stability.get("full_window") or {}
    temporal_stability = stability.get("temporal") or {}
    full_window_summary = full_window_stability.get("summary") or {}
    temporal_summary = temporal_stability.get("summary") or {}
    adjacent_churn = temporal_stability.get("adjacent_churn") or []
    consensus = analysis.get("consensus") or {}
    behavioral = analysis.get("behavioral_clustering") or {}
    consensus_diagnostic = analysis.get("consensus_diagnostic") or {}
    lines = [
        f"# Portfolio Research: {campaign.get('campaign_id')}",
        "",
        f"- Suite: `{campaign.get('suite_name')}`",
        f"- Status: `{campaign.get('status')}`",
        f"- Promotable: `{campaign.get('promotable')}`",
        f"- Candidate count: `{payload.get('candidate_count')}`",
        f"- Experiment count: `{payload.get('experiment_count')}`",
        f"- Temporal folds: `{payload.get('fold_count')}`",
        f"- Common calendar: `{payload.get('calendar', {}).get('common_effective_start')}` to `{payload.get('calendar', {}).get('common_effective_end')}`",
        "",
        "## Corpus Health",
        "",
        f"- Catchup status: `{health.get('status')}`",
        f"- Invalid after catchup: `{health.get('invalid_after_catchup_count', 0)}`",
        f"- Candidate filter rejections: `{health.get('optimizer_rejections', {})}`",
        "",
        "## Selection Stability",
        "",
        "Full-window perturbations and temporal train-window selections are reported separately.",
        "",
        f"- Full-window comparisons: `{full_window_summary.get('comparison_count', 0)}`",
        f"- Full-window median Jaccard: `{float((full_window_summary.get('jaccard') or {}).get('median') or 0.0):.3f}`",
        f"- Temporal comparisons: `{temporal_summary.get('comparison_count', 0)}`",
        f"- Temporal median Jaccard: `{float((temporal_summary.get('jaccard') or {}).get('median') or 0.0):.3f}`",
        f"- Temporal median shared members: `{float((temporal_summary.get('shared_member_count') or {}).get('median') or 0.0):.1f}`",
        f"- Adjacent-fold comparisons: `{len(adjacent_churn)}`",
        f"- Adjacent-fold median churn: `{median(row.get('churn') or 0.0 for row in adjacent_churn):.3f}`",
        "",
        "## Consensus Evidence",
        "",
        f"- Raw fold-stable core: `{consensus.get('raw_core_count', 0)}` strategies",
        f"- Deduplicated stable core: `{consensus.get('core_count', 0)}` strategies",
        f"- Behavioral backend: `{behavioral.get('backend')}`",
        f"- Behavioral clusters: `{behavioral.get('cluster_count', 0)}`",
        f"- Largest behavioral cluster: `{behavioral.get('largest_cluster_size', 0)}` strategies",
        "",
        "Consensus portfolios are diagnostic cross-fold replays. They require a later untouched outer test before promotion.",
        "",
        f"- Best diagnostic portfolio: `{consensus_diagnostic.get('portfolio_id') or 'none'}`",
        f"- Diagnostic portfolio size: `{consensus_diagnostic.get('portfolio_size') or 0}`",
        f"- Diagnostic median test return: `{float(consensus_diagnostic.get('median_test_return_r') or 0.0):.3f} R`",
        f"- Diagnostic worst test return: `{float(consensus_diagnostic.get('worst_test_return_r') or 0.0):.3f} R`",
        f"- Diagnostic promotion eligible: `False`",
        "",
        "## Finalists",
        "",
    ]
    for name in ("champion", "conservative", "return_alternate"):
        finalist = finalists.get(name)
        if not finalist:
            lines.append(f"### {name.replace('_', ' ').title()}\n\nNot available.\n")
            continue
        metrics = finalist.get("full_metrics") or {}
        lines.extend(
            [
                f"### {name.replace('_', ' ').title()}",
                "",
                f"- Portfolio ID: `{finalist.get('portfolio_id')}`",
                f"- Strategies: `{len(finalist.get('selected_attempt_ids') or [])}`",
                f"- Full-window return: `{float(metrics.get('final_r') or 0.0):.3f} R`",
                f"- Full-window max drawdown: `{float(metrics.get('maxdd_r') or 0.0):.3f} R`",
                f"- Median test return: `{float(finalist.get('median_test_return_r') or 0.0):.3f} R`",
                f"- Worst test return: `{float(finalist.get('worst_test_return_r') or 0.0):.3f} R`",
                f"- Worst test drawdown: `{float(finalist.get('worst_test_drawdown_r') or 0.0):.3f} R`",
                f"- Selection support: `{finalist.get('support')}`",
                f"- Gate reasons: `{finalist.get('gate_reasons') or []}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Interpretation Limits",
            "",
            "Level A temporal validation freezes strategy definitions and execution cells. It tests portfolio selection stability, but does not remove strategy-generation or exit-cell lookahead. Nested cell selection and historical AutoResearch simulation remain separate higher-cost validation levels.",
            "",
        ]
    )
    return "\n".join(lines)


def run_research_campaign(
    *,
    campaign_root: Path,
    campaign_id: str,
    suite_name: str,
    suite: dict[str, Any],
    rows: list[dict[str, Any]],
    account: dict[str, Any],
    corpus_health: dict[str, Any],
    provenance: dict[str, Any],
    optimizer_backend: str,
    experiment_limit: int | None = None,
    resume: bool = False,
) -> dict[str, Any]:
    ledger = CampaignLedger(campaign_root, campaign_id=campaign_id, suite_name=suite_name)
    ledger.acquire(resume=resume)
    try:
        campaign_path = campaign_root / "campaign.json"
        campaign_payload: dict[str, Any] = {}
        if resume and campaign_path.exists():
            try:
                campaign_payload = json.loads(campaign_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                campaign_payload = {}
        if resume and campaign_payload:
            existing_limit = campaign_payload.get("experiment_limit")
            existing_backend = str(campaign_payload.get("optimizer_backend") or "")
            if existing_limit != experiment_limit:
                raise PortfolioResearchError(
                    "Resume experiment_limit differs from the frozen campaign setting "
                    f"({existing_limit!r} != {experiment_limit!r})."
                )
            if existing_backend and existing_backend != optimizer_backend:
                raise PortfolioResearchError(
                    "Resume optimizer backend differs from the frozen campaign setting "
                    f"({existing_backend} != {optimizer_backend})."
                )
            ledger_status = ledger._status()
            completed_outputs = ledger_status.get("outputs") or {}
            completed_report = Path(str(completed_outputs.get("report_json") or ""))
            if (
                campaign_payload.get("status") == "complete"
                and ledger_status.get("status") == "complete"
                and completed_report.is_file()
            ):
                completion_manifest_path = campaign_root / "campaign-manifest.json"
                completed_markdown = Path(
                    str(completed_outputs.get("report_markdown") or "")
                )
                validate_artifact_manifest(
                    completion_manifest_path,
                    campaign_root=campaign_root,
                    required_paths=(
                        campaign_path,
                        completed_report,
                        completed_markdown,
                        campaign_root / "inputs" / "candidate-snapshot.json",
                        campaign_root / "inputs" / "candidate-snapshot-manifest.json",
                        *(
                            (campaign_root / "inputs" / "frozen-level-c-cohort.json",)
                            if (campaign_root / "inputs" / "frozen-level-c-cohort.json").is_file()
                            else ()
                        ),
                    ),
                )
                ledger.event("campaign_resume_complete", report_json=str(completed_report))
                return completed_outputs

        suite_path = campaign_root / "inputs" / "resolved-suite.json"
        provenance_path = campaign_root / "inputs" / "provenance.json"
        health_path = campaign_root / "inputs" / "corpus-health.json"
        account_path = campaign_root / "inputs" / "resolved-account.json"
        if resume:
            frozen_inputs = {
                "suite": suite_path,
                "provenance": provenance_path,
                "corpus_health": health_path,
                "account": account_path,
            }
            missing_frozen = [name for name, path in frozen_inputs.items() if not path.is_file()]
            if missing_frozen:
                raise PortfolioResearchError(
                    "Resume is missing frozen campaign inputs: " + ", ".join(missing_frozen)
                )
            suite = json.loads(suite_path.read_text(encoding="utf-8"))
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
            corpus_health = json.loads(health_path.read_text(encoding="utf-8"))
            account = json.loads(account_path.read_text(encoding="utf-8"))
        campaign_payload.update(
            {
                "schema_version": CAMPAIGN_SCHEMA_VERSION,
                "campaign_id": campaign_id,
                "suite_name": suite_name,
                "created_at": campaign_payload.get("created_at") or utc_now(),
                "status": "running",
                "promotable": bool(corpus_health.get("promotable", True))
                and experiment_limit is None
                and bool((suite.get("temporal_validation") or {}).get("enabled", True)),
                "optimizer_backend": optimizer_backend,
                "experiment_limit": experiment_limit,
            }
        )
        write_json_atomic(campaign_path, campaign_payload)
        if not resume:
            write_json_immutable(suite_path, suite)
            write_json_immutable(provenance_path, provenance)
            write_json_atomic(health_path, corpus_health)
            write_json_immutable(account_path, account)

        snapshot_path = campaign_root / "inputs" / "candidate-snapshot.json"
        snapshot_manifest_path = (
            campaign_root / "inputs" / "candidate-snapshot-manifest.json"
        )
        if resume and snapshot_path.exists():
            validate_artifact_manifest(
                snapshot_manifest_path,
                campaign_root=campaign_root,
                required_paths=(snapshot_path,),
            )
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            candidates = [
                candidate_from_snapshot(item) for item in snapshot.get("candidates") or []
            ]
            calendar = dict(snapshot.get("calendar") or {})
        else:
            snapshot_spec_kwargs = _spec_kwargs(
                (suite.get("portfolio") or {}).get("base_optimizer_args") or {}
            )
            snapshot_spec_kwargs.update(
                {
                    "portfolio_name": f"{campaign_id}-snapshot",
                    "portfolio_size": max(
                        int(item) for item in (suite.get("portfolio") or {}).get("sizes") or [1]
                    ),
                    "candidate_limit": -1,
                    "min_score": float("-inf"),
                    "require_positive_source_return": False,
                    "account": dict(account),
                }
            )
            snapshot_spec = PortfolioOptimizerSpec(**snapshot_spec_kwargs)
            candidates, optimizer_rejections = build_optimizer_candidates(rows, snapshot_spec)
            candidates, calendar = intersect_candidate_calendar(candidates)
            requested_size = max(
                int(item) for item in (suite.get("portfolio") or {}).get("sizes") or [1]
            )
            if len(candidates) < requested_size:
                raise PortfolioResearchError(
                    f"Only {len(candidates)} candidates survived; {requested_size} required"
                )
            corpus_health["optimizer_rejections"] = optimizer_rejections
            corpus_health["candidate_count"] = len(candidates)
            corpus_health["calendar"] = calendar
            write_json_atomic(health_path, corpus_health)
            snapshot = {
                "schema_version": 1,
                "generated_at": utc_now(),
                "calendar": calendar,
                "candidate_count": len(candidates),
                "candidates": [candidate_snapshot_payload(item) for item in candidates],
            }
            write_json_immutable(snapshot_path, snapshot)
            write_artifact_manifest(
                snapshot_manifest_path,
                campaign_root=campaign_root,
                paths=(snapshot_path,),
                fields_payload={
                    "candidate_count": len(candidates),
                    "calendar": calendar,
                },
            )

        experiment_input = {
            "suite": suite,
            "snapshot_sha256": file_sha256(snapshot_path),
            "limit": experiment_limit,
        }
        experiment_digest = payload_hash(experiment_input)
        experiment_index_path = campaign_root / "experiments" / "experiment-index.json"
        if resume and ledger.stage_outputs_valid(
            "experiments", experiment_digest, [experiment_index_path]
        ):
            experiments = json.loads(experiment_index_path.read_text(encoding="utf-8"))
            ledger.stage_skip("experiments", experiment_digest)
        else:
            input_digest = ledger.stage_start("experiments", experiment_input)
            experiments = run_experiment_matrix(
                candidates=candidates,
                suite=suite,
                account=account,
                root=campaign_root / "experiments",
                backend=optimizer_backend,
                experiment_limit=experiment_limit,
                ledger=ledger,
            )
            ledger.stage_done(
                "experiments",
                input_digest,
                outputs={"experiment_count": len(experiments)},
                output_paths=[experiment_index_path],
            )

        temporal_input = {
            "suite": suite.get("temporal_validation"),
            "snapshot_sha256": file_sha256(snapshot_path),
        }
        temporal_digest = payload_hash(temporal_input)
        fold_index_path = campaign_root / "temporal-validation" / "fold-index.json"
        temporal_results_path = campaign_root / "temporal-validation" / "temporal-results.json"
        if resume and ledger.stage_outputs_valid(
            "temporal_validation", temporal_digest, [fold_index_path, temporal_results_path]
        ):
            folds = json.loads(fold_index_path.read_text(encoding="utf-8"))
            temporal_results = json.loads(temporal_results_path.read_text(encoding="utf-8"))
            ledger.stage_skip("temporal_validation", temporal_digest)
        else:
            input_digest = ledger.stage_start("temporal_validation", temporal_input)
            folds, temporal_results = run_temporal_validation(
                candidates=candidates,
                suite=suite,
                account=account,
                root=campaign_root / "temporal-validation",
                backend=optimizer_backend,
                ledger=ledger,
            )
            ledger.stage_done(
                "temporal_validation",
                input_digest,
                outputs={"fold_count": len(folds), "result_count": len(temporal_results)},
                output_paths=[fold_index_path, temporal_results_path],
            )

        analysis_input = {
            "experiment_hash": payload_hash(experiments),
            "temporal_hash": payload_hash(temporal_results),
            "selection_policy": suite.get("selection_policy"),
        }
        analysis_digest = payload_hash(analysis_input)
        analysis_result_path = campaign_root / "analysis" / "result.json"
        if resume and ledger.stage_outputs_valid(
            "analysis", analysis_digest, [analysis_result_path]
        ):
            analysis = json.loads(analysis_result_path.read_text(encoding="utf-8"))
            ledger.stage_skip("analysis", analysis_digest)
        else:
            input_digest = ledger.stage_start("analysis", analysis_input)
            analysis = analyze_campaign(
                candidates=candidates,
                suite=suite,
                account=account,
                experiments=experiments,
                folds=folds,
                temporal_results=temporal_results,
                root=campaign_root,
                promotable=campaign_payload["promotable"],
            )
            write_json_atomic(analysis_result_path, analysis, compact=True)
            ledger.stage_done(
                "analysis",
                input_digest,
                outputs={"portfolio_count": len(analysis["portfolio_evaluations"])},
                output_paths=[analysis_result_path],
            )

        if not (analysis.get("finalists") or {}).get("champion"):
            campaign_payload["promotable"] = False
            campaign_payload["non_promotable_reason"] = "no_finalist_passed_selection_gates"

        campaign_payload["status"] = "complete"
        campaign_payload["completed_at"] = campaign_payload.get("completed_at") or utc_now()
        write_json_atomic(campaign_root / "campaign.json", campaign_payload)
        report = {
            "campaign": campaign_payload,
            "corpus_health": corpus_health,
            "provenance": provenance,
            "calendar": calendar,
            "candidate_count": len(candidates),
            "experiment_count": len(experiments),
            "fold_count": len(folds),
            "temporal_result_count": len(temporal_results),
            "analysis": analysis,
        }
        report_path = campaign_root / "report.json"
        write_json_immutable(report_path, report)
        markdown_path = campaign_root / "report.md"
        write_text_immutable(markdown_path, render_markdown_report(report))
        completion_paths = [
            campaign_root / "campaign.json",
            report_path,
            markdown_path,
            snapshot_path,
            snapshot_manifest_path,
            analysis_result_path,
            experiment_index_path,
            fold_index_path,
            temporal_results_path,
        ]
        frozen_cohort_path = campaign_root / "inputs" / "frozen-level-c-cohort.json"
        if frozen_cohort_path.is_file():
            completion_paths.append(frozen_cohort_path)
        completion_paths.extend(
            path
            for path in (
                campaign_root / "finalists" / name / "portfolio.json"
                for name in ("champion", "conservative", "return_alternate")
            )
            if path.is_file()
        )
        write_artifact_manifest(
            campaign_root / "campaign-manifest.json",
            campaign_root=campaign_root,
            paths=completion_paths,
            fields_payload={
                "campaign_id": campaign_id,
                "completed_at": campaign_payload["completed_at"],
            },
        )
        champion = (analysis.get("finalists") or {}).get("champion")
        summary = {
            "campaign_id": campaign_id,
            "campaign_root": str(campaign_root),
            "report_json": str(report_path),
            "report_markdown": str(markdown_path),
            "candidate_count": len(candidates),
            "experiment_count": len(experiments),
            "fold_count": len(folds),
            "promotable": campaign_payload["promotable"],
            "champion": finalist_summary(champion),
        }
        ledger.complete(summary)
        return summary
    except Exception as exc:
        ledger.fail(exc)
        raise
    finally:
        ledger.release()


def rebuild_research_report(*, campaign_root: Path) -> dict[str, Any]:
    required = {
        "campaign": campaign_root / "campaign.json",
        "suite": campaign_root / "inputs" / "resolved-suite.json",
        "snapshot": campaign_root / "inputs" / "candidate-snapshot.json",
        "health": campaign_root / "inputs" / "corpus-health.json",
        "provenance": campaign_root / "inputs" / "provenance.json",
        "account": campaign_root / "inputs" / "resolved-account.json",
        "experiments": campaign_root / "experiments" / "experiment-index.json",
        "folds": campaign_root / "temporal-validation" / "fold-index.json",
        "temporal": campaign_root / "temporal-validation" / "temporal-results.json",
    }
    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        raise PortfolioResearchError(
            f"Campaign cannot rebuild report; missing artifacts: {', '.join(missing)}"
        )
    payloads = {
        name: json.loads(path.read_text(encoding="utf-8"))
        for name, path in required.items()
    }
    snapshot = payloads["snapshot"]
    candidates = [
        candidate_from_snapshot(item) for item in snapshot.get("candidates") or []
    ]
    rebuild_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    rebuild_root = campaign_root / "report-rebuilds" / rebuild_id
    analysis = analyze_campaign(
        candidates=candidates,
        suite=payloads["suite"],
        account=payloads["account"],
        experiments=payloads["experiments"],
        folds=payloads["folds"],
        temporal_results=payloads["temporal"],
        root=rebuild_root,
        promotable=bool(payloads["campaign"].get("promotable")),
    )
    if not (analysis.get("finalists") or {}).get("champion"):
        payloads["campaign"]["promotable"] = False
        payloads["campaign"][
            "non_promotable_reason"
        ] = "no_finalist_passed_selection_gates"
    report = {
        "campaign": payloads["campaign"],
        "corpus_health": payloads["health"],
        "provenance": payloads["provenance"],
        "calendar": snapshot.get("calendar") or {},
        "candidate_count": len(candidates),
        "experiment_count": len(payloads["experiments"]),
        "fold_count": len(payloads["folds"]),
        "temporal_result_count": len(payloads["temporal"]),
        "analysis": analysis,
        "report_rebuilt_at": utc_now(),
    }
    report_path = rebuild_root / "report.json"
    markdown_path = rebuild_root / "report.md"
    write_json_immutable(report_path, report)
    write_text_immutable(markdown_path, render_markdown_report(report))
    write_json_immutable(
        rebuild_root / "manifest.json",
        {
            "schema_version": 1,
            "campaign_root": str(campaign_root),
            "rebuild_id": rebuild_id,
            "report_json": str(report_path),
            "report_json_sha256": file_sha256(report_path),
            "report_markdown": str(markdown_path),
            "report_markdown_sha256": file_sha256(markdown_path),
            "source_artifacts": {
                name: {"path": str(path), "sha256": file_sha256(path)}
                for name, path in required.items()
            },
        },
    )
    return {
        "campaign_id": payloads["campaign"].get("campaign_id"),
        "campaign_root": str(campaign_root),
        "report_json": str(report_path),
        "report_markdown": str(markdown_path),
        "candidate_count": len(candidates),
        "experiment_count": len(payloads["experiments"]),
        "fold_count": len(payloads["folds"]),
        "champion": finalist_summary((analysis.get("finalists") or {}).get("champion")),
    }


def package_research_finalist(
    *,
    campaign_root: Path,
    finalist_name: str,
) -> dict[str, Any]:
    finalist_token = slug(finalist_name).replace("-", "_")
    if finalist_token not in {"champion", "conservative", "return_alternate"}:
        raise PortfolioResearchError(f"Unsupported campaign finalist: {finalist_name}")
    required = {
        "campaign": campaign_root / "campaign.json",
        "completion_manifest": campaign_root / "campaign-manifest.json",
        "report": campaign_root / "report.json",
        "snapshot": campaign_root / "inputs" / "candidate-snapshot.json",
        "snapshot_manifest": campaign_root / "inputs" / "candidate-snapshot-manifest.json",
        "portfolio": campaign_root / "finalists" / finalist_token / "portfolio.json",
    }
    missing = [name for name, path in required.items() if not path.is_file()]
    if missing:
        raise PortfolioResearchError(
            "Campaign finalist cannot be packaged; missing artifacts: " + ", ".join(missing)
        )
    campaign = json.loads(required["campaign"].read_text(encoding="utf-8"))
    report = json.loads(required["report"].read_text(encoding="utf-8"))
    portfolio = json.loads(required["portfolio"].read_text(encoding="utf-8"))
    snapshot = json.loads(required["snapshot"].read_text(encoding="utf-8"))
    validate_artifact_manifest(
        required["snapshot_manifest"],
        campaign_root=campaign_root,
        required_paths=(required["snapshot"],),
    )
    validate_artifact_manifest(
        required["completion_manifest"],
        campaign_root=campaign_root,
        required_paths=(
            required["campaign"],
            required["report"],
            required["snapshot"],
            required["snapshot_manifest"],
            required["portfolio"],
        ),
    )
    if campaign.get("status") != "complete":
        raise PortfolioResearchError("Campaign must be complete before finalist packaging")
    if not campaign.get("promotable"):
        raise PortfolioResearchError("Campaign is not promotable; packaging fails closed")
    if canonical_json(report.get("campaign") or {}) != canonical_json(campaign):
        raise PortfolioResearchError(
            "Completed report campaign state differs from campaign.json"
        )
    level_c_lineage = (report.get("provenance") or {}).get("level_c_cohort") or {}
    if not level_c_lineage.get("verified"):
        raise PortfolioResearchError(
            "Finalist packaging requires a verified frozen Level C discovery cohort"
        )
    reported_finalist = (
        ((report.get("analysis") or {}).get("finalists") or {}).get(finalist_token)
    )
    if canonical_json(reported_finalist) != canonical_json(portfolio):
        raise PortfolioResearchError(
            f"Finalist {finalist_token} differs from the completed campaign report"
        )
    if not portfolio.get("promotion_eligible"):
        raise PortfolioResearchError(
            f"Finalist {finalist_token} is not promotion eligible; packaging fails closed"
        )
    if (
        portfolio.get("evidence_level")
        != "level_ab_inner_consensus_frozen_cell_outer_test"
    ):
        raise PortfolioResearchError(
            "Finalist packaging requires combined Level A/Level B frozen-cell outer evidence"
        )

    candidates_by_id = {
        str(item.get("attempt_id") or ""): item
        for item in snapshot.get("candidates") or []
    }
    selected_ids = [str(item) for item in portfolio.get("selected_attempt_ids") or []]
    profile_sources: list[dict[str, Any]] = []
    for attempt_id in selected_ids:
        candidate = candidates_by_id.get(attempt_id)
        if not candidate:
            raise PortfolioResearchError(
                f"Finalist member is absent from the frozen snapshot: {attempt_id}"
            )
        source = candidate.get("source") or {}
        profile_path_raw = str((source.get("paths") or {}).get("profile_path") or "")
        profile_path = Path(profile_path_raw)
        expected_sha = str((source.get("sha256") or {}).get("profile_path") or "")
        observed_sha = file_sha256(profile_path)
        if not profile_path.is_file() or not expected_sha or observed_sha != expected_sha:
            raise PortfolioResearchError(
                f"Frozen profile source is missing or changed for {attempt_id}: {profile_path}"
            )
        profile_sources.append(
            {
                "attempt_id": attempt_id,
                "candidate_name": candidate.get("candidate_name"),
                "source_path": str(profile_path),
                "sha256": observed_sha,
            }
        )

    package_identity = {
        "schema_version": 1,
        "campaign_id": campaign.get("campaign_id"),
        "finalist": finalist_token,
        "campaign_report_sha256": file_sha256(required["report"]),
        "finalist_portfolio_sha256": file_sha256(required["portfolio"]),
        "candidate_snapshot_sha256": file_sha256(required["snapshot"]),
        "profiles": profile_sources,
    }
    package_digest = payload_hash(package_identity)
    package_root = campaign_root / "packages" / finalist_token / package_digest
    profiles_root = package_root / "profiles"
    profiles_root.mkdir(parents=True, exist_ok=True)
    packaged_profiles: list[dict[str, Any]] = []
    for rank, source in enumerate(profile_sources, start=1):
        destination = profiles_root / f"{rank:02d}-{slug(source['candidate_name'] or source['attempt_id'])}.json"
        candidate = candidates_by_id[source["attempt_id"]]
        profile_document = json.loads(Path(source["source_path"]).read_text(encoding="utf-8"))
        profile_payload = (
            profile_document.get("profile")
            if isinstance(profile_document, dict)
            and isinstance(profile_document.get("profile"), dict)
            else profile_document
        )
        if not isinstance(profile_payload, dict):
            raise PortfolioResearchError(
                f"Frozen profile source is not a JSON object: {source['source_path']}"
            )
        daily_r = [float(value) for value in candidate.get("daily_r") or []]
        peak = 0.0
        max_drawdown_r = 0.0
        for value in daily_r:
            peak = max(peak, value)
            max_drawdown_r = max(max_drawdown_r, peak - value)
        trade_count = int(candidate.get("trade_count") or 0)
        final_r = daily_r[-1] if daily_r else None
        source_metadata = candidate.get("source") or {}
        profile_payload["researchEvidence"] = {
            "campaignId": campaign.get("campaign_id"),
            "campaignReportSha256": package_identity["campaign_report_sha256"],
            "finalist": finalist_token,
            "portfolioRole": "active",
            "attemptId": source["attempt_id"],
            "profileFingerprint": source_metadata.get("profile_fingerprint"),
            "structuralFamily": candidate.get("structural_family_signature")
            or candidate.get("family"),
            "instruments": candidate.get("instruments") or [],
            "evidenceLevel": portfolio.get("evidence_level"),
            "effectiveStart": (candidate.get("dates") or [None])[0],
            "effectiveEnd": (candidate.get("dates") or [None])[-1],
            "expectedTradesPerMonth": candidate.get("trades_per_month"),
            "expectedAverageHoldHours": candidate.get("avg_hold_hours"),
            "expectedFinalR": final_r,
            "expectedMaxDrawdownR": max_drawdown_r,
            "expectedAverageRPerTrade": (
                final_r / trade_count if final_r is not None and trade_count > 0 else None
            ),
        }
        write_json_immutable(destination, profile_document)
        packaged_profiles.append(
            {
                **source,
                "package_path": str(destination),
                "package_sha256": file_sha256(destination),
                "research_evidence": profile_payload["researchEvidence"],
            }
        )
    for name in ("report", "portfolio", "snapshot"):
        destination = package_root / required[name].name
        if destination.exists() and file_sha256(destination) != file_sha256(required[name]):
            raise PortfolioResearchError(f"Immutable package artifact differs: {destination}")
        if not destination.exists():
            shutil.copy2(required[name], destination)
    manifest = {
        **package_identity,
        "package_digest": package_digest,
        "package_root": str(package_root),
        "approval": {"method": "explicit_portfolio_research_package_command"},
        "packaged_profiles": packaged_profiles,
    }
    manifest_path = package_root / "manifest.json"
    write_json_immutable(manifest_path, manifest)
    return {
        "campaign_id": campaign.get("campaign_id"),
        "finalist": finalist_token,
        "package_digest": package_digest,
        "package_root": str(package_root),
        "manifest_path": str(manifest_path),
        "profile_count": len(packaged_profiles),
    }


def finalist_summary(finalist: dict[str, Any] | None) -> dict[str, Any] | None:
    if not finalist:
        return None
    metrics = finalist.get("full_metrics") or {}
    return {
        "portfolio_id": finalist.get("portfolio_id"),
        "selected_attempt_ids": finalist.get("selected_attempt_ids") or [],
        "support": finalist.get("support"),
        "full_final_r": metrics.get("final_r"),
        "full_maxdd_r": metrics.get("maxdd_r"),
        "median_test_return_r": finalist.get("median_test_return_r"),
        "worst_test_return_r": finalist.get("worst_test_return_r"),
        "worst_test_drawdown_r": finalist.get("worst_test_drawdown_r"),
        "gate_reasons": finalist.get("gate_reasons") or [],
    }
