from __future__ import annotations

import json
import mimetypes
import os
import re
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .config import AppConfig
from .ledger import list_run_dirs, load_run_metadata
from .portfolio import (
    dashboard_attempt_score_sort_key,
    dashboard_run_attempt_sort_key,
    filter_dashboard_visible_candidate_rows,
    is_dashboard_canonical_attempt,
    select_dashboard_preferred_attempt_rows,
)


DASHBOARD_APP_ROOT = Path(__file__).resolve().parent / "dashboard"
DASHBOARD_DIST_ROOT = DASHBOARD_APP_ROOT / "dist"
SHORTLIST_REPORT_ROOTNAME = "shortlist-report"
_CURVE_CELL_CACHE: dict[str, tuple[tuple[str, bool, int | None, int | None], dict[str, Any]]] = {}
_RESULT_CELL_CACHE: dict[str, tuple[tuple[str, bool, int | None, int | None], dict[str, Any]]] = {}
_PROFILE_DROP_EXIT_POLICY_CACHE: dict[
    str, tuple[tuple[str, bool, int | None, int | None], dict[str, Any]]
] = {}
LIVE_PORTFOLIO_CACHE_FILENAME = "dashboard-live-portfolio.json"
DASHBOARD_JOB_ROOTNAME = "dashboard-jobs"
DASHBOARD_PORTFOLIO_CONFIG_ROOTNAME = "dashboard-portfolio-configs"
FULL_BACKTEST_CALENDAR_CURVE_FILENAME = "full-backtest-36mo-calendar-curve.json"
LOCAL_JOB_CLIENTS = {"127.0.0.1", "::1", "localhost"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_dashboard_portfolio_config() -> dict[str, Any]:
    return {
        "portfolio_name": "dashboard-auto-portfolio",
        "candidate_scope": "promoted",
        "catch_up_full_backtests": True,
        "catch_up_require_scrutiny_36": False,
        "catch_up_force_rebuild": False,
        "generate_profile_drops": True,
        "export_bundle": False,
        "profile_drop_workers": 4,
        "sleeves": [
            {"name": "core", "shortlist_size": 8},
            {"name": "breadth", "shortlist_size": 8},
        ],
    }


def _default_dashboard_manual_portfolio_config(
    selected_count: int,
    *,
    account: dict[str, Any] | None = None,
    portfolio_name: str | None = None,
) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    size = max(1, int(selected_count))
    return {
        "version": 1,
        "portfolio_name": portfolio_name or f"dashboard-manual-{timestamp}",
        "candidate_scope": "all",
        "dashboard_source": "manual_live_portfolio",
        "catch_up_full_backtests": True,
        "catch_up_force_rebuild": False,
        "catch_up_require_scrutiny_36": False,
        "full_backtest_job_timeout_seconds": 2400,
        "generate_profile_drops": True,
        "export_bundle": True,
        "profile_drop_lookback_months": 36,
        "profile_drop_timeout_seconds": 1800,
        "profile_drop_workers": 4,
        "chart_trades_x_max": 300.0,
        "account": dict(account or {}),
        "sleeves": [
            {
                "name": "manual",
                "prefilter_limit": size,
                "candidate_limit": -1,
                "shortlist_size": size,
                "min_score_36": 0.0,
                "min_retention_ratio": 0.0,
                "min_trades_per_month": 0.0,
                "max_drawdown_r": -1.0,
                "drawdown_penalty": 0.0,
                "trade_rate_bonus_weight": 0.0,
                "trade_rate_bonus_target": 8.0,
                "novelty_penalty": 0.0,
                "max_per_run": -1,
                "max_per_strategy_key": -1,
                "max_sameness_to_board": -1.0,
                "require_full_backtest_36": True,
                "scalar_metric_terms": [],
                "field_filters": [],
            }
        ],
    }


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_optional_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return _read_json(path)
    except Exception:
        return None


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, separators=(",", ":"))


def _tail_text(path: Path, *, max_chars: int = 12000) -> str:
    if not path.exists():
        return ""
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - max_chars))
            data = handle.read()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


class DashboardJobManager:
    def __init__(
        self,
        config: AppConfig,
        *,
        popen_factory: Any = subprocess.Popen,
    ):
        self.config = config
        self.job_root = config.derived_root / DASHBOARD_JOB_ROOTNAME
        self.config_root = config.derived_root / DASHBOARD_PORTFOLIO_CONFIG_ROOTNAME
        self._popen_factory = popen_factory
        self._lock = threading.RLock()
        self._records: dict[str, dict[str, Any]] = {}
        self._processes: dict[str, Any] = {}
        self.job_root.mkdir(parents=True, exist_ok=True)
        self.config_root.mkdir(parents=True, exist_ok=True)
        self._load_records()

    def _load_records(self) -> None:
        for path in sorted(self.job_root.glob("*.json")):
            record = _load_optional_json(path)
            if not isinstance(record, dict):
                continue
            job_id = str(record.get("id") or path.stem).strip()
            if not job_id:
                continue
            if record.get("status") == "running":
                record["status"] = "unknown"
                record["ended_at"] = record.get("ended_at") or _now_iso()
                record["error"] = "Dashboard restarted before this job status was observed."
                _write_json(path, record)
            self._records[job_id] = record

    def _record_path(self, job_id: str) -> Path:
        return self.job_root / f"{job_id}.json"

    def _write_record(self, record: dict[str, Any]) -> None:
        job_id = str(record.get("id") or "").strip()
        if not job_id:
            raise ValueError("Job record is missing an id.")
        _write_json(self._record_path(job_id), record)
        self._records[job_id] = dict(record)

    def _record_payload(self, record: dict[str, Any] | None) -> dict[str, Any] | None:
        if record is None:
            return None
        payload = dict(record)
        log_path = Path(str(payload.get("log_path") or ""))
        payload["log_tail"] = _tail_text(log_path)
        return payload

    def _active_record_unlocked(self) -> dict[str, Any] | None:
        for record in self._records.values():
            if str(record.get("status") or "") in {"running", "canceling"}:
                return record
        return None

    def _latest_record_unlocked(self) -> dict[str, Any] | None:
        if not self._records:
            return None
        return sorted(
            self._records.values(),
            key=lambda item: str(item.get("created_at") or ""),
            reverse=True,
        )[0]

    def current(self) -> dict[str, Any] | None:
        with self._lock:
            return self._record_payload(
                self._active_record_unlocked() or self._latest_record_unlocked()
            )

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._record_payload(self._records.get(str(job_id)))

    def latest_dashboard_portfolio_config(self) -> dict[str, Any]:
        path = self.config_root / "latest-dashboard-auto-portfolio.json"
        payload = _load_optional_json(path)
        if isinstance(payload, dict):
            return payload
        return _default_dashboard_portfolio_config()

    def _write_dashboard_portfolio_config(
        self,
        payload: dict[str, Any],
        *,
        label: str = "auto",
    ) -> Path:
        config_payload = dict(payload or {})
        if not config_payload:
            config_payload = _default_dashboard_portfolio_config()
        if not isinstance(config_payload.get("sleeves"), list):
            config_payload["sleeves"] = _default_dashboard_portfolio_config()["sleeves"]
        label_token = re.sub(r"[^a-z0-9_-]+", "-", str(label or "auto").strip().lower()).strip("-")
        if not label_token:
            label_token = "auto"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        path = self.config_root / f"{timestamp}-dashboard-{label_token}-portfolio.json"
        latest_path = self.config_root / f"latest-dashboard-{label_token}-portfolio.json"
        _write_json(path, config_payload)
        _write_json(latest_path, config_payload)
        return path

    def _finalize_command(self, payload: dict[str, Any]) -> list[str]:
        command = ["uv", "run", "finalize-corpus", "--json"]
        for run_id in _string_list(payload.get("run_ids") or payload.get("run_id")):
            token = str(run_id).strip()
            if token:
                command.extend(["--run-id", token])
        for attempt_id in _string_list(payload.get("attempt_ids") or payload.get("attempt_id")):
            token = str(attempt_id).strip()
            if token:
                command.extend(["--attempt-id", token])
        scope = str(payload.get("scope") or "dashboard").strip()
        if scope in {"dashboard", "all"}:
            command.extend(["--scope", scope])
        for key, option in [
            ("lookback_months", "--lookback-months"),
            ("profile_drop_workers", "--profile-drop-workers"),
            ("profile_drop_timeout_seconds", "--profile-drop-timeout-seconds"),
        ]:
            if payload.get(key) is not None:
                command.extend([option, str(payload[key])])
        if payload.get("force_rebuild"):
            command.append("--force-rebuild")
        if payload.get("allow_presentation_fallback"):
            command.append("--allow-presentation-fallback")
        if payload.get("dry_run"):
            command.append("--dry-run")
        return command

    def _build_portfolio_command(self, payload: dict[str, Any]) -> tuple[list[str], Path]:
        raw_config = payload.get("portfolio_config")
        config_payload = raw_config if isinstance(raw_config, dict) else self.latest_dashboard_portfolio_config()
        config_path = self._write_dashboard_portfolio_config(
            config_payload,
            label=str(payload.get("portfolio_config_label") or "auto"),
        )
        command = [
            "uv",
            "run",
            "build-portfolio",
            "--portfolio-config",
            str(config_path),
            "--json",
        ]
        for run_id in _string_list(payload.get("run_ids") or payload.get("run_id")):
            token = str(run_id).strip()
            if token:
                command.extend(["--run-id", token])
        for attempt_id in _string_list(payload.get("attempt_ids") or payload.get("attempt_id")):
            token = str(attempt_id).strip()
            if token:
                command.extend(["--attempt-id", token])
        if payload.get("candidate_scope") in {"promoted", "all"}:
            command.extend(["--candidate-scope", str(payload["candidate_scope"])])
        for key, true_option, false_option in [
            ("catch_up_full_backtests", "--catch-up-full-backtests", "--no-catch-up-full-backtests"),
            ("catch_up_force_rebuild", "--catch-up-force-rebuild", "--no-catch-up-force-rebuild"),
            ("catch_up_require_scrutiny_36", "--catch-up-require-scrutiny-36", "--no-catch-up-require-scrutiny-36"),
            ("generate_profile_drops", "--generate-profile-drops", "--no-generate-profile-drops"),
            ("export_bundle", "--export-bundle", "--no-export-bundle"),
        ]:
            if payload.get(key) is not None:
                command.append(true_option if payload.get(key) else false_option)
        if payload.get("profile_drop_workers") is not None:
            command.extend(["--profile-drop-workers", str(payload["profile_drop_workers"])])
        return command, config_path

    def _job_env(self) -> dict[str, str]:
        env = os.environ.copy()
        codex_home = env.get("AUTORESEARCH_CODEX_HOME")
        if not codex_home:
            codex_home = str(self.config.repo_root / ".codex-harness" / "codex-home")
            env["AUTORESEARCH_CODEX_HOME"] = codex_home
        env.pop("CODEX_HOME", None)
        return env

    def start(self, kind: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        kind = str(kind or "").strip()
        with self._lock:
            active = self._active_record_unlocked()
            if active is not None:
                raise RuntimeError(f"Dashboard job already running: {active.get('id')}")
        if kind == "finalize-corpus":
            command = self._finalize_command(payload)
            config_path = None
        elif kind == "build-portfolio":
            command, config_path = self._build_portfolio_command(payload)
        else:
            raise ValueError(f"Unsupported dashboard job kind: {kind}")

        with self._lock:
            active = self._active_record_unlocked()
            if active is not None:
                raise RuntimeError(f"Dashboard job already running: {active.get('id')}")
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            job_id = f"{timestamp}-{kind.replace('-', '_')}-{uuid.uuid4().hex[:8]}"
            log_path = self.job_root / f"{job_id}.log"
            record = {
                "id": job_id,
                "kind": kind,
                "status": "running",
                "created_at": _now_iso(),
                "started_at": _now_iso(),
                "ended_at": None,
                "command": command,
                "cwd": str(self.config.repo_root),
                "returncode": None,
                "log_path": str(log_path),
                "portfolio_config_path": str(config_path) if config_path is not None else None,
            }
            log_handle = log_path.open("w", encoding="utf-8")
            log_handle.write("$ " + " ".join(command) + "\n\n")
            log_handle.flush()
            try:
                process = self._popen_factory(
                    command,
                    cwd=str(self.config.repo_root),
                    env=self._job_env(),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                )
            except Exception:
                log_handle.close()
                raise
            self._processes[job_id] = process
            self._write_record(record)
            thread = threading.Thread(
                target=self._monitor_job,
                args=(job_id, process, log_handle),
                daemon=True,
            )
            thread.start()
            return self._record_payload(record) or record

    def _monitor_job(self, job_id: str, process: Any, log_handle: Any) -> None:
        error: str | None = None
        try:
            returncode = int(process.wait())
        except Exception as exc:
            returncode = -1
            error = str(exc)
        finally:
            try:
                log_handle.close()
            except Exception:
                pass
        with self._lock:
            record = dict(self._records.get(job_id) or {})
            prior_status = str(record.get("status") or "")
            record["returncode"] = returncode
            record["ended_at"] = _now_iso()
            if prior_status == "canceling":
                record["status"] = "canceled"
            elif returncode == 0:
                record["status"] = "completed"
            else:
                record["status"] = "failed"
            if error:
                record["error"] = error
            self._processes.pop(job_id, None)
            self._write_record(record)

    def cancel(self, job_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            record = self._records.get(str(job_id)) if job_id else self._active_record_unlocked()
            if record is None:
                return None
            job_id = str(record.get("id") or "")
            process = self._processes.get(job_id)
            if process is None or str(record.get("status") or "") not in {"running", "canceling"}:
                return self._record_payload(record)
            record = dict(record)
            record["status"] = "canceling"
            record["cancel_requested_at"] = _now_iso()
            self._write_record(record)
            try:
                process.terminate()
            except Exception as exc:
                record["error"] = str(exc)
                self._write_record(record)
            return self._record_payload(record)


def _latest_portfolio_report_path(config: AppConfig) -> Path | None:
    root = config.derived_root / "portfolio-report"
    if not root.exists():
        return None
    candidates = sorted(
        root.glob("*/portfolio-report.json"),
        key=lambda path: path.stat().st_mtime_ns if path.exists() else 0,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _file_signature(path: Path) -> tuple[str, bool, int | None, int | None]:
    try:
        stat = path.stat()
        return (str(path), True, stat.st_mtime_ns, stat.st_size)
    except FileNotFoundError:
        return (str(path), False, None, None)


def _dashboard_frontend_signature() -> tuple[tuple[str, bool, int | None, int | None], ...]:
    tracked = [
        DASHBOARD_APP_ROOT / "src" / "App.tsx",
        DASHBOARD_APP_ROOT / "src" / "main.tsx",
        DASHBOARD_APP_ROOT / "src" / "index.css",
        DASHBOARD_APP_ROOT / "src" / "lib" / "types.ts",
        DASHBOARD_APP_ROOT / "src" / "components" / "attempt-table.tsx",
        DASHBOARD_APP_ROOT / "src" / "pages" / "CatalogPage.tsx",
        DASHBOARD_APP_ROOT / "src" / "pages" / "PortfolioWorkbenchPage.tsx",
        DASHBOARD_APP_ROOT / "src" / "pages" / "RunDetailPage.tsx",
        DASHBOARD_APP_ROOT / "src" / "pages" / "RunsPage.tsx",
        DASHBOARD_APP_ROOT / "package.json",
        DASHBOARD_DIST_ROOT / "index.html",
    ]
    return tuple(_file_signature(path) for path in tracked)


def _ensure_dashboard_dist() -> None:
    index_path = DASHBOARD_DIST_ROOT / "index.html"
    if index_path.exists():
        source_paths = [
            DASHBOARD_APP_ROOT / "src" / "App.tsx",
            DASHBOARD_APP_ROOT / "src" / "main.tsx",
            DASHBOARD_APP_ROOT / "src" / "index.css",
            DASHBOARD_APP_ROOT / "src" / "lib" / "types.ts",
            DASHBOARD_APP_ROOT / "src" / "components" / "attempt-table.tsx",
            DASHBOARD_APP_ROOT / "src" / "pages" / "CatalogPage.tsx",
            DASHBOARD_APP_ROOT / "src" / "pages" / "PortfolioWorkbenchPage.tsx",
            DASHBOARD_APP_ROOT / "src" / "pages" / "RunDetailPage.tsx",
            DASHBOARD_APP_ROOT / "src" / "pages" / "RunsPage.tsx",
            DASHBOARD_APP_ROOT / "package.json",
        ]
        dist_mtime = index_path.stat().st_mtime_ns
        if all((not path.exists()) or path.stat().st_mtime_ns <= dist_mtime for path in source_paths):
            return
    if index_path.exists():
        print("Autoresearch dashboard source changed; rebuilding frontend bundle...", flush=True)
    else:
        print("Autoresearch dashboard bundle missing; building frontend bundle...", flush=True)
    subprocess.run(
        ["npm", "run", "build"],
        cwd=str(DASHBOARD_APP_ROOT),
        check=True,
        timeout=600,
    )


def _repo_relative(config: AppConfig, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(config.repo_root.resolve())).replace(
            "\\", "/"
        )
    except Exception:
        return None


def _file_url(config: AppConfig, path: Path | None) -> str | None:
    relative = _repo_relative(config, path)
    if not relative:
        return None
    return f"/files?path={relative}"


def _serve_repo_file(repo_root: Path, raw_relative: str) -> tuple[bytes, str] | None:
    candidate = (repo_root / raw_relative).resolve()
    if repo_root.resolve() not in candidate.parents and candidate != repo_root.resolve():
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    mime_type, _ = mimetypes.guess_type(str(candidate))
    return candidate.read_bytes(), mime_type or "application/octet-stream"


def _serve_dist_file(raw_path: str) -> tuple[bytes, str] | None:
    relative = raw_path.lstrip("/") or "index.html"
    candidate = (DASHBOARD_DIST_ROOT / relative).resolve()
    if DASHBOARD_DIST_ROOT.resolve() not in candidate.parents and candidate != DASHBOARD_DIST_ROOT.resolve():
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    mime_type, _ = mimetypes.guess_type(str(candidate))
    return candidate.read_bytes(), mime_type or "application/octet-stream"


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_curve_cell_prefix(path: Path) -> dict[str, Any]:
    signature = _file_signature(path)
    cache_key = str(path)
    cached = _CURVE_CELL_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return dict(cached[1])
    if not signature[1]:
        _CURVE_CELL_CACHE[cache_key] = (signature, {})
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            prefix = handle.read(16384)
    except Exception:
        _CURVE_CELL_CACHE[cache_key] = (signature, {})
        return {}
    marker_index = prefix.find('"cell"')
    if marker_index < 0:
        _CURVE_CELL_CACHE[cache_key] = (signature, {})
        return {}
    colon_index = prefix.find(":", marker_index)
    if colon_index < 0:
        _CURVE_CELL_CACHE[cache_key] = (signature, {})
        return {}
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(prefix[colon_index + 1 :].lstrip())
    except json.JSONDecodeError:
        value = {}
    cell = value if isinstance(value, dict) else {}
    _CURVE_CELL_CACHE[cache_key] = (signature, dict(cell))
    return dict(cell)


def _load_result_recommended_cell(path: Path) -> dict[str, Any]:
    signature = _file_signature(path)
    cache_key = str(path)
    cached = _RESULT_CELL_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return dict(cached[1])
    if not signature[1]:
        _RESULT_CELL_CACHE[cache_key] = (signature, {})
        return {}
    payload = _load_optional_json(path)
    if not isinstance(payload, dict):
        _RESULT_CELL_CACHE[cache_key] = (signature, {})
        return {}
    aggregate = payload.get("data")
    if isinstance(aggregate, dict) and isinstance(aggregate.get("aggregate"), dict):
        aggregate = aggregate.get("aggregate")
    if not isinstance(aggregate, dict):
        aggregate = payload

    cell = aggregate.get("recommended_cell")
    if not isinstance(cell, dict):
        matrix_summary = aggregate.get("matrix_summary")
        if isinstance(matrix_summary, dict):
            cell = matrix_summary.get("robust_cell")
    if not isinstance(cell, dict):
        cell = {}
    _RESULT_CELL_CACHE[cache_key] = (signature, dict(cell))
    return dict(cell)


def _normalize_exit_policy_cell(cell: dict[str, Any], *, basis: str | None = None) -> dict[str, Any]:
    reward_multiple = _safe_float(cell.get("reward_multiple", cell.get("rewardMultiple")))
    stop_loss_percent = _safe_float(cell.get("stop_loss_percent", cell.get("stopLossPercent")))
    take_profit_percent = _safe_float(
        cell.get("take_profit_percent", cell.get("takeProfitPercent"))
    )
    normalized: dict[str, Any] = {}
    if reward_multiple is not None:
        normalized["reward_multiple"] = reward_multiple
    if stop_loss_percent is not None:
        normalized["stop_loss_percent"] = stop_loss_percent
    if take_profit_percent is not None:
        normalized["take_profit_percent"] = take_profit_percent
    if basis:
        normalized["_basis"] = basis
    return normalized


def _load_profile_document_exit_policy_cell(path: Path) -> dict[str, Any]:
    signature = _file_signature(path)
    cache_key = str(path)
    cached = _PROFILE_DROP_EXIT_POLICY_CACHE.get(cache_key)
    if cached and cached[0] == signature:
        return dict(cached[1])
    if not signature[1]:
        _PROFILE_DROP_EXIT_POLICY_CACHE[cache_key] = (signature, {})
        return {}
    document = _load_optional_json(path)
    if not isinstance(document, dict):
        _PROFILE_DROP_EXIT_POLICY_CACHE[cache_key] = (signature, {})
        return {}
    profile_document = document.get("profile") if isinstance(document.get("profile"), dict) else document
    execution_config = profile_document.get("executionConfig")
    execution_config = execution_config if isinstance(execution_config, dict) else {}
    exit_policy = execution_config.get("exitPolicy")
    exit_policy = exit_policy if isinstance(exit_policy, dict) else {}
    recommendation = exit_policy.get("recommendation")
    recommendation = recommendation if isinstance(recommendation, dict) else {}
    recommended_cell = recommendation.get("cell")
    basis = str(recommendation.get("basis") or "").strip() or None
    if isinstance(recommended_cell, dict):
        normalized = _normalize_exit_policy_cell(recommended_cell, basis=basis)
        if normalized:
            _PROFILE_DROP_EXIT_POLICY_CACHE[cache_key] = (signature, dict(normalized))
            return dict(normalized)

    selected_cell = exit_policy.get("selectedCell")
    if isinstance(selected_cell, dict):
        normalized = _normalize_exit_policy_cell(
            selected_cell,
            basis=basis or "profile_drop_exit_policy",
        )
        _PROFILE_DROP_EXIT_POLICY_CACHE[cache_key] = (signature, dict(normalized))
        return dict(normalized)

    _PROFILE_DROP_EXIT_POLICY_CACHE[cache_key] = (signature, {})
    return {}


def _profile_drop_document_candidates(root: Path) -> list[Path]:
    package_root = root / ".profile-drop-36mo"
    if not package_root.exists():
        return []
    candidates = [
        path
        for path in package_root.glob("bundle/*/profile-document.json")
        if path.exists() and path.is_file()
    ]
    candidates.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)
    source_document = package_root / "profile-drop-36mo.source-profile-document.json"
    if source_document.exists() and source_document.is_file():
        candidates.append(source_document)
    return candidates


def _load_profile_drop_exit_policy_cell(*roots: Path) -> dict[str, Any]:
    seen: set[Path] = set()
    for root in roots:
        for candidate in _profile_drop_document_candidates(root):
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            cell = _load_profile_document_exit_policy_cell(candidate)
            if cell:
                return dict(cell)
    return {}


def _normalize_chart_entry(config: AppConfig, raw_path: str | None) -> dict[str, Any] | None:
    if not raw_path:
        return None
    path = Path(str(raw_path))
    return {
        "path": str(path),
        "url": _file_url(config, path) if path.exists() else None,
        "exists": path.exists(),
    }


def _normalize_path_fields(config: AppConfig, row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for key in [
        "artifact_dir",
        "profile_path",
        "scrutiny_result_path_12m",
        "scrutiny_curve_path_12m",
        "scrutiny_result_path_36m",
        "scrutiny_curve_path_36m",
        "full_backtest_result_path_36m",
        "full_backtest_curve_path_36m",
        "full_backtest_calendar_curve_path_36m",
    ]:
        value = normalized.get(key)
        if isinstance(value, str) and value.strip():
            normalized[f"{key}_url"] = _file_url(config, Path(value))
    artifact_dir = normalized.get("artifact_dir")
    if isinstance(artifact_dir, str) and artifact_dir.strip():
        artifact_path = Path(artifact_dir)
        profile_drop_png = artifact_path / "profile-drop-36mo.png"
        profile_drop_manifest = artifact_path / "profile-drop-36mo.manifest.json"
        if not profile_drop_png.exists():
            run_id = str(normalized.get("run_id") or "").strip()
            run_drop_png = config.runs_root / run_id / "profile-drop-36mo.png"
            run_drop_manifest = config.runs_root / run_id / "profile-drop-36mo.manifest.json"
            if run_id and run_drop_png.exists():
                profile_drop_png = run_drop_png
                profile_drop_manifest = run_drop_manifest
        normalized["profile_drop_36m_png_path"] = str(profile_drop_png)
        normalized["profile_drop_36m_png_url"] = (
            _file_url(config, profile_drop_png) if profile_drop_png.exists() else None
        )
        normalized["profile_drop_36m_manifest_path"] = str(profile_drop_manifest)
        normalized["profile_drop_36m_manifest_url"] = (
            _file_url(config, profile_drop_manifest)
            if profile_drop_manifest.exists()
            else None
        )
    curve_path_value = normalized.get("full_backtest_curve_path_36m")
    setup_cell: dict[str, Any] = {}
    if isinstance(curve_path_value, str) and curve_path_value.strip():
        curve_cell = _load_curve_cell_prefix(Path(curve_path_value))
        setup_cell.update(curve_cell)
        if curve_cell:
            normalized["reward_multiple_basis_36m"] = "curve_cell"
    result_path_value = normalized.get("full_backtest_result_path_36m")
    if isinstance(result_path_value, str) and result_path_value.strip():
        recommended_cell = _load_result_recommended_cell(Path(result_path_value))
        if recommended_cell:
            setup_cell.update(recommended_cell)
            normalized["reward_multiple_basis_36m"] = "recommended_cell"
    profile_drop_roots: list[Path] = []
    if isinstance(artifact_dir, str) and artifact_dir.strip():
        profile_drop_roots.append(Path(artifact_dir))
    run_id = str(normalized.get("run_id") or "").strip()
    if run_id:
        profile_drop_roots.append(config.runs_root / run_id)
    profile_drop_cell = _load_profile_drop_exit_policy_cell(*profile_drop_roots)
    if profile_drop_cell:
        basis = str(profile_drop_cell.pop("_basis", "") or "").strip()
        setup_cell.update(profile_drop_cell)
        normalized["reward_multiple_basis_36m"] = basis or "profile_drop_exit_policy"
    reward_multiple = _safe_float(setup_cell.get("reward_multiple"))
    stop_loss_percent = _safe_float(setup_cell.get("stop_loss_percent"))
    take_profit_percent = _safe_float(setup_cell.get("take_profit_percent"))
    if reward_multiple is not None:
        normalized["reward_multiple_36m"] = reward_multiple
    if stop_loss_percent is not None:
        normalized["selected_stop_loss_percent_36m"] = stop_loss_percent
    if take_profit_percent is not None:
        normalized["selected_take_profit_percent_36m"] = take_profit_percent
    return normalized


def _is_canonical_playhand_attempt(row: dict[str, Any]) -> bool:
    return is_dashboard_canonical_attempt(row)


def _preferred_run_attempt(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    selected, _info = select_dashboard_preferred_attempt_rows(rows)
    return selected[0] if selected else None


def _run_attempt_sort_key(row: dict[str, Any]) -> tuple[bool, tuple[bool, float, float, str]]:
    return dashboard_run_attempt_sort_key(row)


def _build_run_summaries(config: AppConfig, catalog_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in catalog_rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        grouped.setdefault(run_id, []).append(row)

    metadata_by_run_id = {
        run_dir.name: load_run_metadata(run_dir) for run_dir in list_run_dirs(config.runs_root)
    }

    run_summaries: list[dict[str, Any]] = []
    for run_id, rows in grouped.items():
        metadata = metadata_by_run_id.get(run_id) or {}
        best_row = _preferred_run_attempt(rows)
        created_at_values = [str(row.get("created_at") or "") for row in rows if row.get("created_at")]
        latest_created_at = max(created_at_values) if created_at_values else None
        progress_png = config.runs_root / run_id / "progress.png"
        run_summaries.append(
            {
                "run_id": run_id,
                "created_at": metadata.get("created_at"),
                "latest_created_at": latest_created_at,
                "explorer_model": metadata.get("explorer_model"),
                "explorer_profile": metadata.get("explorer_profile"),
                "supervisor_model": metadata.get("supervisor_model"),
                "supervisor_profile": metadata.get("supervisor_profile"),
                "quality_score_preset": metadata.get("quality_score_preset"),
                "attempt_count": len(rows),
                "scored_attempt_count": sum(
                    1 for row in rows if _safe_float(row.get("composite_score")) is not None
                ),
                "full_backtest_36m_count": sum(
                    1 for row in rows if bool(row.get("has_full_backtest_36m"))
                ),
                "score_36m_count": sum(
                    1 for row in rows if _safe_float(row.get("score_36m")) is not None
                ),
                "best_attempt": _normalize_path_fields(config, best_row) if best_row else None,
                "canonical_attempt_id": metadata.get("canonical_attempt_id"),
                "canonical_candidate_name": metadata.get("canonical_candidate_name"),
                "progress_png_url": _file_url(config, progress_png) if progress_png.exists() else None,
            }
        )
    run_summaries.sort(
        key=lambda row: (
            str(row.get("created_at") or row.get("latest_created_at") or ""),
            str(row.get("run_id") or ""),
        ),
        reverse=True,
    )
    return run_summaries


def _cadence_band_rows(rows: list[dict[str, Any]], *, min_score: float | None = None) -> list[dict[str, Any]]:
    bands = [
        ("0-1", 0.0, 1.0),
        ("1-2", 1.0, 2.0),
        ("2-5", 2.0, 5.0),
        ("5-10", 5.0, 10.0),
        ("10-20", 10.0, 20.0),
        ("20+", 20.0, None),
    ]
    scored_rows = []
    for row in rows:
        score = _safe_float(row.get("score_36m"))
        tpm = _safe_float(row.get("trades_per_month_36m"))
        if score is None or tpm is None:
            continue
        if min_score is not None and score < min_score:
            continue
        scored_rows.append((row, score, tpm))

    payload: list[dict[str, Any]] = []
    for label, lo, hi in bands:
        bucket = [
            (row, score, tpm)
            for row, score, tpm in scored_rows
            if tpm >= lo and (hi is None or tpm < hi)
        ]
        if not bucket:
            payload.append(
                {
                    "band": label,
                    "count": 0,
                    "mean_score_36m": None,
                    "max_score_36m": None,
                    "mean_drawdown_r_36m": None,
                }
            )
            continue
        drawdowns = [
            value
            for value in (_safe_float(row.get("max_drawdown_r_36m")) for row, *_ in bucket)
            if value is not None
        ]
        payload.append(
            {
                "band": label,
                "count": len(bucket),
                "mean_score_36m": round(mean(score for _, score, _ in bucket), 4),
                "max_score_36m": round(max(score for _, score, _ in bucket), 4),
                "mean_drawdown_r_36m": round(mean(drawdowns), 4) if drawdowns else None,
            }
        )
    return payload


def _normalize_shortlist_payload(config: AppConfig, payload: dict[str, Any] | None) -> dict[str, Any]:
    report = dict(payload or {})
    report_root = config.derived_root / SHORTLIST_REPORT_ROOTNAME
    filters = dict(report.get("filters") or {})
    scope = dict(report.get("scope") or {})
    is_filtered = bool(filters.get("run_ids") or filters.get("attempt_ids"))
    report["scope"] = {
        "is_canonical": bool(scope.get("is_canonical", True)),
        "is_filtered": bool(scope.get("is_filtered", is_filtered)),
        "report_root": str(scope.get("report_root") or report_root),
    }
    if report["scope"]["is_filtered"]:
        report["warning"] = (
            "The current shortlist artifact was built from a filtered run/attempt slice, "
            "not the full corpus."
        )
    charts = dict(report.get("charts") or {})
    overlay_png = report_root / "charts" / "shortlist-overlay-score-vs-trades-36mo.png"
    overlay_json = report_root / "charts" / "shortlist-overlay-score-vs-trades-36mo.json"
    if overlay_png.exists():
        charts["shortlist_overlay_score_vs_trades"] = str(overlay_png)
    normalized_charts = {
        key: _normalize_chart_entry(config, str(value))
        for key, value in charts.items()
        if value
    }
    if overlay_json.exists():
        normalized_charts["shortlist_overlay_score_vs_trades_json"] = _normalize_chart_entry(
            config, str(overlay_json)
        )
    report["charts"] = normalized_charts
    profile_drops = []
    for item in list(report.get("profile_drops") or []):
        normalized = dict(item)
        png_path = Path(str(item.get("png_path") or "")) if item.get("png_path") else None
        manifest_path = (
            Path(str(item.get("manifest_path") or "")) if item.get("manifest_path") else None
        )
        normalized["png_url"] = _file_url(config, png_path) if png_path and png_path.exists() else None
        normalized["manifest_url"] = (
            _file_url(config, manifest_path) if manifest_path and manifest_path.exists() else None
        )
        profile_drops.append(normalized)
    report["profile_drops"] = profile_drops
    missing_drop_count = sum(
        1
        for item in profile_drops
        if str(item.get("status") or "") in {"rendered", "cached"}
        and not item.get("png_url")
    )
    if missing_drop_count > 0:
        report["warning"] = (
            f"{missing_drop_count} rendered profile-drop records are missing their PNG or manifest on disk. "
            "This usually means a prior shortlist build was interrupted after writing some assets but before fully refreshing the report."
        )
    report["selected"] = [
        _normalize_path_fields(config, row) for row in list(report.get("selected") or [])
    ]
    report["alternates"] = [
        _normalize_path_fields(config, row)
        for row in list(report.get("alternates") or [])
    ]
    return report


def _merge_attempt_unions(
    rows: list[dict[str, Any]] | None,
    *,
    label_field: str,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in list(rows or []):
        attempt_id = str(row.get("attempt_id") or "").strip()
        if not attempt_id:
            continue
        if attempt_id not in merged:
            merged[attempt_id] = dict(row)
            continue
        prior = merged[attempt_id]
        labels = list(prior.get(label_field) or [])
        for label in list(row.get(label_field) or []):
            if label not in labels:
                labels.append(label)
        if labels:
            prior[label_field] = labels
            prior[f"{label_field}_count"] = len(labels)
    merged_rows = list(merged.values())
    merged_rows.sort(key=dashboard_attempt_score_sort_key)
    return merged_rows


def _normalize_portfolio_as_shortlist(
    config: AppConfig, payload: dict[str, Any] | None, report_path: Path
) -> dict[str, Any]:
    report = dict(payload or {})
    portfolio_spec = dict(report.get("portfolio_spec") or {})
    selected_rows = list(report.get("selected") or [])
    alternates = _merge_attempt_unions(
        [
            dict(item)
            for sleeve in list(report.get("sleeves") or [])
            for item in list((sleeve or {}).get("alternates") or [])
        ],
        label_field="selected_by_sleeves",
    )
    charts = dict(report.get("charts") or {})
    mapped_charts: dict[str, str] = {}
    chart_key_map = {
        "portfolio_candidate_score_vs_drawdown": "corpus_score_vs_drawdown",
        "portfolio_candidate_score_vs_sameness": "corpus_score_vs_sameness",
        "portfolio_score_vs_trades": "shortlist_score_vs_trades",
        "portfolio_score_vs_drawdown": "shortlist_score_vs_drawdown",
        "portfolio_score_vs_sameness": "shortlist_score_vs_sameness",
        "portfolio_similarity_heatmap": "shortlist_similarity_heatmap",
        "portfolio_overlay_score_vs_trades": "shortlist_overlay_score_vs_trades",
    }
    for source_key, target_key in chart_key_map.items():
        if charts.get(source_key):
            mapped_charts[target_key] = str(charts[source_key])
    normalized = {
        "generated_at": report.get("generated_at"),
        "source_type": "portfolio",
        "source_label": "Portfolio",
        "portfolio_name": report.get("portfolio_name"),
        "filters": {
            "sleeve_count": len(list(report.get("sleeves") or [])),
            "selected_overlap_count": int(report.get("selected_overlap_count") or 0),
            "profile_drop_workers": portfolio_spec.get("profile_drop_workers"),
            "chart_trades_x_max": portfolio_spec.get("chart_trades_x_max"),
        },
        "candidate_count": int(report.get("candidate_union_count") or 0),
        "selected_count": int(report.get("selected_union_count") or len(selected_rows)),
        "alternate_count": len(alternates),
        "selected_basket_summary": report.get("selected_basket_summary") or {},
        "selected_basket_curve_36m": report.get("selected_basket_curve_36m") or {},
        "selected": selected_rows,
        "alternates": alternates,
        "profile_drops": list(report.get("profile_drops") or []),
        "charts": mapped_charts,
        "scope": {
            "is_canonical": True,
            "is_filtered": bool(report.get("run_ids") or report.get("attempt_ids")),
            "report_root": str(report_path.parent),
        },
        "warning": None,
        "selected_trade_rate_summary": report.get("selected_trade_rate_summary") or {},
        "candidate_trade_rate_summary": report.get("candidate_trade_rate_summary") or {},
        "selected_overlap_count": int(report.get("selected_overlap_count") or 0),
        "sleeves": list(report.get("sleeves") or []),
    }
    normalized["selected"] = [
        _normalize_path_fields(config, row) for row in list(normalized.get("selected") or [])
    ]
    normalized["alternates"] = [
        _normalize_path_fields(config, row) for row in list(normalized.get("alternates") or [])
    ]
    normalized["profile_drops"] = [
        {
            **dict(item),
            "png_url": (
                _file_url(config, Path(str(item.get("png_path"))))
                if item.get("png_path") and Path(str(item.get("png_path"))).exists()
                else None
            ),
            "manifest_url": (
                _file_url(config, Path(str(item.get("manifest_path"))))
                if item.get("manifest_path") and Path(str(item.get("manifest_path"))).exists()
                else None
            ),
        }
        for item in list(normalized.get("profile_drops") or [])
    ]
    missing_drop_count = sum(
        1
        for item in list(normalized.get("profile_drops") or [])
        if str(item.get("status") or "") in {"rendered", "cached"}
        and not item.get("png_url")
    )
    if missing_drop_count > 0:
        normalized["warning"] = (
            f"{missing_drop_count} rendered portfolio profile-drop records are missing their PNG or manifest on disk. "
            "This usually means a later portfolio build updated drop folders without finishing the final report refresh."
        )
    normalized["charts"] = {
        key: _normalize_chart_entry(config, str(value))
        for key, value in mapped_charts.items()
        if value
    }
    return normalized


def _normalize_promotion_payload(config: AppConfig, payload: dict[str, Any] | None) -> dict[str, Any]:
    report = dict(payload or {})
    report["selected"] = [
        _normalize_path_fields(config, row) for row in list(report.get("selected") or [])
    ]
    report["alternates"] = [
        _normalize_path_fields(config, row)
        for row in list(report.get("alternates") or [])
    ]
    return report


def _build_viewer_payload(config: AppConfig, catalog_rows: list[dict[str, Any]]) -> dict[str, Any]:
    visible_catalog_rows, visibility_info = filter_dashboard_visible_candidate_rows(
        catalog_rows
    )
    summary = _load_optional_json(config.attempt_catalog_summary_path) or {}
    summary = dict(summary) if isinstance(summary, dict) else {}
    summary["tombstoned_run_count"] = visibility_info["tombstoned_run_count"]
    summary["tombstoned_attempt_count"] = visibility_info["tombstoned_dropped_count"]
    summary["incomplete_playhand_run_count"] = visibility_info[
        "incomplete_playhand_run_count"
    ]
    summary["incomplete_playhand_attempt_count"] = visibility_info[
        "incomplete_playhand_dropped_count"
    ]
    audit = _load_optional_json(config.full_backtest_audit_json_path) or {}
    latest_portfolio_path = _latest_portfolio_report_path(config)
    if latest_portfolio_path:
        shortlist = _normalize_portfolio_as_shortlist(
            config,
            _load_optional_json(latest_portfolio_path),
            latest_portfolio_path,
        )
    else:
        shortlist = _normalize_shortlist_payload(
            config,
            _load_optional_json(config.derived_root / SHORTLIST_REPORT_ROOTNAME / "shortlist-report.json"),
        )
    promotion = _normalize_promotion_payload(
        config,
        _load_optional_json(config.promotion_board_json_path),
    )
    runs = _build_run_summaries(config, visible_catalog_rows)
    charts = {
        "corpus_score_vs_trades": _normalize_chart_entry(config, str(config.corpus_tradeoff_plot_path)),
        "corpus_score_vs_trades_json": _normalize_chart_entry(
            config, str(config.corpus_tradeoff_json_path)
        ),
        "promotion_board_csv": _normalize_chart_entry(config, str(config.promotion_board_csv_path)),
        "attempt_catalog_csv": _normalize_chart_entry(config, str(config.attempt_catalog_csv_path)),
    }
    shortlist_charts = shortlist.get("charts") or {}
    for key in [
        "corpus_score_vs_drawdown",
        "corpus_score_vs_sameness",
        "shortlist_score_vs_trades",
        "shortlist_score_vs_drawdown",
        "shortlist_score_vs_sameness",
        "shortlist_similarity_heatmap",
        "shortlist_overlay_score_vs_trades",
    ]:
        if key in shortlist_charts:
            charts[key] = shortlist_charts[key]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "corpus_summary": summary,
        "audit": audit,
        "shortlist": shortlist,
        "promotion": promotion,
        "runs": runs,
        "charts": charts,
        "cadence_bands_all_scored": _cadence_band_rows(visible_catalog_rows),
        "cadence_bands_score_ge_40": _cadence_band_rows(visible_catalog_rows, min_score=40.0),
    }


class ViewerState:
    def __init__(self, config: AppConfig):
        self.config = config
        self.job_manager = DashboardJobManager(config)
        self._lock = threading.RLock()
        self._snapshot: dict[str, Any] | None = None
        self._snapshot_signature: tuple[Any, ...] | None = None
        self._catalog_rows: list[dict[str, Any]] | None = None
        self._catalog_signature: tuple[str, bool, int | None, int | None] | None = None

    def _load_catalog_rows(self) -> list[dict[str, Any]]:
        rows = _load_optional_json(self.config.attempt_catalog_json_path) or []
        if not isinstance(rows, list):
            return []
        return [dict(row) for row in rows if isinstance(row, dict)]

    def catalog_rows(self) -> list[dict[str, Any]]:
        signature = _file_signature(self.config.attempt_catalog_json_path)
        with self._lock:
            if self._catalog_rows is None or self._catalog_signature != signature:
                self._catalog_rows = self._load_catalog_rows()
                self._catalog_signature = signature
            return list(self._catalog_rows)

    def snapshot(self) -> dict[str, Any]:
        latest_portfolio_path = _latest_portfolio_report_path(self.config)
        signature = (
            _file_signature(self.config.attempt_catalog_summary_path),
            _file_signature(self.config.full_backtest_audit_json_path),
            _file_signature(self.config.promotion_board_json_path),
            _file_signature(self.config.attempt_catalog_json_path),
            _file_signature(
                self.config.derived_root / SHORTLIST_REPORT_ROOTNAME / "shortlist-report.json"
            ),
            _file_signature(latest_portfolio_path) if latest_portfolio_path else ("", False, None, None),
            _file_signature(self.config.corpus_tradeoff_plot_path),
        )
        with self._lock:
            if self._snapshot is None or self._snapshot_signature != signature:
                self._snapshot = _build_viewer_payload(self.config, self.catalog_rows())
                self._snapshot_signature = signature
            return dict(self._snapshot)


def _run_detail_payload(
    config: AppConfig, catalog_rows: list[dict[str, Any]], run_id: str
) -> dict[str, Any] | None:
    attempts = [
        _normalize_path_fields(config, row)
        for row in catalog_rows
        if str(row.get("run_id") or "") == run_id
    ]
    if not attempts:
        return None
    run_summary = next(
        (row for row in _build_run_summaries(config, catalog_rows) if row["run_id"] == run_id),
        None,
    )
    attempts.sort(key=_run_attempt_sort_key)
    return {
        "run": run_summary,
        "attempts": attempts,
    }


def _attempt_detail_payload(
    config: AppConfig, catalog_rows: list[dict[str, Any]], attempt_id: str
) -> dict[str, Any] | None:
    row = next(
        (
            _normalize_path_fields(config, candidate)
            for candidate in catalog_rows
            if str(candidate.get("attempt_id") or "") == attempt_id
        ),
        None,
    )
    if row is None:
        return None
    result_payload = None
    curve_payload = None
    calendar_curve_payload = None
    result_path = row.get("full_backtest_result_path_36m")
    curve_path = row.get("full_backtest_curve_path_36m")
    calendar_curve_path = row.get("full_backtest_calendar_curve_path_36m")
    if isinstance(result_path, str) and result_path.strip():
        result_payload = _load_optional_json(Path(result_path))
    if isinstance(calendar_curve_path, str) and calendar_curve_path.strip():
        calendar_curve_payload = _load_optional_json(Path(calendar_curve_path))
    if isinstance(curve_path, str) and curve_path.strip():
        resolved_curve_path = Path(curve_path)
        curve_payload = _load_optional_json(resolved_curve_path)
        if calendar_curve_payload is None:
            calendar_curve_payload = _load_optional_json(
                resolved_curve_path.parent / FULL_BACKTEST_CALENDAR_CURVE_FILENAME
            )
    return {
        "attempt": row,
        "full_backtest_result": result_payload,
        "full_backtest_curve": curve_payload,
        "full_backtest_calendar_curve": calendar_curve_payload,
    }


def _live_portfolio_cache_path(config: AppConfig) -> Path:
    return config.derived_root / LIVE_PORTFOLIO_CACHE_FILENAME


def _normalize_attempt_id_list(value: Any) -> list[str]:
    seen: set[str] = set()
    attempt_ids: list[str] = []
    for item in list(value or []):
        attempt_id = str(item or "").strip()
        if not attempt_id or attempt_id in seen:
            continue
        seen.add(attempt_id)
        attempt_ids.append(attempt_id)
    return attempt_ids


def _live_portfolio_payload(config: AppConfig) -> dict[str, Any]:
    path = _live_portfolio_cache_path(config)
    payload = _load_optional_json(path)
    payload = payload if isinstance(payload, dict) else {}
    attempt_ids = _normalize_attempt_id_list(payload.get("selected_attempt_ids"))
    return {
        "selected_attempt_ids": attempt_ids,
        "updated_at": payload.get("updated_at"),
        "path": str(path),
        "path_url": _file_url(config, path) if path.exists() else None,
    }


def _write_live_portfolio_payload(config: AppConfig, attempt_ids: list[str]) -> dict[str, Any]:
    path = _live_portfolio_cache_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "selected_attempt_ids": _normalize_attempt_id_list(attempt_ids),
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, separators=(",", ":"))
    return _live_portfolio_payload(config)


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def make_handler(state: ViewerState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "AutoresearchViewer/0.2"

        def _send(self, status: int, payload: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def _send_json(self, payload: Any, *, status: int = 200) -> None:
            self._send(status, _json_bytes(payload), "application/json; charset=utf-8")

        def _send_text(self, payload: str, *, status: int = 200) -> None:
            self._send(status, payload.encode("utf-8"), "text/plain; charset=utf-8")

        def _send_dist_index(self) -> None:
            index_path = DASHBOARD_DIST_ROOT / "index.html"
            self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")

        def _read_json_body(self) -> dict[str, Any] | None:
            try:
                content_length = int(self.headers.get("Content-Length", "0") or "0")
            except ValueError:
                content_length = 0
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                return None
            return payload if isinstance(payload, dict) else None

        def _is_local_request(self) -> bool:
            client_host = str((self.client_address or ("",))[0] or "")
            return client_host in LOCAL_JOB_CLIENTS or client_host.startswith("127.")

        def _require_local_jobs(self) -> bool:
            if self._is_local_request():
                return True
            self._send_json({"error": "local_only_job_api"}, status=403)
            return False

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/api/state", "/api/overview"}:
                return self._send_json(state.snapshot())
            if parsed.path == "/api/live-portfolio":
                return self._send_json(_live_portfolio_payload(state.config))
            if parsed.path == "/api/portfolio-config":
                if not self._require_local_jobs():
                    return
                return self._send_json(state.job_manager.latest_dashboard_portfolio_config())
            if parsed.path == "/api/jobs/current":
                if not self._require_local_jobs():
                    return
                return self._send_json(state.job_manager.current() or {"status": "idle"})
            if parsed.path.startswith("/api/jobs/"):
                if not self._require_local_jobs():
                    return
                parts = [unquote(part) for part in parsed.path.split("/") if part]
                if len(parts) == 3:
                    job = state.job_manager.get(parts[2])
                    if job is None:
                        return self._send_json({"error": "job_not_found"}, status=404)
                    return self._send_json(job)
            if parsed.path == "/api/catalog":
                visible_rows, _visibility_info = filter_dashboard_visible_candidate_rows(
                    state.catalog_rows()
                )
                rows = [_normalize_path_fields(state.config, row) for row in visible_rows]
                return self._send_json(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "attempt_count": len(rows),
                        "rows": rows,
                    }
                )
            if parsed.path == "/api/runs":
                snapshot = state.snapshot()
                return self._send_json(
                    {
                        "generated_at": snapshot.get("generated_at"),
                        "run_count": len(snapshot.get("runs") or []),
                        "runs": snapshot.get("runs") or [],
                    }
                )
            if parsed.path.startswith("/api/runs/"):
                parts = [unquote(part) for part in parsed.path.split("/") if part]
                if len(parts) == 3:
                    detail = _run_detail_payload(state.config, state.catalog_rows(), parts[2])
                    if detail is None:
                        return self._send_json({"error": "run_not_found"}, status=404)
                    return self._send_json(detail)
            if parsed.path.startswith("/api/attempts/"):
                parts = [unquote(part) for part in parsed.path.split("/") if part]
                if len(parts) == 3:
                    detail = _attempt_detail_payload(state.config, state.catalog_rows(), parts[2])
                    if detail is None:
                        return self._send_json({"error": "attempt_not_found"}, status=404)
                    return self._send_json(detail)
            if parsed.path == "/files":
                query = parse_qs(parsed.query)
                raw_relative = str((query.get("path") or [""])[0] or "").strip()
                if not raw_relative:
                    return self._send_json({"error": "missing_path"}, status=400)
                result = _serve_repo_file(state.config.repo_root, raw_relative)
                if result is None:
                    return self._send_json({"error": "file_not_found"}, status=404)
                body, content_type = result
                return self._send(200, body, content_type)
            if parsed.path.startswith("/assets/") or parsed.path.endswith(".js") or parsed.path.endswith(".css") or parsed.path.endswith(".svg") or parsed.path.endswith(".png") or parsed.path.endswith(".woff2"):
                result = _serve_dist_file(parsed.path)
                if result is None:
                    return self._send_json({"error": "asset_not_found"}, status=404)
                body, content_type = result
                return self._send(200, body, content_type)
            if parsed.path == "/favicon.ico":
                result = _serve_dist_file("/favicon.ico")
                if result is not None:
                    body, content_type = result
                    return self._send(200, body, content_type)
            return self._send_dist_index()

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/jobs/finalize-corpus":
                if not self._require_local_jobs():
                    return
                payload = self._read_json_body()
                if payload is None:
                    return self._send_json({"error": "invalid_json"}, status=400)
                try:
                    return self._send_json(
                        state.job_manager.start("finalize-corpus", payload),
                        status=202,
                    )
                except RuntimeError as exc:
                    return self._send_json({"error": "job_active", "message": str(exc)}, status=409)
                except Exception as exc:
                    return self._send_json({"error": "job_start_failed", "message": str(exc)}, status=400)
            if parsed.path == "/api/jobs/build-portfolio":
                if not self._require_local_jobs():
                    return
                payload = self._read_json_body()
                if payload is None:
                    return self._send_json({"error": "invalid_json"}, status=400)
                try:
                    return self._send_json(
                        state.job_manager.start("build-portfolio", payload),
                        status=202,
                    )
                except RuntimeError as exc:
                    return self._send_json({"error": "job_active", "message": str(exc)}, status=409)
                except Exception as exc:
                    return self._send_json({"error": "job_start_failed", "message": str(exc)}, status=400)
            if parsed.path == "/api/jobs/export-live-portfolio":
                if not self._require_local_jobs():
                    return
                payload = self._read_json_body()
                if payload is None:
                    return self._send_json({"error": "invalid_json"}, status=400)
                attempt_ids = _normalize_attempt_id_list(
                    payload.get("selected_attempt_ids")
                    or _live_portfolio_payload(state.config).get("selected_attempt_ids")
                )
                if not attempt_ids:
                    return self._send_json({"error": "empty_live_portfolio"}, status=400)
                known_attempt_ids = {
                    str(row.get("attempt_id") or "")
                    for row in state.catalog_rows()
                    if row.get("attempt_id")
                }
                unknown = [
                    attempt_id
                    for attempt_id in attempt_ids
                    if known_attempt_ids and attempt_id not in known_attempt_ids
                ]
                if unknown:
                    return self._send_json(
                        {"error": "unknown_attempt_ids", "attempt_ids": unknown[:20]},
                        status=400,
                    )
                account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
                portfolio_name = str(payload.get("portfolio_name") or "").strip() or None
                portfolio_config = _default_dashboard_manual_portfolio_config(
                    len(attempt_ids),
                    account=account,
                    portfolio_name=portfolio_name,
                )
                try:
                    return self._send_json(
                        state.job_manager.start(
                            "build-portfolio",
                            {
                                "attempt_ids": attempt_ids,
                                "portfolio_config": portfolio_config,
                                "portfolio_config_label": "manual",
                            },
                        ),
                        status=202,
                    )
                except RuntimeError as exc:
                    return self._send_json({"error": "job_active", "message": str(exc)}, status=409)
                except Exception as exc:
                    return self._send_json({"error": "job_start_failed", "message": str(exc)}, status=400)
            if parsed.path == "/api/jobs/cancel":
                if not self._require_local_jobs():
                    return
                payload = self._read_json_body() or {}
                job = state.job_manager.cancel(str(payload.get("id") or "").strip() or None)
                if job is None:
                    return self._send_json({"error": "job_not_found"}, status=404)
                return self._send_json(job)
            if parsed.path == "/api/portfolio-config":
                if not self._require_local_jobs():
                    return
                payload = self._read_json_body()
                if payload is None:
                    return self._send_json({"error": "invalid_json"}, status=400)
                path = state.job_manager._write_dashboard_portfolio_config(payload)
                return self._send_json({"path": str(path), "config": payload})
            if parsed.path == "/api/live-portfolio":
                payload = self._read_json_body()
                if payload is None:
                    return self._send_json({"error": "invalid_json"}, status=400)
                attempt_ids = _normalize_attempt_id_list(payload.get("selected_attempt_ids"))
                known_attempt_ids = {
                    str(row.get("attempt_id") or "")
                    for row in state.catalog_rows()
                    if row.get("attempt_id")
                }
                unknown = [
                    attempt_id
                    for attempt_id in attempt_ids
                    if known_attempt_ids and attempt_id not in known_attempt_ids
                ]
                if unknown:
                    return self._send_json(
                        {"error": "unknown_attempt_ids", "attempt_ids": unknown[:20]},
                        status=400,
                    )
                return self._send_json(_write_live_portfolio_payload(state.config, attempt_ids))
            return self._send_json({"error": "read_only_viewer"}, status=405)

        def do_DELETE(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/live-portfolio":
                return self._send_json(_write_live_portfolio_payload(state.config, []))
            return self._send_json({"error": "read_only_viewer"}, status=405)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def serve_dashboard(
    config: AppConfig,
    *,
    host: str,
    port: int,
    limit: int = 25,
    refresh_on_start: bool = True,
    force_rebuild: bool = False,
) -> None:
    _ensure_dashboard_dist()
    state = ViewerState(config)
    public_url = f"http://{host}:{port}"
    local_url = f"http://127.0.0.1:{port}"
    print("Autoresearch dashboard viewer starting...", flush=True)
    print(f"  bind: {host}:{port}", flush=True)
    print(f"  local url: {local_url}", flush=True)
    if host not in {"127.0.0.1", "localhost"}:
        print(f"  bound url: {public_url}", flush=True)
    if limit != 25 or not refresh_on_start or force_rebuild:
        print(
            "  note: legacy dashboard flags are ignored; viewer reads existing derived artifacts only.",
            flush=True,
        )
    print(f"  frontend root: {DASHBOARD_APP_ROOT}", flush=True)
    print(f"  dist root: {DASHBOARD_DIST_ROOT}", flush=True)
    print(f"  derived root: {config.derived_root}", flush=True)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    print("Autoresearch dashboard viewer ready.", flush=True)
    print(f"  open: {local_url}", flush=True)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
