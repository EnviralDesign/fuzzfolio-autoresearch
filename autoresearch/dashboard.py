from __future__ import annotations

import json
import mimetypes
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .config import AppConfig
from .ledger import list_run_dirs, load_all_run_attempts, load_run_attempts, load_run_metadata
from .plotting import (
    _attempt_effective_window_months,
    _attempt_trade_count,
    _attempt_trades_per_month,
    render_leaderboard_artifacts,
    render_model_leaderboard_artifacts,
    render_progress_artifacts,
    render_tradeoff_leaderboard_artifacts,
)


STATIC_ROOT = Path(__file__).resolve().parent / "dashboard_static"


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


def _run_streaming_subprocess(argv: list[str], *, cwd: str, prefix: str) -> tuple[int, str, str]:
    process = subprocess.Popen(
        argv,
        cwd=cwd,
        text=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )
    stdout_tail: deque[str] = deque(maxlen=60)
    stderr_tail: deque[str] = deque(maxlen=60)

    def pump(stream: Any, sink: Any, tail: deque[str], label: str) -> None:
        if stream is None:
            return
        for raw_line in stream:
            line = raw_line.rstrip("\n")
            tail.append(line)
            print(f"{prefix}{label}{line}", file=sink, flush=True)

    stdout_thread = threading.Thread(
        target=pump,
        args=(process.stdout, sys.stdout, stdout_tail, ""),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=pump,
        args=(process.stderr, sys.stderr, stderr_tail, "stderr: "),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    return_code = process.wait()
    stdout_thread.join(timeout=2)
    stderr_thread.join(timeout=2)
    return return_code, "\n".join(stdout_tail), "\n".join(stderr_tail)


def _metric_value(payload: dict[str, Any], *path: str) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    try:
        value = float(current)
    except (TypeError, ValueError):
        return None
    return value


def _best_summary(attempt: dict[str, Any]) -> dict[str, Any]:
    payload = attempt.get("best_summary")
    return payload if isinstance(payload, dict) else {}


def _run_metadata_by_id(config: AppConfig) -> dict[str, dict[str, Any]]:
    return {run_dir.name: load_run_metadata(run_dir) for run_dir in list_run_dirs(config.runs_root)}


def refresh_dashboard_sources(config: AppConfig, *, limit: int, force_rebuild: bool) -> dict[str, str]:
    print("dashboard refresh: loading run attempts", flush=True)
    attempts = load_all_run_attempts(config.runs_root)
    run_metadata_by_run_id = _run_metadata_by_id(config)
    print("dashboard refresh: rendering aggregate progress", flush=True)
    render_progress_artifacts(
        attempts,
        config.aggregate_plot_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    render_leaderboard_artifacts(
        attempts,
        config.leaderboard_plot_path,
        config.leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
        limit=limit,
    )
    render_model_leaderboard_artifacts(
        attempts,
        config.model_leaderboard_plot_path,
        config.model_leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
    )
    render_tradeoff_leaderboard_artifacts(
        attempts,
        config.tradeoff_leaderboard_plot_path,
        config.tradeoff_leaderboard_json_path,
        run_metadata_by_run_id=run_metadata_by_run_id,
        lower_is_better=config.research.plot_lower_is_better,
    )
    print("dashboard refresh: running leaderboard pipeline", flush=True)
    if force_rebuild:
        print("dashboard refresh: force rebuild enabled", flush=True)
    leaderboard_argv = [sys.executable, "-m", "autoresearch", "leaderboard", "--limit", str(limit)]
    if force_rebuild:
        leaderboard_argv.append("--force-rebuild")
    return_code, stdout_tail, stderr_tail = _run_streaming_subprocess(
        leaderboard_argv,
        cwd=str(config.repo_root),
        prefix="  leaderboard | ",
    )
    if return_code != 0:
        raise RuntimeError(
            f"leaderboard refresh failed\nstdout:\n{stdout_tail[:1600]}\n\nstderr:\n{stderr_tail[:1600]}"
        )
    print("dashboard refresh: done", flush=True)
    return {
        "aggregate_plot": str(config.aggregate_plot_path),
        "leaderboard_plot": str(config.leaderboard_plot_path),
        "model_plot": str(config.model_leaderboard_plot_path),
        "tradeoff_plot": str(config.tradeoff_leaderboard_plot_path),
        "validation_scatter_plot": str(config.validation_scatter_plot_path),
        "validation_delta_plot": str(config.validation_delta_plot_path),
        "similarity_heatmap_plot": str(config.similarity_heatmap_plot_path),
        "similarity_scatter_plot": str(config.similarity_scatter_plot_path),
    }


def _repo_relative(config: AppConfig, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(config.repo_root.resolve())).replace("\\", "/")
    except Exception:
        return None


def _file_url(config: AppConfig, path: Path | None) -> str | None:
    relative = _repo_relative(config, path)
    if not relative:
        return None
    return f"/files?path={relative}"


def _attempt_max_drawdown_r(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    candidates = [
        best_summary.get("best_cell_path_metrics"),
        best_summary.get("quality_score_payload"),
    ]
    for payload in candidates:
        value = _metric_value(payload if isinstance(payload, dict) else {}, "max_drawdown_r")
        if value is not None:
            return value
        value = _metric_value(payload if isinstance(payload, dict) else {}, "inputs", "max_drawdown_r")
        if value is not None:
            return value
    return None


def _attempt_positive_cell_ratio(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    value = _metric_value(best_summary, "matrix_summary", "positive_cell_ratio")
    if value is not None:
        return value
    return _metric_value(best_summary, "quality_score_payload", "inputs", "positive_cell_ratio")


def _attempt_expectancy_r(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    value = _metric_value(best_summary, "quality_score_payload", "inputs", "expectancy_r")
    if value is not None:
        return value
    return _metric_value(best_summary, "best_cell", "avg_net_r_per_closed_trade")


def _attempt_profit_factor(attempt: dict[str, Any]) -> float | None:
    return _metric_value(_best_summary(attempt), "best_cell", "profit_factor")


def _attempt_signal_selectivity(attempt: dict[str, Any]) -> str | None:
    payload = _best_summary(attempt).get("behavior_summary")
    if not isinstance(payload, dict):
        return None
    value = payload.get("signal_selectivity")
    return str(value).strip() if value else None


def _attempt_instrument(attempt: dict[str, Any]) -> str | None:
    value = _best_summary(attempt).get("instrument")
    if value and value != "__BASKET__":
        return str(value)
    response_path = _best_summary(attempt).get("response_path")
    if isinstance(response_path, str):
        sensitivity = _load_optional_json(Path(response_path))
        aggregate = ((sensitivity or {}).get("data") or {}).get("aggregate") if isinstance(sensitivity, dict) else None
        if isinstance(aggregate, dict):
            instrument = aggregate.get("instrument")
            if instrument:
                return str(instrument)
    return "basket"


def _attempt_timeframe(attempt: dict[str, Any]) -> str | None:
    value = _best_summary(attempt).get("timeframe")
    if value:
        return str(value)
    response_path = _best_summary(attempt).get("response_path")
    if isinstance(response_path, str):
        deep_replay_job = _load_optional_json(Path(response_path).with_name("deep-replay-job.json"))
        request_payload = deep_replay_job.get("request") if isinstance(deep_replay_job, dict) else None
        if isinstance(request_payload, dict) and request_payload.get("timeframe"):
            return str(request_payload.get("timeframe"))
    return None


def _curve_path_for_attempt(attempt: dict[str, Any]) -> Path | None:
    artifact_dir = attempt.get("artifact_dir")
    if not isinstance(artifact_dir, str) or not artifact_dir.strip():
        return None
    path = Path(artifact_dir) / "best-cell-path-detail.json"
    return path if path.exists() else None


def _deep_replay_job_path_for_attempt(attempt: dict[str, Any]) -> Path | None:
    artifact_dir = attempt.get("artifact_dir")
    if not isinstance(artifact_dir, str) or not artifact_dir.strip():
        return None
    path = Path(artifact_dir) / "deep-replay-job.json"
    return path if path.exists() else None


def _sensitivity_path_for_attempt(attempt: dict[str, Any]) -> Path | None:
    raw = attempt.get("sensitivity_snapshot_path") or _best_summary(attempt).get("response_path")
    if not isinstance(raw, str) or not raw.strip():
        return None
    path = Path(raw)
    return path if path.exists() else None


def _count_advisor_injections(run_dir: Path) -> tuple[int, int | None, str | None]:
    log_path = run_dir / "controller-log.jsonl"
    if not log_path.exists():
        return 0, None, None
    advisor_count = 0
    latest_step: int | None = None
    latest_timestamp: str | None = None
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                latest_step = int(payload.get("step"))
            except (TypeError, ValueError):
                pass
            timestamp = payload.get("timestamp")
            if timestamp:
                latest_timestamp = str(timestamp)
            for result in payload.get("results", []):
                if isinstance(result, dict) and result.get("tool") == "advisor_guidance":
                    advisor_count += 1
    return advisor_count, latest_step, latest_timestamp


def _attempt_summary(config: AppConfig, attempt: dict[str, Any]) -> dict[str, Any]:
    best_summary = _best_summary(attempt)
    curve_path = _curve_path_for_attempt(attempt)
    profile_path = Path(str(attempt.get("profile_path"))) if attempt.get("profile_path") else None
    artifact_dir = Path(str(attempt.get("artifact_dir"))) if attempt.get("artifact_dir") else None
    sensitivity_path = _sensitivity_path_for_attempt(attempt)
    deep_replay_job_path = _deep_replay_job_path_for_attempt(attempt)
    return {
        "attemptId": attempt.get("attempt_id"),
        "sequence": attempt.get("sequence"),
        "createdAt": attempt.get("created_at"),
        "candidateName": attempt.get("candidate_name"),
        "score": attempt.get("composite_score"),
        "scoreBasis": attempt.get("score_basis"),
        "metrics": attempt.get("metrics") or {},
        "tradeCount": _attempt_trade_count(attempt),
        "tradesPerMonth": _attempt_trades_per_month(attempt),
        "effectiveWindowMonths": _attempt_effective_window_months(attempt),
        "maxDrawdownR": _attempt_max_drawdown_r(attempt),
        "positiveCellRatio": _attempt_positive_cell_ratio(attempt),
        "expectancyR": _attempt_expectancy_r(attempt),
        "profitFactor": _attempt_profit_factor(attempt),
        "signalSelectivity": _attempt_signal_selectivity(attempt),
        "instrument": _attempt_instrument(attempt),
        "timeframe": _attempt_timeframe(attempt),
        "profileRef": attempt.get("profile_ref"),
        "artifactDir": str(artifact_dir) if artifact_dir else None,
        "artifactDirUrl": _file_url(config, artifact_dir) if artifact_dir else None,
        "profilePath": str(profile_path) if profile_path else None,
        "profilePathUrl": _file_url(config, profile_path) if profile_path else None,
        "sensitivityPath": str(sensitivity_path) if sensitivity_path else None,
        "sensitivityPathUrl": _file_url(config, sensitivity_path) if sensitivity_path else None,
        "curvePath": str(curve_path) if curve_path else None,
        "curvePathUrl": _file_url(config, curve_path) if curve_path else None,
        "deepReplayJobPath": str(deep_replay_job_path) if deep_replay_job_path else None,
        "deepReplayJobPathUrl": _file_url(config, deep_replay_job_path) if deep_replay_job_path else None,
        "bestSummary": best_summary,
    }


def _best_attempt_for_run(attempts: list[dict[str, Any]], *, lower_is_better: bool) -> dict[str, Any] | None:
    scored = [attempt for attempt in attempts if attempt.get("composite_score") is not None]
    if not scored:
        return None
    return sorted(
        scored,
        key=lambda row: float(row.get("composite_score")),
        reverse=not lower_is_better,
    )[0]


def _run_summary(config: AppConfig, run_dir: Path) -> dict[str, Any]:
    metadata = load_run_metadata(run_dir)
    attempts = load_run_attempts(run_dir)
    advisor_count, latest_step, latest_timestamp = _count_advisor_injections(run_dir)
    best_attempt = _best_attempt_for_run(attempts, lower_is_better=config.research.plot_lower_is_better)
    scored_count = sum(1 for attempt in attempts if attempt.get("composite_score") is not None)
    curve_count = sum(1 for attempt in attempts if _curve_path_for_attempt(attempt) is not None)
    latest_attempt_at = max((str(attempt.get("created_at") or "") for attempt in attempts), default=None)
    return {
        "runId": run_dir.name,
        "createdAt": metadata.get("created_at"),
        "explorerProfile": metadata.get("explorer_profile"),
        "explorerModel": metadata.get("explorer_model"),
        "supervisorProfile": metadata.get("supervisor_profile"),
        "supervisorModel": metadata.get("supervisor_model"),
        "qualityScorePreset": metadata.get("quality_score_preset"),
        "attemptCount": len(attempts),
        "scoredAttemptCount": scored_count,
        "curveAttemptCount": curve_count,
        "latestAttemptAt": latest_attempt_at,
        "latestStep": latest_step,
        "latestLogTimestamp": latest_timestamp,
        "advisorGuidanceCount": advisor_count,
        "progressPngUrl": _file_url(config, run_dir / "progress.png"),
        "profileDrop12PngUrl": _file_url(config, run_dir / "profile-drop-12mo.png") if (run_dir / "profile-drop-12mo.png").exists() else None,
        "profileDrop36PngUrl": _file_url(config, run_dir / "profile-drop-36mo.png") if (run_dir / "profile-drop-36mo.png").exists() else None,
        "bestAttempt": _attempt_summary(config, best_attempt) if best_attempt else None,
    }


def _model_consistency_rows(leaderboard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in leaderboard_rows:
        metadata = row.get("run_metadata") or {}
        label = str(metadata.get("explorer_model") or metadata.get("explorer_profile") or "unknown")
        grouped.setdefault(label, []).append(row)

    rows: list[dict[str, Any]] = []
    for label, items in grouped.items():
        scores = [float(item.get("composite_score")) for item in items if item.get("composite_score") is not None]
        if not scores:
            continue
        rows.append(
            {
                "modelLabel": label,
                "runCount": len(items),
                "averageScore": sum(scores) / len(scores),
                "medianScore": median(scores),
                "bestScore": max(scores),
                "score70PlusRate": sum(1 for score in scores if score >= 70.0) / len(scores),
                "score80PlusRate": sum(1 for score in scores if score >= 80.0) / len(scores),
            }
        )
    rows.sort(key=lambda row: row["averageScore"], reverse=True)
    return rows


def _drawdown_rows(leaderboard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in leaderboard_rows:
        drawdown = _attempt_max_drawdown_r(row)
        if drawdown is None or row.get("composite_score") is None:
            continue
        rows.append(
            {
                "runId": row.get("run_id"),
                "attemptId": row.get("attempt_id"),
                "label": row.get("leaderboard_label"),
                "score": float(row.get("composite_score")),
                "maxDrawdownR": drawdown,
                "tradesPerMonth": _attempt_trades_per_month(row),
                "tradeCount": _attempt_trade_count(row),
            }
        )
    rows.sort(key=lambda row: (row["maxDrawdownR"], -row["score"]))
    return rows


def build_dashboard_payload(config: AppConfig, *, limit: int) -> dict[str, Any]:
    run_dirs = list_run_dirs(config.runs_root)
    attempts = load_all_run_attempts(config.runs_root)
    leaderboard_rows = _load_optional_json(config.leaderboard_json_path) or []
    model_rows = _load_optional_json(config.model_leaderboard_json_path) or []
    tradeoff_rows = _load_optional_json(config.tradeoff_leaderboard_json_path) or []
    validation_rows = _load_optional_json(config.validation_leaderboard_json_path) or []
    similarity_payload = _load_optional_json(config.similarity_leaderboard_json_path) or {}
    similarity_leaders = list((similarity_payload or {}).get("leaders") or [])
    similarity_pairs = list((similarity_payload or {}).get("pairs") or [])
    run_summaries = [_run_summary(config, run_dir) for run_dir in reversed(run_dirs)]

    best_scores = [
        float(row["bestAttempt"]["score"])
        for row in run_summaries
        if isinstance(row.get("bestAttempt"), dict) and row["bestAttempt"].get("score") is not None
    ]
    overview = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "repoRoot": str(config.repo_root),
        "runsRoot": str(config.runs_root),
        "runCount": len(run_summaries),
        "attemptCount": len(attempts),
        "scoredRunCount": len(best_scores),
        "bestScore": max(best_scores) if best_scores else None,
        "medianBestScore": median(best_scores) if best_scores else None,
        "profileDropCount": sum(1 for row in run_summaries if row.get("profileDropPngUrl")),
        "curveCoverageCount": sum(1 for row in run_summaries if (row.get("curveAttemptCount") or 0) > 0),
        "leaderboardCount": len(leaderboard_rows),
        "modelBucketCount": len(model_rows),
        "tradeoffPointCount": len(tradeoff_rows),
        "validationPointCount": len(validation_rows),
        "similarityLeaderCount": len(similarity_leaders),
    }

    return {
        "overview": overview,
        "images": {
            "aggregatePlotUrl": _file_url(config, config.aggregate_plot_path) if config.aggregate_plot_path.exists() else None,
            "leaderboardPlotUrl": _file_url(config, config.leaderboard_plot_path) if config.leaderboard_plot_path.exists() else None,
            "modelLeaderboardPlotUrl": _file_url(config, config.model_leaderboard_plot_path) if config.model_leaderboard_plot_path.exists() else None,
            "tradeoffPlotUrl": _file_url(config, config.tradeoff_leaderboard_plot_path) if config.tradeoff_leaderboard_plot_path.exists() else None,
            "validationScatterPlotUrl": _file_url(config, config.validation_scatter_plot_path) if config.validation_scatter_plot_path.exists() else None,
            "validationDeltaPlotUrl": _file_url(config, config.validation_delta_plot_path) if config.validation_delta_plot_path.exists() else None,
            "similarityHeatmapPlotUrl": _file_url(config, config.similarity_heatmap_plot_path) if config.similarity_heatmap_plot_path.exists() else None,
            "similarityScatterPlotUrl": _file_url(config, config.similarity_scatter_plot_path) if config.similarity_scatter_plot_path.exists() else None,
        },
        "leaderboard": leaderboard_rows,
        "modelAverages": model_rows,
        "modelConsistency": _model_consistency_rows(leaderboard_rows),
        "tradeoff": tradeoff_rows,
        "validation": validation_rows,
        "similarity": similarity_leaders,
        "similarityPairs": similarity_pairs,
        "scoreVsDrawdown": _drawdown_rows(leaderboard_rows),
        "runs": run_summaries,
        "limit": limit,
    }


def build_run_detail(config: AppConfig, run_id: str) -> dict[str, Any] | None:
    run_dir = config.runs_root / run_id
    if not run_dir.exists() or not run_dir.is_dir():
        return None
    summary = _run_summary(config, run_dir)
    attempts = load_run_attempts(run_dir)
    attempt_rows = [_attempt_summary(config, attempt) for attempt in attempts]
    attempt_rows.sort(
        key=lambda row: (
            row.get("score") is None,
            -(float(row["score"]) if row.get("score") is not None else float("-inf")),
            int(row.get("sequence") or 0),
        )
    )
    return {
        "run": summary,
        "attempts": attempt_rows,
    }


def build_attempt_detail(config: AppConfig, run_id: str, attempt_id: str) -> dict[str, Any] | None:
    run_dir = config.runs_root / run_id
    if not run_dir.exists() or not run_dir.is_dir():
        return None
    for attempt in load_run_attempts(run_dir):
        if str(attempt.get("attempt_id")) != attempt_id:
            continue
        summary = _attempt_summary(config, attempt)
        curve_payload = _load_optional_json(_curve_path_for_attempt(attempt) or Path("__missing__"))
        sensitivity_payload = _load_optional_json(_sensitivity_path_for_attempt(attempt) or Path("__missing__"))
        deep_replay_job = _load_optional_json(_deep_replay_job_path_for_attempt(attempt) or Path("__missing__"))
        profile_payload = _load_optional_json(Path(str(attempt.get("profile_path")))) if attempt.get("profile_path") else None
        return {
            "runId": run_id,
            "attempt": summary,
            "curve": curve_payload,
            "sensitivity": sensitivity_payload,
            "deepReplayJob": deep_replay_job,
            "profile": profile_payload,
            "profileDrop12PngUrl": _file_url(config, run_dir / "profile-drop-12mo.png") if (run_dir / "profile-drop-12mo.png").exists() else None,
            "profileDrop36PngUrl": _file_url(config, run_dir / "profile-drop-36mo.png") if (run_dir / "profile-drop-36mo.png").exists() else None,
        }
    return None


class DashboardState:
    def __init__(self, config: AppConfig, *, limit: int, force_rebuild: bool):
        self.config = config
        self.limit = limit
        self.force_rebuild = force_rebuild
        self._lock = threading.Lock()
        self.payload: dict[str, Any] = {}

    def rebuild(self) -> dict[str, Any]:
        refresh_dashboard_sources(self.config, limit=self.limit, force_rebuild=self.force_rebuild)
        payload = build_dashboard_payload(self.config, limit=self.limit)
        with self._lock:
            self.payload = payload
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.payload)


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")


def _serve_file(repo_root: Path, raw_relative: str) -> tuple[bytes, str] | None:
    candidate = (repo_root / raw_relative).resolve()
    if repo_root.resolve() not in candidate.parents and candidate != repo_root.resolve():
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    mime_type, _ = mimetypes.guess_type(str(candidate))
    return candidate.read_bytes(), mime_type or "application/octet-stream"


def make_handler(state: DashboardState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "AutoresearchDashboard/0.1"

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

        def _send_static(self, path: Path, content_type: str) -> None:
            self._send(200, path.read_bytes(), content_type)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                return self._send_static(STATIC_ROOT / "index.html", "text/html; charset=utf-8")
            if parsed.path == "/app.js":
                return self._send_static(STATIC_ROOT / "app.js", "application/javascript; charset=utf-8")
            if parsed.path == "/styles.css":
                return self._send_static(STATIC_ROOT / "styles.css", "text/css; charset=utf-8")
            if parsed.path == "/api/overview":
                return self._send_json(state.snapshot())
            if parsed.path == "/api/refresh":
                payload = state.rebuild()
                return self._send_json(payload)
            if parsed.path.startswith("/api/runs/"):
                parts = [unquote(part) for part in parsed.path.split("/") if part]
                if len(parts) == 3:
                    detail = build_run_detail(state.config, parts[2])
                    if detail is None:
                        return self._send_json({"error": "run_not_found"}, status=404)
                    return self._send_json(detail)
                if len(parts) == 5 and parts[3] == "attempts":
                    detail = build_attempt_detail(state.config, parts[2], parts[4])
                    if detail is None:
                        return self._send_json({"error": "attempt_not_found"}, status=404)
                    return self._send_json(detail)
            if parsed.path == "/files":
                query = parse_qs(parsed.query)
                raw_relative = str((query.get("path") or [""])[0] or "").strip()
                if not raw_relative:
                    return self._send_json({"error": "missing_path"}, status=400)
                result = _serve_file(state.config.repo_root, raw_relative)
                if result is None:
                    return self._send_json({"error": "file_not_found"}, status=404)
                body, content_type = result
                return self._send(200, body, content_type)
            if parsed.path.startswith("/assets/"):
                asset_path = (STATIC_ROOT / parsed.path.removeprefix("/assets/")).resolve()
                if STATIC_ROOT.resolve() not in asset_path.parents or not asset_path.exists():
                    return self._send_json({"error": "asset_not_found"}, status=404)
                mime_type, _ = mimetypes.guess_type(str(asset_path))
                return self._send(200, asset_path.read_bytes(), mime_type or "application/octet-stream")
            return self._send_static(STATIC_ROOT / "index.html", "text/html; charset=utf-8")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/refresh":
                payload = state.rebuild()
                return self._send_json(payload)
            return self._send_json({"error": "not_found"}, status=404)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def serve_dashboard(
    config: AppConfig,
    *,
    host: str,
    port: int,
    limit: int,
    refresh_on_start: bool = True,
    force_rebuild: bool = False,
) -> None:
    state = DashboardState(config, limit=limit, force_rebuild=force_rebuild)
    public_url = f"http://{host}:{port}"
    local_url = f"http://127.0.0.1:{port}"
    print("Autoresearch dashboard starting...", flush=True)
    print(f"  bind: {host}:{port}", flush=True)
    print(f"  local url: {local_url}", flush=True)
    if host not in {"127.0.0.1", "localhost"}:
        print(f"  bound url: {public_url}", flush=True)
    print(f"  leaderboard limit: {limit}", flush=True)
    print(f"  refresh on start: {'yes' if refresh_on_start else 'no'}", flush=True)
    print(f"  force rebuild: {'yes' if force_rebuild else 'no'}", flush=True)
    print(f"  runs root: {config.runs_root}", flush=True)
    if refresh_on_start:
        print("  building dashboard payload from source artifacts...", flush=True)
        state.rebuild()
    else:
        print("  loading dashboard payload from existing derived artifacts...", flush=True)
        state.payload = build_dashboard_payload(config, limit=limit)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    print("Autoresearch dashboard ready.", flush=True)
    print(f"  open: {local_url}", flush=True)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
