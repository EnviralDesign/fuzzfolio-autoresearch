from __future__ import annotations

import json
import mimetypes
import subprocess
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .config import AppConfig
from .ledger import list_run_dirs, load_run_metadata


DASHBOARD_APP_ROOT = Path(__file__).resolve().parent / "dashboard"
DASHBOARD_DIST_ROOT = DASHBOARD_APP_ROOT / "dist"
SHORTLIST_REPORT_ROOTNAME = "shortlist-report"


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
        DASHBOARD_APP_ROOT / "package.json",
        DASHBOARD_DIST_ROOT / "index.html",
    ]
    return tuple(_file_signature(path) for path in tracked)


def _ensure_dashboard_dist() -> None:
    index_path = DASHBOARD_DIST_ROOT / "index.html"
    if index_path.exists():
        return
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
    ]:
        value = normalized.get(key)
        if isinstance(value, str) and value.strip():
            normalized[f"{key}_url"] = _file_url(config, Path(value))
    return normalized


def _score_sort_key(row: dict[str, Any]) -> tuple[bool, float, float, str]:
    score_36 = _safe_float(row.get("score_36m"))
    composite = _safe_float(row.get("composite_score"))
    primary = (
        score_36
        if score_36 is not None
        else (composite if composite is not None else float("-inf"))
    )
    secondary = composite if composite is not None else float("-inf")
    return (
        primary == float("-inf"),
        -primary,
        -secondary,
        str(row.get("attempt_id") or ""),
    )


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
        sorted_rows = sorted(rows, key=_score_sort_key)
        best_row = sorted_rows[0] if sorted_rows else None
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
    merged_rows.sort(key=_score_sort_key)
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
    summary = _load_optional_json(config.attempt_catalog_summary_path) or {}
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
    runs = _build_run_summaries(config, catalog_rows)
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
        "cadence_bands_all_scored": _cadence_band_rows(catalog_rows),
        "cadence_bands_score_ge_40": _cadence_band_rows(catalog_rows, min_score=40.0),
    }


class ViewerState:
    def __init__(self, config: AppConfig):
        self.config = config
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
    attempts.sort(key=_score_sort_key)
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
    result_path = row.get("full_backtest_result_path_36m")
    curve_path = row.get("full_backtest_curve_path_36m")
    if isinstance(result_path, str) and result_path.strip():
        result_payload = _load_optional_json(Path(result_path))
    if isinstance(curve_path, str) and curve_path.strip():
        curve_payload = _load_optional_json(Path(curve_path))
    return {
        "attempt": row,
        "full_backtest_result": result_payload,
        "full_backtest_curve": curve_payload,
    }


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")


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

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/api/state", "/api/overview"}:
                return self._send_json(state.snapshot())
            if parsed.path == "/api/catalog":
                rows = [_normalize_path_fields(state.config, row) for row in state.catalog_rows()]
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
