"""Per-strategy risk sizing for portfolio exports.

The portfolio optimizer selects *which* strategies belong together. This module
adds a separable post-step for deciding how much risk each imported FuzzFolio
strategy should carry while preserving the selected replay exit cell.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


METAL_SYMBOLS = {"XAUUSD", "XAGUSD"}
INDEX_SYMBOLS = {"DE40", "US500", "USTECH", "US30", "JP225", "UK100"}


@dataclass(frozen=True)
class RiskSizingSpec:
    min_risk_pct: float = 0.25
    max_risk_pct: float = 5.0
    default_cap_pct: float = 2.5


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _round_step(value: float, step: float = 0.05) -> float:
    if step <= 0:
        return round(value, 4)
    return round(round(value / step) * step, 2)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _profile_name_from_export_dir(export_dir: Path) -> str | None:
    for path in sorted(export_dir.glob("*.json")):
        if path.name.endswith(".manifest.json") or path.name == "portfolio-export-manifest.json":
            continue
        try:
            payload = _load_json(path)
        except Exception:
            continue
        profile = payload.get("profile") if isinstance(payload, dict) else None
        if isinstance(profile, dict):
            name = str(profile.get("name") or "").strip()
            if name:
                return name
    return None


def _export_profile_names(export_bundle: Path) -> dict[str, str]:
    """Return attempt_id -> FuzzFolio profile display name for an export bundle."""
    names: dict[str, str] = {}
    if not export_bundle.exists():
        return names
    for manifest_path in sorted(export_bundle.rglob("profile-drop-36mo.manifest.json")):
        try:
            manifest = _load_json(manifest_path)
        except Exception:
            continue
        attempt_id = str(manifest.get("attempt_id") or "").strip()
        if not attempt_id:
            continue
        name = _profile_name_from_export_dir(manifest_path.parent)
        if name:
            names[attempt_id] = name
    return names


def _asset_class(instruments: list[str]) -> str:
    symbols = {str(item).upper() for item in instruments}
    if symbols & METAL_SYMBOLS:
        return "metal"
    if symbols & INDEX_SYMBOLS:
        return "index"
    return "fx"


def _path_metrics_for_row(row: dict[str, Any]) -> dict[str, Any]:
    raw_path = str(row.get("full_backtest_calendar_curve_path_36m") or "").strip()
    if raw_path:
        path = Path(raw_path)
        if path.exists():
            try:
                payload = _load_json(path)
                metrics = payload.get("path_metrics")
                if isinstance(metrics, dict):
                    return metrics
            except Exception:
                pass
    return {}


def _risk_for_row(
    row: dict[str, Any],
    path_metrics: dict[str, Any],
    spec: RiskSizingSpec,
) -> tuple[float, dict[str, Any], str, str]:
    score = _safe_float(row.get("score_36m") or row.get("final_scrutiny_score"))
    final_r = _safe_float(path_metrics.get("final_equity_r") or row.get("realized_r_total_36m"))
    maxdd_r = max(
        _safe_float(path_metrics.get("max_drawdown_r") or row.get("max_drawdown_r_36m"), 1.0),
        0.25,
    )
    return_to_dd = final_r / maxdd_r if maxdd_r > 0 else 0.0
    avg_hold_h = _safe_float(path_metrics.get("avg_holding_hours") or row.get("avg_holding_hours_36m"), 999.0)
    p90_hold_h = _safe_float(path_metrics.get("p90_holding_hours") or row.get("p90_holding_hours_36m"), 999.0)
    max_hold_h = _safe_float(path_metrics.get("max_holding_hours") or row.get("max_holding_hours_36m"))
    max_losses = _safe_float(path_metrics.get("max_consecutive_losses"))
    avg_loss_streak = _safe_float(path_metrics.get("avg_loss_streak"))
    temporal_breadth = path_metrics.get("temporal_breadth") or {}
    if not isinstance(temporal_breadth, dict):
        temporal_breadth = {}
    top_10_day_share = _safe_float(temporal_breadth.get("top_10_day_share"))
    positive_week_ratio = _safe_float(
        temporal_breadth.get("positive_week_ratio") or row.get("positive_week_ratio_36m")
    )
    instruments = [
        str(item).upper()
        for item in (row.get("instruments_36m") or row.get("base_instruments") or [])
        if str(item).strip()
    ]
    asset_class = _asset_class(instruments)

    risk = 0.65
    risk += _clamp((score - 60.0) / 25.0, 0.0, 1.0) * 0.65
    risk += _clamp(return_to_dd / 30.0, 0.0, 1.0) * 0.95
    risk += _clamp((final_r - 25.0) / 90.0, 0.0, 1.0) * 0.45
    risk += _clamp((12.0 - avg_loss_streak) / 12.0, 0.0, 1.0) * 0.35
    risk += _clamp(positive_week_ratio / 0.45, 0.0, 1.0) * 0.20

    if avg_hold_h <= 8:
        risk += 0.55
    elif avg_hold_h <= 16:
        risk += 0.35
    elif avg_hold_h <= 24:
        risk += 0.15
    elif avg_hold_h <= 36:
        risk -= 0.20
    elif avg_hold_h <= 48:
        risk -= 0.45
    else:
        risk -= 0.85

    if p90_hold_h > 144:
        risk -= 0.85
    elif p90_hold_h > 120:
        risk -= 0.55
    elif p90_hold_h > 96:
        risk -= 0.35
    elif p90_hold_h > 72:
        risk -= 0.20

    if maxdd_r > 20:
        risk -= 0.75
    elif maxdd_r > 14:
        risk -= 0.45
    elif maxdd_r > 9:
        risk -= 0.20

    if max_losses > 12:
        risk -= 0.60
    elif max_losses > 9:
        risk -= 0.35
    elif max_losses > 6:
        risk -= 0.15

    if top_10_day_share > 0.55:
        risk -= 0.35
    elif top_10_day_share > 0.45:
        risk -= 0.18
    if asset_class == "metal" and maxdd_r > 5:
        risk -= 0.20
    if asset_class == "index" and p90_hold_h > 48:
        risk -= 0.15

    cap = min(spec.max_risk_pct, spec.default_cap_pct)
    if avg_hold_h > 48 or p90_hold_h > 144 or max_losses > 14:
        cap = min(spec.max_risk_pct, 1.0)
    elif avg_hold_h > 36 or p90_hold_h > 120 or maxdd_r > 18:
        cap = min(spec.max_risk_pct, 1.4)
    elif avg_hold_h > 24 or p90_hold_h > 96 or maxdd_r > 12 or max_losses > 10:
        cap = min(spec.max_risk_pct, 1.8)
    elif maxdd_r <= 2.5 and avg_hold_h <= 8 and p90_hold_h <= 16 and max_losses <= 8 and return_to_dd >= 20:
        cap = min(spec.max_risk_pct, 4.0)
    elif maxdd_r <= 5 and avg_hold_h <= 16 and p90_hold_h <= 36 and max_losses <= 8 and return_to_dd >= 15:
        cap = min(spec.max_risk_pct, 3.25)

    risk_pct = _round_step(_clamp(risk, spec.min_risk_pct, cap))
    if risk_pct >= 3.0:
        tier = "high"
    elif risk_pct >= 1.75:
        tier = "medium_high"
    elif risk_pct >= 1.0:
        tier = "medium"
    else:
        tier = "capped"

    reasons: list[str] = []
    if avg_hold_h <= 8 and maxdd_r <= 3:
        reasons.append("short-hold/low-DD sleeve")
    if return_to_dd >= 20:
        reasons.append("strong return-to-DD")
    if p90_hold_h > 96:
        reasons.append("hold-time cap")
    if max_losses >= 10:
        reasons.append("loss-streak cap")
    if maxdd_r > 12:
        reasons.append("standalone-DD cap")
    if not reasons:
        reasons.append("balanced portfolio sleeve")

    metrics = {
        "score": round(score, 4),
        "final_r": round(final_r, 4),
        "maxdd_r": round(maxdd_r, 4),
        "return_to_dd": round(return_to_dd, 4),
        "avg_hold_h": round(avg_hold_h, 4),
        "p90_hold_h": round(p90_hold_h, 4),
        "max_hold_h": round(max_hold_h, 4),
        "max_consecutive_losses": round(max_losses, 4),
        "avg_loss_streak": round(avg_loss_streak, 4),
        "top_10_day_share": round(top_10_day_share, 4),
        "positive_week_ratio": round(positive_week_ratio, 4),
        "asset_class": asset_class,
        "cap": round(cap, 4),
    }
    return risk_pct, metrics, tier, "; ".join(reasons)


def build_risk_sizing_schedule(
    *,
    portfolio_report_path: Path,
    export_bundle_path: Path | None = None,
    spec: RiskSizingSpec | None = None,
) -> list[dict[str, Any]]:
    spec = spec or RiskSizingSpec()
    report = _load_json(portfolio_report_path)
    export_bundle = export_bundle_path
    if export_bundle is None:
        raw_bundle = ((report.get("export_bundle") or {}).get("bundle_root") or "").strip()
        export_bundle = Path(raw_bundle) if raw_bundle else None
    profile_names = _export_profile_names(export_bundle) if export_bundle else {}
    rows = report.get("selected") or []
    schedule: list[dict[str, Any]] = []
    for row in rows:
        attempt_id = str(row.get("attempt_id") or "").strip()
        name = profile_names.get(attempt_id) or str(row.get("candidate_name") or attempt_id).strip()
        metrics = _path_metrics_for_row(row)
        risk_pct, risk_metrics, tier, rationale = _risk_for_row(row, metrics, spec)
        schedule.append(
            {
                "name": name,
                "attempt_id": attempt_id,
                "risk_percent": risk_pct,
                "tier": tier,
                "rationale": rationale,
                **risk_metrics,
            }
        )
    return sorted(schedule, key=lambda item: (-float(item["risk_percent"]), str(item["name"])))


def write_risk_sizing_report(
    *,
    schedule: list[dict[str, Any]],
    output_dir: Path,
    portfolio_report_path: Path,
    export_bundle_path: Path | None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "risk-sizing-schedule.json"
    csv_path = output_dir / "risk-sizing-schedule.csv"
    md_path = output_dir / "risk-sizing-report.md"
    json_path.write_text(json.dumps(schedule, indent=2), encoding="utf-8")
    if schedule:
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(schedule[0].keys()))
            writer.writeheader()
            writer.writerows(schedule)
    else:
        csv_path.write_text("", encoding="utf-8")

    risks = [float(row["risk_percent"]) for row in schedule]
    tier_counts = {
        tier: sum(1 for row in schedule if row.get("tier") == tier)
        for tier in sorted({str(row.get("tier")) for row in schedule})
    }
    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "portfolio_report_path": str(portfolio_report_path),
        "export_bundle_path": str(export_bundle_path) if export_bundle_path else None,
        "output_dir": str(output_dir),
        "schedule_count": len(schedule),
        "min_risk_percent": min(risks) if risks else None,
        "max_risk_percent": max(risks) if risks else None,
        "avg_risk_percent": (sum(risks) / len(risks)) if risks else None,
        "tier_counts": tier_counts,
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "markdown_path": str(md_path),
    }
    lines = [
        "# Portfolio Risk Sizing",
        "",
        f"- Strategies: {len(schedule)}",
        f"- Risk range: {summary['min_risk_percent']}% to {summary['max_risk_percent']}%",
        f"- Average risk: {round(float(summary['avg_risk_percent'] or 0.0), 4)}%",
        f"- Tiers: {json.dumps(tier_counts, sort_keys=True)}",
        "",
        "| Strategy | Risk % | Tier | Rationale | Score | R/DD | Avg Hold | P90 Hold | Max Losses |",
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in schedule:
        lines.append(
            "| {name} | {risk:.2f} | {tier} | {rationale} | {score:.1f} | {rdd:.1f} | {avg:.1f}h | {p90:.1f}h | {losses:.0f} |".format(
                name=str(row.get("name") or "").replace("|", "/"),
                risk=float(row.get("risk_percent") or 0.0),
                tier=row.get("tier"),
                rationale=str(row.get("rationale") or "").replace("|", "/"),
                score=float(row.get("score") or 0.0),
                rdd=float(row.get("return_to_dd") or 0.0),
                avg=float(row.get("avg_hold_h") or 0.0),
                p90=float(row.get("p90_hold_h") or 0.0),
                losses=float(row.get("max_consecutive_losses") or 0.0),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (output_dir / "manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
