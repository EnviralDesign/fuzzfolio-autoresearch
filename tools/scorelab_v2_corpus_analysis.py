from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TRADING_DASHBOARD_ROOT = Path("C:/repos/Trading-Dashboard")
RUNS = ROOT / "runs"
OUT = RUNS / "derived" / "scorelab-v2-analysis-20260430"
INDICATORS_PATH = TRADING_DASHBOARD_ROOT / "shared" / "constants" / "indicators.json"


def safe_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def gm(values: list[float | None]) -> float | None:
    filtered = [clamp01(value) for value in values if value is not None]
    if not filtered:
        return None
    product = 1.0
    for value in filtered:
        product *= max(1e-6, value)
    return clamp01(product ** (1.0 / len(filtered)))


def wgm(values: list[tuple[float | None, float]]) -> float | None:
    weight_sum = 0.0
    log_sum = 0.0
    for value, weight in values:
        if value is None or weight <= 0:
            continue
        weight_sum += weight
        log_sum += weight * math.log(max(1e-6, clamp01(value)))
    if weight_sum <= 0:
        return None
    return clamp01(math.exp(log_sum / weight_sum))


def lin(value: Any, low: float, high: float) -> float | None:
    numeric = safe_float(value)
    if numeric is None or high <= low:
        return None
    return clamp01((numeric - low) / (high - low))


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def nested(payload: Any, path: list[str]) -> Any:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def percentile(values: list[float], q: float) -> float | None:
    sorted_values = sorted(value for value in values if value is not None)
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return sorted_values[int(position)]
    return sorted_values[low] * (high - position) + sorted_values[high] * (position - low)


def stats(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [safe_float(row.get(field)) for row in rows]
    values = [value for value in values if value is not None]
    return {
        "count": len(values),
        "mean": statistics.mean(values) if values else None,
        "median": percentile(values, 0.5),
        "p10": percentile(values, 0.1),
        "p25": percentile(values, 0.25),
        "p75": percentile(values, 0.75),
        "p90": percentile(values, 0.9),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def correlation(rows: list[dict[str, Any]], left: str, right: str) -> float | None:
    pairs = [
        (safe_float(row.get(left)), safe_float(row.get(right)))
        for row in rows
    ]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    span_x = math.sqrt(sum((value - mean_x) ** 2 for value in xs))
    span_y = math.sqrt(sum((value - mean_y) ** 2 for value in ys))
    if span_x <= 0 or span_y <= 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / (span_x * span_y)


def indicator_metadata() -> dict[str, dict[str, Any]]:
    payload = load_json(INDICATORS_PATH)
    result: dict[str, dict[str, Any]] = {}
    if not isinstance(payload, dict):
        return result
    for indicator in payload.get("indicators") or []:
        if not isinstance(indicator, dict):
            continue
        meta = indicator.get("meta") or {}
        config = indicator.get("config") or {}
        indicator_id = str(meta.get("id") or "").strip().upper()
        if not indicator_id:
            continue
        result[indicator_id] = {
            "name": meta.get("name") or config.get("label") or indicator_id,
            "label": config.get("label") or meta.get("name") or indicator_id,
            "role": meta.get("preferredTimeframeRole") or "",
            "category": meta.get("category") or "",
        }
    return result


def scorelab_maps(score_lab: dict[str, Any]) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, list[dict[str, Any]]]]:
    axes: dict[str, float] = {}
    components: dict[str, float] = {}
    raws: dict[str, float] = {}
    diagnostics: dict[str, list[dict[str, Any]]] = {}
    for axis in score_lab.get("axes") or []:
        if not isinstance(axis, dict):
            continue
        axis_key = str(axis.get("key") or "").strip()
        if axis_key:
            axes[f"axis_{axis_key}"] = safe_float(axis.get("score"))
        for component in axis.get("components") or []:
            if not isinstance(component, dict):
                continue
            component_key = str(component.get("key") or "").strip()
            if not axis_key or not component_key:
                continue
            key = f"{axis_key}.{component_key}"
            components[key] = safe_float(component.get("score"))
            raws[key] = safe_float(component.get("raw"))
            if isinstance(component.get("diagnostics"), list):
                diagnostics[key] = component["diagnostics"]
    return axes, components, raws, diagnostics


def entry_spacing_score(behavior: dict[str, Any]) -> float | None:
    max_run = safe_float(behavior.get("max_consecutive_signal_run"))
    coverage = safe_float(behavior.get("signal_coverage_ratio"))
    bars_per_signal = safe_float(behavior.get("bars_per_signal"))
    if coverage is None or coverage <= 0 or bars_per_signal is None:
        return None
    return gm(
        [
            None if max_run is None else math.exp(-max(0.0, max_run - 1.0) / 10.0),
            None if coverage is None else math.exp(-max(0.0, coverage - 0.08) / 0.10),
            lin(bars_per_signal, 3.0, 20.0),
        ]
    )


def build_v2_draft(components: dict[str, float], entry_spacing: float | None) -> dict[str, float | None]:
    proof = wgm(
        [
            (components.get("proof.belief"), 1.5),
            (components.get("proof.trade_support"), 1.0),
        ]
    )
    edge = wgm(
        [
            (components.get("edge.expectancy"), 1.2),
            (components.get("edge.profit_factor"), 1.0),
            (components.get("edge.edge_rate"), 0.55),
        ]
    )
    capital_pain = gm(
        [
            components.get("ride.drawdown_resilience"),
            components.get("ride.ulcer_resilience"),
            components.get("ride.underwater_resilience"),
        ]
    )
    accrual_quality = gm(
        [
            components.get("ride.smoothness"),
            components.get("ride.temporal_breadth"),
            components.get("ride.spike_resistance"),
        ]
    )
    ride = wgm(
        [
            (capital_pain, 1.25),
            (components.get("ride.loss_streak_resilience"), 1.75),
            (accrual_quality, 1.0),
        ]
    )
    global_pocket = gm(
        [
            components.get("stability.positive_cell_area"),
            components.get("stability.largest_cluster"),
        ]
    )
    local_pocket = gm(
        [
            components.get("stability.neighbor_support"),
            components.get("stability.perturbation_stability"),
            components.get("stability.best_neighbors"),
        ]
    )
    stability = wgm(
        [
            (global_pocket, 1.0),
            (local_pocket, 1.25),
            (components.get("stability.robust_expectancy"), 1.0),
        ]
    )
    viability = wgm(
        [
            (components.get("viability.cadence"), 1.0),
            (components.get("viability.longevity"), 1.35),
            (components.get("viability.instrument_consensus"), 1.0),
            (entry_spacing, 1.25),
        ]
    )
    score = wgm(
        [
            (proof, 1.0),
            (edge, 1.0),
            (ride, 1.0),
            (stability, 1.0),
            (viability, 1.0),
        ]
    )
    return {
        "proof_v2": proof,
        "edge_v2": edge,
        "capital_pain_v2": capital_pain,
        "accrual_quality_v2": accrual_quality,
        "ride_v2": ride,
        "global_pocket_v2": global_pocket,
        "local_pocket_v2": local_pocket,
        "stability_v2": stability,
        "viability_v2": viability,
        "score_v2_draft": score,
    }


def parse_attempts() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metadata = indicator_metadata()
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    new_entry_ids = {
        "RSI_CROSSBACK",
        "STOCHRSI_CROSSBACK",
        "PRICE_RECLAIM_MA",
        "CHANNEL_REENTRY",
        "BREAKOUT_FIRST_CLOSE",
        "WICK_REJECTION",
    }
    for attempts_path in RUNS.rglob("attempts.jsonl"):
        run_dir = attempts_path.parent
        run_meta = load_json(run_dir / "run-metadata.json")
        run_meta = run_meta if isinstance(run_meta, dict) else {}
        with attempts_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip() or line.lstrip("\x00").strip() == "":
                    if line.strip():
                        errors.append({"file": str(attempts_path), "line": line_number, "kind": "nul_or_blank"})
                    continue
                try:
                    attempt = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append({"file": str(attempts_path), "line": line_number, "kind": type(exc).__name__, "sample": line[:120]})
                    continue
                best_summary = attempt.get("best_summary") or {}
                score_lab = best_summary.get("score_lab_payload") or {}
                axes, components, raws, diagnostics = scorelab_maps(score_lab if isinstance(score_lab, dict) else {})
                behavior = best_summary.get("behavior_summary") or {}
                best_cell = best_summary.get("best_cell") or {}
                path_metrics = best_summary.get("best_cell_path_metrics") or {}
                quality_payload = best_summary.get("quality_score_payload") or {}
                quality_inputs = quality_payload.get("inputs") if isinstance(quality_payload.get("inputs"), dict) else {}
                matrix_summary = best_summary.get("matrix_summary") or {}
                profile_path = Path(str(attempt.get("profile_path") or ""))
                profile_payload = load_json(profile_path) if profile_path.exists() else None
                profile = profile_payload.get("profile") if isinstance(profile_payload, dict) else {}

                indicator_ids: list[str] = []
                indicator_labels: list[str] = []
                indicator_roles: list[str] = []
                indicator_timeframes: list[str] = []
                trend_count = 0
                mean_reversion_count = 0
                entry_count = 0
                new_trigger_count = 0
                for indicator in profile.get("indicators") or []:
                    if not isinstance(indicator, dict):
                        continue
                    indicator_id = str(nested(indicator, ["meta", "id"]) or "").strip().upper()
                    config = indicator.get("config") or {}
                    meta = metadata.get(indicator_id, {})
                    if indicator_id:
                        indicator_ids.append(indicator_id)
                    indicator_labels.append(str(config.get("label") or meta.get("label") or indicator_id))
                    role = str(meta.get("role") or "")
                    if role:
                        indicator_roles.append(role)
                    if role == "entry":
                        entry_count += 1
                    if indicator_id in new_entry_ids:
                        new_trigger_count += 1
                    timeframe = str(config.get("timeframe") or "").strip().upper()
                    if timeframe:
                        indicator_timeframes.append(timeframe)
                    if config.get("isTrendFollowing") is True:
                        trend_count += 1
                    elif config.get("isTrendFollowing") is False:
                        mean_reversion_count += 1

                spacing = entry_spacing_score(behavior)
                v2 = build_v2_draft(components, spacing)
                score_v1 = safe_float(attempt.get("composite_score"))
                if score_v1 is None:
                    score_v1 = safe_float(best_summary.get("score_lab"))
                row = {
                    "run_id": attempt.get("run_id"),
                    "attempt_id": attempt.get("attempt_id"),
                    "sequence": attempt.get("sequence"),
                    "candidate_name": attempt.get("candidate_name"),
                    "created_at": attempt.get("created_at"),
                    "artifact_dir": attempt.get("artifact_dir"),
                    "profile_ref": attempt.get("profile_ref"),
                    "profile_path": attempt.get("profile_path"),
                    "explorer_model": run_meta.get("explorer_model"),
                    "explorer_profile": run_meta.get("explorer_profile"),
                    "score_v1": score_v1,
                    "score_v2_draft": None if v2["score_v2_draft"] is None else v2["score_v2_draft"] * 100.0,
                    "score_delta_v2_minus_v1": None if score_v1 is None or v2["score_v2_draft"] is None else v2["score_v2_draft"] * 100.0 - score_v1,
                    "score_basis": attempt.get("score_basis"),
                    "scorelab_version": score_lab.get("version") if isinstance(score_lab, dict) else None,
                    "requested_horizon_months": attempt.get("requested_horizon_months"),
                    "effective_window_months": attempt.get("effective_window_months"),
                    "timeframe": best_summary.get("timeframe") or attempt.get("effective_timeframe") or attempt.get("requested_timeframe"),
                    "instrument": best_summary.get("instrument"),
                    "profile_instruments": ",".join(profile.get("instruments") or []) if isinstance(profile, dict) else "",
                    "instrument_count": len(profile.get("instruments") or []) if isinstance(profile, dict) else None,
                    "indicator_ids": ",".join(sorted(set(indicator_ids))),
                    "indicator_labels": " + ".join(indicator_labels),
                    "indicator_roles": ",".join(sorted(set(indicator_roles))),
                    "indicator_count": len(indicator_ids),
                    "entry_indicator_count": entry_count,
                    "new_trigger_indicator_count": new_trigger_count,
                    "trend_indicator_count": trend_count,
                    "mean_reversion_indicator_count": mean_reversion_count,
                    "indicator_timeframes": ",".join(sorted(set(indicator_timeframes))),
                    "best_reward_multiple": safe_float(best_cell.get("reward_multiple")),
                    "best_stop_loss_percent": safe_float(best_cell.get("stop_loss_percent")),
                    "best_take_profit_percent": safe_float(best_cell.get("take_profit_percent")),
                    "best_expectancy_r": safe_float(best_cell.get("avg_net_r_per_closed_trade")),
                    "best_profit_factor": safe_float(best_cell.get("profit_factor")),
                    "resolved_trades": safe_float(
                        attempt.get("resolved_trades")
                        or best_cell.get("resolved_trades")
                        or quality_inputs.get("resolved_trades")
                        or path_metrics.get("trade_count")
                    ),
                    "trades_per_month": safe_float(attempt.get("trades_per_month") or quality_inputs.get("trades_per_month")),
                    "final_equity_r": safe_float(path_metrics.get("final_equity_r")),
                    "max_drawdown_r": safe_float(path_metrics.get("max_drawdown_r")),
                    "ulcer_index_r": safe_float(path_metrics.get("ulcer_index_r")),
                    "time_under_water_ratio": safe_float(path_metrics.get("time_under_water_ratio")),
                    "psr": safe_float(path_metrics.get("psr") or quality_inputs.get("belief")),
                    "k_ratio": safe_float(path_metrics.get("k_ratio")),
                    "signal_count": safe_float(best_summary.get("signal_count")),
                    "signal_selectivity": behavior.get("signal_selectivity"),
                    "signal_coverage_ratio": safe_float(behavior.get("signal_coverage_ratio")),
                    "bars_per_signal": safe_float(behavior.get("bars_per_signal")),
                    "max_consecutive_signal_run": safe_float(behavior.get("max_consecutive_signal_run")),
                    "median_signal_run": safe_float(behavior.get("median_signal_run")),
                    "direction_flip_rate": safe_float(behavior.get("direction_flip_rate")),
                    "entry_spacing_score_experimental": None if spacing is None else spacing * 100.0,
                    "state_only_profile": behavior.get("state_only_profile"),
                    "calibrated_entry_label": behavior.get("calibrated_entry_label"),
                    "tail_capture_label": behavior.get("tail_capture_label"),
                    "positive_cell_ratio": safe_float(matrix_summary.get("positive_cell_ratio")),
                    "largest_positive_cluster_ratio": safe_float(matrix_summary.get("largest_positive_cluster_ratio")),
                    "best_cell_positive_neighbor_count": safe_float(matrix_summary.get("best_cell_positive_neighbor_count")),
                }
                for key, value in axes.items():
                    row[key] = None if value is None else value * 100.0
                for key, value in components.items():
                    row["component_" + key.replace(".", "__")] = None if value is None else value * 100.0
                for key, value in raws.items():
                    row["raw_" + key.replace(".", "__")] = value
                for key, value in v2.items():
                    row[key] = None if value is None else value * 100.0
                for item in diagnostics.get("ride.loss_streak_resilience") or []:
                    label = str(item.get("label") or "").lower().replace(" ", "_")
                    if label:
                        row["loss_diag_" + label] = safe_float(item.get("value"))
                rows.append(row)
    return rows, errors


def parse_full36_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    full_rows: list[dict[str, Any]] = []
    for source in rows:
        artifact_dir = Path(str(source.get("artifact_dir") or ""))
        result_path = artifact_dir / "full-backtest-36mo-result.json"
        if not result_path.exists():
            continue
        payload = load_json(result_path)
        aggregate = nested(payload, ["data", "aggregate"])
        if not isinstance(aggregate, dict):
            aggregate = nested(payload, ["data"])
        if not isinstance(aggregate, dict):
            continue
        score_lab = aggregate.get("score_lab") or aggregate.get("score_lab_payload") or {}
        axes, components, raws, diagnostics = scorelab_maps(score_lab if isinstance(score_lab, dict) else {})
        behavior = aggregate.get("behavior_summary") or {}
        best_cell = aggregate.get("best_cell") or {}
        path_metrics = aggregate.get("best_cell_path_metrics") or {}
        matrix_summary = aggregate.get("matrix_summary") or {}
        market_window = aggregate.get("market_data_window") or {}
        spacing = entry_spacing_score(behavior)
        v2 = build_v2_draft(components, spacing)
        score_36 = safe_float(score_lab.get("score")) if isinstance(score_lab, dict) else safe_float(aggregate.get("score_lab"))
        row = {
            "run_id": source.get("run_id"),
            "attempt_id": source.get("attempt_id"),
            "sequence": source.get("sequence"),
            "candidate_name": source.get("candidate_name"),
            "artifact_dir": source.get("artifact_dir"),
            "profile_ref": source.get("profile_ref"),
            "profile_path": source.get("profile_path"),
            "score_short_window": source.get("score_v1"),
            "score_36m": score_36,
            "score_v2_draft_36m": None if v2["score_v2_draft"] is None else v2["score_v2_draft"] * 100.0,
            "score_delta_v2_minus_36m": None if score_36 is None or v2["score_v2_draft"] is None else v2["score_v2_draft"] * 100.0 - score_36,
            "score_delta_36m_minus_short": None if score_36 is None or safe_float(source.get("score_v1")) is None else score_36 - float(source["score_v1"]),
            "scorelab_version_36m": score_lab.get("version") if isinstance(score_lab, dict) else None,
            "analysis_status_36m": aggregate.get("analysis_status"),
            "effective_window_months_36m": safe_float(market_window.get("effective_window_months")),
            "timeframe": aggregate.get("timeframe") or source.get("timeframe"),
            "instrument": aggregate.get("instrument"),
            "profile_instruments": source.get("profile_instruments"),
            "instrument_count": source.get("instrument_count"),
            "indicator_count": source.get("indicator_count"),
            "entry_indicator_count": source.get("entry_indicator_count"),
            "new_trigger_indicator_count": source.get("new_trigger_indicator_count"),
            "indicator_roles": source.get("indicator_roles"),
            "indicator_ids": source.get("indicator_ids"),
            "indicator_labels": source.get("indicator_labels"),
            "best_reward_multiple": safe_float(best_cell.get("reward_multiple")),
            "best_stop_loss_percent": safe_float(best_cell.get("stop_loss_percent")),
            "best_take_profit_percent": safe_float(best_cell.get("take_profit_percent")),
            "best_expectancy_r": safe_float(best_cell.get("avg_net_r_per_closed_trade")),
            "best_profit_factor": safe_float(best_cell.get("profit_factor")),
            "resolved_trades": safe_float(best_cell.get("resolved_trades") or path_metrics.get("trade_count")),
            "trades_per_month": (
                safe_float(best_cell.get("resolved_trades") or path_metrics.get("trade_count")) / safe_float(market_window.get("effective_window_months"))
                if safe_float(best_cell.get("resolved_trades") or path_metrics.get("trade_count")) is not None
                and safe_float(market_window.get("effective_window_months")) not in {None, 0.0}
                else None
            ),
            "final_equity_r": safe_float(path_metrics.get("final_equity_r")),
            "max_drawdown_r": safe_float(path_metrics.get("max_drawdown_r")),
            "ulcer_index_r": safe_float(path_metrics.get("ulcer_index_r")),
            "time_under_water_ratio": safe_float(path_metrics.get("time_under_water_ratio")),
            "psr": safe_float(path_metrics.get("psr")),
            "k_ratio": safe_float(path_metrics.get("k_ratio")),
            "win_rate": safe_float(path_metrics.get("win_rate")),
            "max_consecutive_losses": safe_float(path_metrics.get("max_consecutive_losses")),
            "expected_longest_loss_streak": safe_float(path_metrics.get("expected_longest_loss_streak")),
            "avg_loss_streak": safe_float(path_metrics.get("avg_loss_streak")),
            "p90_loss_streak": safe_float(path_metrics.get("p90_loss_streak")),
            "signal_count": safe_float(aggregate.get("signal_count")),
            "signal_selectivity": behavior.get("signal_selectivity"),
            "signal_coverage_ratio": safe_float(behavior.get("signal_coverage_ratio")),
            "bars_per_signal": safe_float(behavior.get("bars_per_signal")),
            "max_consecutive_signal_run": safe_float(behavior.get("max_consecutive_signal_run")),
            "median_signal_run": safe_float(behavior.get("median_signal_run")),
            "trigger_indicator_count": safe_float(behavior.get("trigger_indicator_count")),
            "entry_spacing_score_experimental": None if spacing is None else spacing * 100.0,
            "state_only_profile": behavior.get("state_only_profile"),
            "calibrated_entry_label": behavior.get("calibrated_entry_label"),
            "tail_capture_label": behavior.get("tail_capture_label"),
            "positive_cell_ratio": safe_float(matrix_summary.get("positive_cell_ratio")),
            "largest_positive_cluster_ratio": safe_float(matrix_summary.get("largest_positive_cluster_ratio")),
            "best_cell_positive_neighbor_count": safe_float(matrix_summary.get("best_cell_positive_neighbor_count")),
        }
        for key, value in axes.items():
            row[key] = None if value is None else value * 100.0
        for key, value in components.items():
            row["component_" + key.replace(".", "__")] = None if value is None else value * 100.0
        for key, value in raws.items():
            row["raw_" + key.replace(".", "__")] = value
        for key, value in v2.items():
            row[key] = None if value is None else value * 100.0
        for item in diagnostics.get("ride.loss_streak_resilience") or []:
            label = str(item.get("label") or "").lower().replace(" ", "_")
            if label:
                row["loss_diag_" + label] = safe_float(item.get("value"))
        full_rows.append(row)
    return full_rows


def write_csv(path: Path, rows: list[dict[str, Any]], preferred: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    preferred = preferred or []
    columns = preferred + sorted({key for row in rows for key in row} - set(preferred))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def aggregate_row(key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    high = [row for row in rows if (safe_float(row.get("score_v1")) or -1.0) >= 60.0]
    return {
        "key": key,
        "attempts": len(rows),
        "high60_count": len(high),
        "high60_rate": len(high) / len(rows) if rows else None,
        "score_mean": stats(rows, "score_v1")["mean"],
        "score_median": stats(rows, "score_v1")["median"],
        "score_p90": stats(rows, "score_v1")["p90"],
        "score_v2_mean": stats(rows, "score_v2_draft")["mean"],
        "entry_spacing_mean": stats(rows, "entry_spacing_score_experimental")["mean"],
        "ride_mean": stats(rows, "axis_ride")["mean"],
        "stability_mean": stats(rows, "axis_stability")["mean"],
        "viability_mean": stats(rows, "axis_viability")["mean"],
        "avg_signal_coverage": stats(rows, "signal_coverage_ratio")["mean"],
        "avg_bars_per_signal": stats(rows, "bars_per_signal")["mean"],
        "avg_max_signal_run": stats(rows, "max_consecutive_signal_run")["mean"],
        "avg_reward_multiple": stats(rows, "best_reward_multiple")["mean"],
        "avg_stop_loss_percent": stats(rows, "best_stop_loss_percent")["mean"],
    }


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["|" + "|".join(headers) + "|", "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        out.append("|" + "|".join(str(value).replace("|", "/") for value in row) + "|")
    return "\n".join(out)


def fmt(value: Any, digits: int = 2) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.{digits}f}"


def entry_bucket(row: dict[str, Any]) -> str:
    max_run = safe_float(row.get("max_consecutive_signal_run")) or 0.0
    coverage = safe_float(row.get("signal_coverage_ratio")) or 0.0
    bars_per_signal = safe_float(row.get("bars_per_signal"))
    if coverage == 0 or bars_per_signal is None:
        return "no_signals"
    if max_run >= 20 or coverage >= 0.20 or bars_per_signal <= 5:
        return "clustered_dense"
    if max_run >= 6 or coverage >= 0.08 or bars_per_signal <= 12:
        return "moderate_cluster"
    return "spaced_selective"


def main() -> None:
    rows, errors = parse_attempts()
    full_rows = parse_full36_rows(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    preferred = [
        "run_id",
        "attempt_id",
        "sequence",
        "candidate_name",
        "score_v1",
        "score_v2_draft",
        "score_delta_v2_minus_v1",
        "requested_horizon_months",
        "effective_window_months",
        "timeframe",
        "profile_instruments",
        "instrument_count",
        "indicator_count",
        "entry_indicator_count",
        "new_trigger_indicator_count",
        "indicator_roles",
        "indicator_ids",
        "indicator_labels",
        "best_reward_multiple",
        "best_stop_loss_percent",
        "resolved_trades",
        "trades_per_month",
        "axis_proof",
        "axis_edge",
        "axis_ride",
        "axis_stability",
        "axis_viability",
        "entry_spacing_score_experimental",
        "signal_coverage_ratio",
        "bars_per_signal",
        "max_consecutive_signal_run",
        "median_signal_run",
        "profile_ref",
        "artifact_dir",
        "profile_path",
    ]
    write_csv(OUT / "attempt_metrics.csv", rows, preferred)
    write_csv(
        OUT / "full36_metrics.csv",
        full_rows,
        [
            "run_id",
            "attempt_id",
            "candidate_name",
            "score_short_window",
            "score_36m",
            "score_v2_draft_36m",
            "score_delta_v2_minus_36m",
            "score_delta_36m_minus_short",
            "effective_window_months_36m",
            "timeframe",
            "profile_instruments",
            "indicator_count",
            "entry_indicator_count",
            "new_trigger_indicator_count",
            "indicator_roles",
            "indicator_ids",
            "best_reward_multiple",
            "best_stop_loss_percent",
            "resolved_trades",
            "trades_per_month",
            "max_consecutive_losses",
            "avg_loss_streak",
            "p90_loss_streak",
            "win_rate",
            "axis_proof",
            "axis_edge",
            "axis_ride",
            "axis_stability",
            "axis_viability",
            "entry_spacing_score_experimental",
            "signal_coverage_ratio",
            "bars_per_signal",
            "max_consecutive_signal_run",
            "profile_ref",
            "artifact_dir",
        ],
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "attempt_count": len(rows),
        "run_count": len({row.get("run_id") for row in rows if row.get("run_id")}),
        "full_backtest_36mo_result_count": sum(1 for _ in RUNS.rglob("full-backtest-36mo-result.json")),
        "full36_parsed_count": len(full_rows),
        "parse_errors": errors,
        "score_v1": stats(rows, "score_v1"),
        "score_v2_draft": stats(rows, "score_v2_draft"),
        "score_delta_v2_minus_v1": stats(rows, "score_delta_v2_minus_v1"),
        "high_score_counts": {str(threshold): sum(1 for row in rows if (safe_float(row.get("score_v1")) or -1.0) >= threshold) for threshold in [40, 50, 60, 70, 80]},
        "axis_stats": {field: stats(rows, field) for field in ["axis_proof", "axis_edge", "axis_ride", "axis_stability", "axis_viability"]},
        "entry_spacing_stats": stats(rows, "entry_spacing_score_experimental"),
        "signal_stats": {field: stats(rows, field) for field in ["signal_coverage_ratio", "bars_per_signal", "max_consecutive_signal_run", "median_signal_run"]},
        "loss_streak_stats": {field: stats(rows, field) for field in ["loss_diag_max_losses", "loss_diag_expected_longest", "loss_diag_avg_streak", "loss_diag_p90_streak", "loss_diag_win_rate"]},
        "full36": {
            "score_36m": stats(full_rows, "score_36m"),
            "score_v2_draft_36m": stats(full_rows, "score_v2_draft_36m"),
            "score_delta_v2_minus_36m": stats(full_rows, "score_delta_v2_minus_36m"),
            "score_delta_36m_minus_short": stats(full_rows, "score_delta_36m_minus_short"),
            "axis_stats": {field: stats(full_rows, field) for field in ["axis_proof", "axis_edge", "axis_ride", "axis_stability", "axis_viability"]},
            "entry_spacing_stats": stats(full_rows, "entry_spacing_score_experimental"),
            "loss_streak_stats": {field: stats(full_rows, field) for field in ["max_consecutive_losses", "expected_longest_loss_streak", "avg_loss_streak", "p90_loss_streak", "win_rate"]},
            "high_score_counts": {str(threshold): sum(1 for row in full_rows if (safe_float(row.get("score_36m")) or -1.0) >= threshold) for threshold in [40, 50, 60, 70, 80]},
        },
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    metric_fields = [
        key
        for key in sorted({key for row in rows for key in row})
        if key.startswith("axis_") or key.startswith("component_") or key in {
            "entry_spacing_score_experimental",
            "proof_v2",
            "edge_v2",
            "ride_v2",
            "stability_v2",
            "viability_v2",
            "score_v2_draft",
        }
    ]
    component_stats = []
    for field in metric_fields:
        item = {"field": field, **stats(rows, field)}
        item["pearson_to_score_v1"] = correlation(rows, field, "score_v1")
        item["pearson_to_score_v2_draft"] = correlation(rows, field, "score_v2_draft")
        component_stats.append(item)
    write_csv(OUT / "score_component_stats.csv", component_stats)

    full_metric_fields = [
        key
        for key in sorted({key for row in full_rows for key in row})
        if key.startswith("axis_") or key.startswith("component_") or key in {
            "entry_spacing_score_experimental",
            "proof_v2",
            "edge_v2",
            "ride_v2",
            "stability_v2",
            "viability_v2",
            "score_v2_draft_36m",
        }
    ]
    full_component_stats = []
    for field in full_metric_fields:
        item = {"field": field, **stats(full_rows, field)}
        item["pearson_to_score_36m"] = correlation(full_rows, field, "score_36m")
        item["pearson_to_score_v2_draft_36m"] = correlation(full_rows, field, "score_v2_draft_36m")
        full_component_stats.append(item)
    write_csv(OUT / "full36_component_stats.csv", full_component_stats)

    metadata = indicator_metadata()
    by_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_combo: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_entry_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        indicator_ids = [item for item in str(row.get("indicator_ids") or "").split(",") if item]
        combo_key = "+".join(indicator_ids) if indicator_ids else "(none)"
        role_key = str(row.get("indicator_roles") or "(none)") or "(none)"
        by_combo[combo_key].append(row)
        by_role[role_key].append(row)
        by_entry_bucket[entry_bucket(row)].append(row)
        for indicator_id in indicator_ids:
            by_indicator[indicator_id].append(row)

    indicator_rows = []
    for indicator_id, group in sorted(by_indicator.items(), key=lambda item: (-len(item[1]), item[0])):
        item = aggregate_row(indicator_id, group)
        item.update(metadata.get(indicator_id, {}))
        indicator_rows.append(item)
    write_csv(OUT / "indicator_breakdown.csv", indicator_rows)
    write_csv(OUT / "indicator_combo_breakdown.csv", [aggregate_row(key, group) for key, group in sorted(by_combo.items()) if len(group) >= 2])
    write_csv(OUT / "indicator_role_breakdown.csv", [aggregate_row(key, group) for key, group in sorted(by_role.items())])
    write_csv(OUT / "entry_spacing_buckets.csv", [aggregate_row(key, group) for key, group in sorted(by_entry_bucket.items())])

    full_by_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    full_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
    full_by_entry_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in full_rows:
        full_by_role[str(row.get("indicator_roles") or "(none)") or "(none)"].append(row)
        full_by_entry_bucket[entry_bucket(row)].append(row)
        for indicator_id in [item for item in str(row.get("indicator_ids") or "").split(",") if item]:
            full_by_indicator[indicator_id].append(row)

    def full_aggregate_row(key: str, group: list[dict[str, Any]]) -> dict[str, Any]:
        high = [row for row in group if (safe_float(row.get("score_36m")) or -1.0) >= 60.0]
        return {
            "key": key,
            "attempts": len(group),
            "high60_count": len(high),
            "high60_rate": len(high) / len(group) if group else None,
            "score_mean": stats(group, "score_36m")["mean"],
            "score_median": stats(group, "score_36m")["median"],
            "score_p90": stats(group, "score_36m")["p90"],
            "score_v2_mean": stats(group, "score_v2_draft_36m")["mean"],
            "entry_spacing_mean": stats(group, "entry_spacing_score_experimental")["mean"],
            "ride_mean": stats(group, "axis_ride")["mean"],
            "stability_mean": stats(group, "axis_stability")["mean"],
            "viability_mean": stats(group, "axis_viability")["mean"],
            "avg_signal_coverage": stats(group, "signal_coverage_ratio")["mean"],
            "avg_bars_per_signal": stats(group, "bars_per_signal")["mean"],
            "avg_max_signal_run": stats(group, "max_consecutive_signal_run")["mean"],
            "avg_reward_multiple": stats(group, "best_reward_multiple")["mean"],
            "avg_stop_loss_percent": stats(group, "best_stop_loss_percent")["mean"],
        }

    full_indicator_rows = []
    for indicator_id, group in sorted(full_by_indicator.items(), key=lambda item: (-len(item[1]), item[0])):
        item = full_aggregate_row(indicator_id, group)
        item.update(metadata.get(indicator_id, {}))
        full_indicator_rows.append(item)
    write_csv(OUT / "full36_indicator_breakdown.csv", full_indicator_rows)
    write_csv(OUT / "full36_indicator_role_breakdown.csv", [full_aggregate_row(key, group) for key, group in sorted(full_by_role.items())])
    write_csv(OUT / "full36_entry_spacing_buckets.csv", [full_aggregate_row(key, group) for key, group in sorted(full_by_entry_bucket.items())])

    def top(rows_in: list[dict[str, Any]], field: str, limit: int = 25, reverse: bool = True) -> list[dict[str, Any]]:
        valid = [row for row in rows_in if safe_float(row.get(field)) is not None]
        return sorted(valid, key=lambda row: safe_float(row.get(field)) or 0.0, reverse=reverse)[:limit]

    shortlist = []
    buckets = {
        "top_v1": top(rows, "score_v1"),
        "top_v2_draft": top(rows, "score_v2_draft"),
        "largest_v2_drop": top(rows, "score_delta_v2_minus_v1", reverse=False),
        "largest_v2_gain": top(rows, "score_delta_v2_minus_v1"),
        "high_v1_bad_spacing": top([row for row in rows if (safe_float(row.get("score_v1")) or -1.0) >= 55], "entry_spacing_score_experimental", reverse=False),
        "high_v1_bad_loss_streak": top([row for row in rows if (safe_float(row.get("score_v1")) or -1.0) >= 55], "component_ride__loss_streak_resilience", reverse=False),
    }
    keep = [
        "run_id",
        "attempt_id",
        "candidate_name",
        "score_v1",
        "score_v2_draft",
        "score_delta_v2_minus_v1",
        "axis_proof",
        "axis_edge",
        "axis_ride",
        "axis_stability",
        "axis_viability",
        "ride_v2",
        "viability_v2",
        "entry_spacing_score_experimental",
        "component_ride__loss_streak_resilience",
        "loss_diag_max_losses",
        "best_reward_multiple",
        "best_stop_loss_percent",
        "signal_coverage_ratio",
        "bars_per_signal",
        "max_consecutive_signal_run",
        "indicator_ids",
        "indicator_labels",
        "profile_ref",
        "artifact_dir",
    ]
    for bucket_name, bucket_rows in buckets.items():
        for row in bucket_rows:
            shortlist.append({"bucket": bucket_name, **{key: row.get(key) for key in keep}})
    write_csv(OUT / "review_shortlist.csv", shortlist, ["bucket", *keep])

    full_shortlist = []
    full_buckets = {
        "top_36m": top(full_rows, "score_36m"),
        "top_v2_draft_36m": top(full_rows, "score_v2_draft_36m"),
        "largest_v2_drop_36m": top(full_rows, "score_delta_v2_minus_36m", reverse=False),
        "largest_v2_gain_36m": top(full_rows, "score_delta_v2_minus_36m"),
        "largest_36m_decay_from_short": top(full_rows, "score_delta_36m_minus_short", reverse=False),
        "high_36m_bad_loss_streak": top([row for row in full_rows if (safe_float(row.get("score_36m")) or -1.0) >= 60], "component_ride__loss_streak_resilience", reverse=False),
    }
    full_keep = [
        "run_id",
        "attempt_id",
        "candidate_name",
        "score_short_window",
        "score_36m",
        "score_v2_draft_36m",
        "score_delta_v2_minus_36m",
        "score_delta_36m_minus_short",
        "axis_proof",
        "axis_edge",
        "axis_ride",
        "axis_stability",
        "axis_viability",
        "ride_v2",
        "viability_v2",
        "entry_spacing_score_experimental",
        "component_ride__loss_streak_resilience",
        "max_consecutive_losses",
        "avg_loss_streak",
        "best_reward_multiple",
        "best_stop_loss_percent",
        "signal_coverage_ratio",
        "bars_per_signal",
        "max_consecutive_signal_run",
        "indicator_ids",
        "indicator_labels",
        "profile_ref",
        "artifact_dir",
    ]
    for bucket_name, bucket_rows in full_buckets.items():
        for row in bucket_rows:
            full_shortlist.append({"bucket": bucket_name, **{key: row.get(key) for key in full_keep}})
    write_csv(OUT / "full36_review_shortlist.csv", full_shortlist, ["bucket", *full_keep])

    top_indicators = sorted(
        [row for row in indicator_rows if row["attempts"] >= 8],
        key=lambda row: safe_float(row.get("score_p90")) or -1.0,
        reverse=True,
    )[:12]
    role_rows = [aggregate_row(key, group) for key, group in sorted(by_role.items(), key=lambda item: (-len(item[1]), item[0]))][:10]
    entry_bucket_rows = [aggregate_row(key, group) for key, group in sorted(by_entry_bucket.items())]
    full_entry_bucket_rows = [full_aggregate_row(key, group) for key, group in sorted(full_by_entry_bucket.items())]
    full_top_indicators = sorted(
        [row for row in full_indicator_rows if row["attempts"] >= 8],
        key=lambda row: safe_float(row.get("score_p90")) or -1.0,
        reverse=True,
    )[:12]
    high_bad_spacing = [
        row
        for row in rows
        if (safe_float(row.get("score_v1")) or -1.0) >= 55
        and (safe_float(row.get("entry_spacing_score_experimental")) or 999.0) < 50
    ]
    high_12r = [
        row
        for row in rows
        if (safe_float(row.get("score_v1")) or -1.0) >= 55
        and (safe_float(row.get("best_reward_multiple")) or 0.0) >= 10
    ]

    findings = [
        "# Scorelab V2 Corpus Findings",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        "## Corpus Shape",
        "",
        f"- Parsed {len(rows)} attempts across {summary['run_count']} runs. Parse anomalies: {len(errors)}.",
        f"- Attempt-local full 36mo result files present: {summary['full_backtest_36mo_result_count']}; parsed into this package: {len(full_rows)}.",
        f"- Score v1 distribution: median {fmt(summary['score_v1']['median'])}; p75 {fmt(summary['score_v1']['p75'])}; p90 {fmt(summary['score_v1']['p90'])}; max {fmt(summary['score_v1']['max'])}.",
        f"- High score counts: >=60: {summary['high_score_counts']['60']}; >=70: {summary['high_score_counts']['70']}; >=80: {summary['high_score_counts']['80']}.",
        f"- 36mo score distribution: median {fmt(summary['full36']['score_36m']['median'])}; p75 {fmt(summary['full36']['score_36m']['p75'])}; p90 {fmt(summary['full36']['score_36m']['p90'])}; max {fmt(summary['full36']['score_36m']['max'])}.",
        f"- 36mo high score counts: >=60: {summary['full36']['high_score_counts']['60']}; >=70: {summary['full36']['high_score_counts']['70']}; >=80: {summary['full36']['high_score_counts']['80']}.",
        "",
        "## Axis Distribution: Short Window",
        "",
        md_table(
            ["Axis", "Median", "P75", "P90", "Mean"],
            [
                [
                    axis.replace("axis_", ""),
                    fmt(summary["axis_stats"][axis]["median"]),
                    fmt(summary["axis_stats"][axis]["p75"]),
                    fmt(summary["axis_stats"][axis]["p90"]),
                    fmt(summary["axis_stats"][axis]["mean"]),
                ]
                for axis in ["axis_proof", "axis_edge", "axis_ride", "axis_stability", "axis_viability"]
            ],
        ),
        "",
        "## Axis Distribution: Full 36mo",
        "",
        md_table(
            ["Axis", "Median", "P75", "P90", "Mean"],
            [
                [
                    axis.replace("axis_", ""),
                    fmt(summary["full36"]["axis_stats"][axis]["median"]),
                    fmt(summary["full36"]["axis_stats"][axis]["p75"]),
                    fmt(summary["full36"]["axis_stats"][axis]["p90"]),
                    fmt(summary["full36"]["axis_stats"][axis]["mean"]),
                ]
                for axis in ["axis_proof", "axis_edge", "axis_ride", "axis_stability", "axis_viability"]
            ],
        ),
        "",
        "## Entry Spacing / Signal Clustering: Full 36mo",
        "",
        md_table(
            ["Bucket", "Attempts", "High>=60", "Mean score", "Mean spacing", "Signal cov %", "Bars/signal", "Max run"],
            [
                [
                    row["key"],
                    row["attempts"],
                    row["high60_count"],
                    fmt(row["score_mean"]),
                    fmt(row["entry_spacing_mean"]),
                    fmt((row["avg_signal_coverage"] or 0) * 100),
                    fmt(row["avg_bars_per_signal"]),
                    fmt(row["avg_max_signal_run"]),
                ]
                for row in full_entry_bucket_rows
            ],
        ),
        "",
        f"- Short-window high-scoring attempts with draft entry-spacing score under 50: {len(high_bad_spacing)}.",
        f"- Short-window high-scoring attempts using reward multiple >=10R: {len(high_12r)}.",
        "",
        "## Indicator Signals: Full 36mo",
        "",
        md_table(
            ["Indicator", "Role", "Attempts", "High>=60 %", "Median", "P90", "Spacing", "Max signal run"],
            [
                [
                    row.get("name") or row["key"],
                    row.get("role") or "",
                    row["attempts"],
                    fmt((row["high60_rate"] or 0) * 100),
                    fmt(row["score_median"]),
                    fmt(row["score_p90"]),
                    fmt(row["entry_spacing_mean"]),
                    fmt(row["avg_max_signal_run"]),
                ]
                for row in full_top_indicators
            ],
        ),
        "",
        "## Indicator Role Mix",
        "",
        md_table(
            ["Role mix", "Attempts", "High>=60 %", "Median", "Spacing", "Bars/signal"],
            [
                [
                    row["key"],
                    row["attempts"],
                    fmt((row["high60_rate"] or 0) * 100),
                    fmt(row["score_median"]),
                    fmt(row["entry_spacing_mean"]),
                    fmt(row["avg_bars_per_signal"]),
                ]
                for row in role_rows
            ],
        ),
        "",
        "## Raw Files",
        "",
        "- attempt_metrics.csv",
        "- score_component_stats.csv",
        "- indicator_breakdown.csv",
        "- indicator_combo_breakdown.csv",
        "- indicator_role_breakdown.csv",
        "- entry_spacing_buckets.csv",
        "- review_shortlist.csv",
        "- full36_metrics.csv",
        "- full36_component_stats.csv",
        "- full36_indicator_breakdown.csv",
        "- full36_indicator_role_breakdown.csv",
        "- full36_entry_spacing_buckets.csv",
        "- full36_review_shortlist.csv",
        "- full36_review_shortlist.md",
        "- summary.json",
    ]
    (OUT / "findings.md").write_text("\n".join(findings) + "\n", encoding="utf-8")

    recommendations = [
        "# Scorelab V2 Recommendations / Interpretation",
        "",
        "## Proposed Shape",
        "",
        "Keep the five-axis top-level geometric mean. The current axis names still work: Proof, Edge, Ride, Stability, Viability. V2 should mostly be a tier-3 repack plus one new scored component, not a new philosophy.",
        "",
        "## Tier-2 / Tier-3 Assembly Changes",
        "",
        "- **Proof:** weighted geometric mean of belief and trade support, with belief slightly heavier. Belief is the statistical gate; trade support is the sample-size modifier.",
        "- **Edge:** keep expectancy and profit factor as the core. Treat R/month productivity as a lighter bridge metric because it overlaps with viability cadence.",
        "- **Ride:** group drawdown, ulcer, and underwater into `capital pain`; keep loss-streak resilience as its own heavier subscore; group smoothness, temporal breadth, and spike resistance into `accrual quality`. This stops seven ride components from diluting the loss-streak metric.",
        "- **Stability:** group positive cell area + largest cluster as `global pocket`; group neighbor support + perturbation + best-neighbors as `local pocket`; keep robust-cell expectancy as economic support.",
        "- **Viability:** keep cadence, longevity, and instrument consensus. Add `entry spacing hygiene` from behavior_summary after threshold review. This belongs in viability because it describes whether the signal stream is operationally usable before exits are tested.",
        "",
        "## Draft Formula Tested Here",
        "",
        "```text",
        "proof_v2 = wgm(belief 1.5, trade_support 1.0)",
        "edge_v2 = wgm(expectancy 1.2, profit_factor 1.0, edge_rate 0.55)",
        "ride_v2 = wgm(capital_pain 1.25, loss_streak_resilience 1.75, accrual_quality 1.0)",
        "stability_v2 = wgm(global_pocket 1.0, local_pocket 1.25, robust_expectancy 1.0)",
        "viability_v2 = wgm(cadence 1.0, longevity 1.35, instrument_consensus 1.0, entry_spacing_hygiene 1.25)",
        "score_v2 = geomean(proof_v2, edge_v2, ride_v2, stability_v2, viability_v2)",
        "```",
        "",
        "## What Looks Useful",
        "",
        "- Loss-streak resilience should stay and should become more influential inside Ride. In the full 36mo corpus, high-scoring 10R+ candidates still commonly carry very low loss-streak resilience, so the current v1 score is still too willing to forgive painful sequences when pocket/stability/proof are strong.",
        "- Entry-spacing hygiene looks like the missing metric for the bar-to-bar clustering problem. It is not the same as cadence: cadence can be acceptable while raw signals arrive in clumps. This must be gated on actual signal/trade support so no-signal profiles do not receive accidental credit.",
        "- First-event trigger indicators are strategically important because they convert broad state into discrete entries. In the full 36mo slice, profiles with two entry indicators had a materially higher high-score hit rate than profiles with zero or one, while also producing cleaner spacing.",
        "",
        "## What Looks Redundant Or Needs Repacking",
        "",
        "- Drawdown, ulcer, and underwater are complementary but same-family; group them before they vote.",
        "- Positive cell area and largest cluster are also same-family; group them under global pocket stability.",
        "- Edge-rate overlaps with cadence. Keep it light unless we intentionally want productivity to dominate.",
        "",
        "## Indicator / Construct Gaps",
        "",
        "- Add more first-event triggers: retest confirmation, pullback-resume, range-break-and-hold, volatility expansion after compression, failed-breakdown/failed-breakout, and session/killzone filters.",
        "- Keep oscillators and trend filters, but prefer them as context/setup inputs rather than the only low-timeframe entry source.",
        "",
        "## Immediate Next Step",
        "",
        "Use `full36_review_shortlist.csv` to inspect top 36mo, top draft-v2, largest draft drops, largest short-to-36mo decays, and high-36mo bad-loss-streak cases in the UI. Then implement a narrow scorelab_v2 branch with grouped Ride/Stability plus entry-spacing hygiene and run another fresh explorer slice.",
    ]
    (OUT / "recommendations.md").write_text("\n".join(recommendations) + "\n", encoding="utf-8")

    short_md = [
        "# Review Shortlist",
        "",
        "These are candidates to inspect manually. See `review_shortlist.csv` for full paths and profile refs.",
        "",
    ]
    for bucket_name in ["top_v1", "top_v2_draft", "largest_v2_drop", "high_v1_bad_spacing", "high_v1_bad_loss_streak"]:
        short_md.append(f"## {bucket_name}")
        subset = [row for row in shortlist if row.get("bucket") == bucket_name][:10]
        short_md.append(
            md_table(
                ["score", "v2", "delta", "candidate", "profile_ref", "reward", "max_run", "spacing", "loss_streak", "indicators"],
                [
                    [
                        fmt(row.get("score_v1")),
                        fmt(row.get("score_v2_draft")),
                        fmt(row.get("score_delta_v2_minus_v1")),
                        row.get("candidate_name"),
                        row.get("profile_ref"),
                        fmt(row.get("best_reward_multiple")),
                        fmt(row.get("max_consecutive_signal_run")),
                        fmt(row.get("entry_spacing_score_experimental")),
                        fmt(row.get("component_ride__loss_streak_resilience")),
                        row.get("indicator_ids"),
                    ]
                    for row in subset
                ],
            )
        )
        short_md.append("")
    (OUT / "review_shortlist.md").write_text("\n".join(short_md) + "\n", encoding="utf-8")

    full_short_md = [
        "# Full 36mo Review Shortlist",
        "",
        "These are candidates to inspect manually from completed full-backtest artifacts. See `full36_review_shortlist.csv` for full paths and profile refs.",
        "",
    ]
    for bucket_name in ["top_36m", "top_v2_draft_36m", "largest_v2_drop_36m", "largest_36m_decay_from_short", "high_36m_bad_loss_streak"]:
        full_short_md.append(f"## {bucket_name}")
        subset = [row for row in full_shortlist if row.get("bucket") == bucket_name][:10]
        full_short_md.append(
            md_table(
                ["36m", "short", "v2", "delta", "candidate", "profile_ref", "reward", "losses", "loss_streak", "indicators"],
                [
                    [
                        fmt(row.get("score_36m")),
                        fmt(row.get("score_short_window")),
                        fmt(row.get("score_v2_draft_36m")),
                        fmt(row.get("score_delta_v2_minus_36m")),
                        row.get("candidate_name"),
                        row.get("profile_ref"),
                        fmt(row.get("best_reward_multiple")),
                        fmt(row.get("max_consecutive_losses")),
                        fmt(row.get("component_ride__loss_streak_resilience")),
                        row.get("indicator_ids"),
                    ]
                    for row in subset
                ],
            )
        )
        full_short_md.append("")
    (OUT / "full36_review_shortlist.md").write_text("\n".join(full_short_md) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "out": str(OUT),
                "attempts": len(rows),
                "runs": summary["run_count"],
                "parse_errors": len(errors),
                "files": sorted(path.name for path in OUT.iterdir()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
