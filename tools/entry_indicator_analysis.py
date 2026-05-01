from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "runs" / "derived" / "scorelab-v2-analysis-20260430"
FULL36 = OUT / "full36_metrics.csv"
TD_ROOT = Path("C:/repos/Trading-Dashboard")
INDICATORS_PATH = TD_ROOT / "shared" / "constants" / "indicators.json"


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def stats(values: list[float | None]) -> dict[str, Any]:
    nums = sorted(v for v in values if v is not None)
    if not nums:
        return {"count": 0, "mean": None, "median": None, "p90": None, "min": None, "max": None}
    return {
        "count": len(nums),
        "mean": statistics.mean(nums),
        "median": nums[len(nums) // 2],
        "p90": nums[int((len(nums) - 1) * 0.9)],
        "min": nums[0],
        "max": nums[-1],
    }


def load_indicator_meta() -> dict[str, dict[str, Any]]:
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
        }
    return result


def profile_indicators(path: str) -> list[dict[str, Any]]:
    payload = load_json(Path(path))
    profile = payload.get("profile") if isinstance(payload, dict) else None
    indicators = profile.get("indicators") if isinstance(profile, dict) else None
    return indicators if isinstance(indicators, list) else []


def nested(payload: Any, *keys: str) -> Any:
    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def aggregate_row(key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [safe_float(row.get("score_36m")) for row in rows]
    high60 = [row for row in rows if (safe_float(row.get("score_36m")) or -1) >= 60]
    high70 = [row for row in rows if (safe_float(row.get("score_36m")) or -1) >= 70]
    return {
        "key": key,
        "attempts": len(rows),
        "high60_count": len(high60),
        "high60_rate": len(high60) / len(rows) if rows else None,
        "high70_count": len(high70),
        "high70_rate": len(high70) / len(rows) if rows else None,
        "score_mean": stats(scores)["mean"],
        "score_median": stats(scores)["median"],
        "score_p90": stats(scores)["p90"],
        "entry_spacing_mean": stats([safe_float(row.get("entry_spacing_score_experimental")) for row in rows])["mean"],
        "max_signal_run_mean": stats([safe_float(row.get("max_consecutive_signal_run")) for row in rows])["mean"],
        "bars_per_signal_median": stats([safe_float(row.get("bars_per_signal")) for row in rows])["median"],
        "loss_streak_resilience_median": stats([safe_float(row.get("component_ride__loss_streak_resilience")) for row in rows])["median"],
        "reward_multiple_median": stats([safe_float(row.get("best_reward_multiple")) for row in rows])["median"],
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def fmt(value: Any, digits: int = 2) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.{digits}f}"


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["|" + "|".join(headers) + "|", "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        out.append("|" + "|".join(str(value).replace("|", "/") for value in row) + "|")
    return "\n".join(out)


def main() -> None:
    meta = load_indicator_meta()
    with FULL36.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    entry_rows: list[dict[str, Any]] = []
    per_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    per_indicator_lookback: dict[str, list[dict[str, Any]]] = defaultdict(list)
    per_attempt_lookback: dict[str, list[dict[str, Any]]] = defaultdict(list)
    per_entry_signature: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        indicators = profile_indicators(str(row.get("profile_path") or ""))
        entry_ids: list[str] = []
        entry_lookbacks: list[int] = []
        for indicator in indicators:
            indicator_id = str(nested(indicator, "meta", "id") or "").strip().upper()
            role = str(meta.get(indicator_id, {}).get("role") or "")
            if role != "entry":
                continue
            config = indicator.get("config") or {}
            lookback = safe_float(config.get("lookbackBars"))
            lookback_int = int(lookback) if lookback is not None else None
            entry_ids.append(indicator_id)
            if lookback_int is not None:
                entry_lookbacks.append(lookback_int)
            record = {
                **row,
                "entry_indicator_id": indicator_id,
                "entry_indicator_name": meta.get(indicator_id, {}).get("name") or indicator_id,
                "lookback_bars": lookback_int,
                "timeframe": str(config.get("timeframe") or row.get("timeframe") or "").upper(),
                "use_forming_bar": config.get("useFormingBar"),
                "is_trend_following": config.get("isTrendFollowing"),
                "weight": config.get("weight"),
                "buy_range": json.dumps((config.get("ranges") or {}).get("buy")),
                "sell_range": json.dumps((config.get("ranges") or {}).get("sell")),
            }
            entry_rows.append(record)
            per_indicator[indicator_id].append(record)
            per_indicator_lookback[f"{indicator_id}|lookback={lookback_int}"].append(record)

        if entry_ids:
            signature = "+".join(sorted(entry_ids))
            lookback_signature = "+".join(str(v) for v in sorted(entry_lookbacks)) if entry_lookbacks else "none"
            max_lb = max(entry_lookbacks) if entry_lookbacks else None
            any_lb_gt1 = any(v > 1 for v in entry_lookbacks)
        else:
            signature = "(none)"
            lookback_signature = "none"
            max_lb = None
            any_lb_gt1 = False
        attempt_record = {
            **row,
            "entry_signature": signature,
            "entry_lookback_signature": lookback_signature,
            "entry_max_lookback": max_lb,
            "entry_any_lookback_gt1": any_lb_gt1,
        }
        per_attempt_lookback[lookback_signature].append(attempt_record)
        per_entry_signature[signature].append(attempt_record)

    indicator_summary = []
    for key, group in sorted(per_indicator.items(), key=lambda item: (-len(item[1]), item[0])):
        item = aggregate_row(key, group)
        item["name"] = meta.get(key, {}).get("name") or key
        item["lookback_values"] = ",".join(str(v) for v in sorted({record.get("lookback_bars") for record in group if record.get("lookback_bars") is not None}))
        item["lookback_gt1_rate"] = sum(1 for record in group if (record.get("lookback_bars") or 0) > 1) / len(group)
        indicator_summary.append(item)

    lookback_summary = [
        aggregate_row(key, group)
        for key, group in sorted(per_attempt_lookback.items(), key=lambda item: (-len(item[1]), item[0]))
    ]
    indicator_lookback_summary = [
        aggregate_row(key, group)
        for key, group in sorted(per_indicator_lookback.items(), key=lambda item: (-len(item[1]), item[0]))
    ]
    entry_signature_summary = [
        aggregate_row(key, group)
        for key, group in sorted(per_entry_signature.items(), key=lambda item: (-len(item[1]), item[0]))
        if len(group) >= 3
    ]

    write_csv(OUT / "full36_entry_indicator_rows.csv", entry_rows)
    write_csv(OUT / "full36_entry_indicator_summary.csv", indicator_summary)
    write_csv(OUT / "full36_entry_lookback_summary.csv", lookback_summary)
    write_csv(OUT / "full36_entry_indicator_lookback_summary.csv", indicator_lookback_summary)
    write_csv(OUT / "full36_entry_signature_summary.csv", entry_signature_summary)

    top_indicators = sorted(
        [row for row in indicator_summary if int(row["attempts"]) >= 8],
        key=lambda row: safe_float(row.get("score_p90")) or -1,
        reverse=True,
    )[:14]
    best_lookbacks = sorted(
        [row for row in indicator_lookback_summary if int(row["attempts"]) >= 5],
        key=lambda row: safe_float(row.get("score_p90")) or -1,
        reverse=True,
    )[:18]
    lookback_rows = sorted(lookback_summary, key=lambda row: (-int(row["attempts"]), row["key"]))

    notes = [
        "# Full 36mo Entry Indicator Analysis",
        "",
        "## By Entry Indicator",
        "",
        md_table(
            ["Indicator", "Attempts", "High60 %", "High70 %", "Median", "P90", "Lookbacks", "LB>1 %", "Spacing", "Max run"],
            [
                [
                    row.get("name") or row["key"],
                    row["attempts"],
                    fmt((row["high60_rate"] or 0) * 100),
                    fmt((row["high70_rate"] or 0) * 100),
                    fmt(row["score_median"]),
                    fmt(row["score_p90"]),
                    row.get("lookback_values"),
                    fmt((row["lookback_gt1_rate"] or 0) * 100),
                    fmt(row["entry_spacing_mean"]),
                    fmt(row["max_signal_run_mean"]),
                ]
                for row in top_indicators
            ],
        ),
        "",
        "## By Entry Lookback Signature",
        "",
        md_table(
            ["Lookbacks", "Attempts", "High60 %", "High70 %", "Median", "P90", "Spacing", "Bars/signal", "Max run"],
            [
                [
                    row["key"],
                    row["attempts"],
                    fmt((row["high60_rate"] or 0) * 100),
                    fmt((row["high70_rate"] or 0) * 100),
                    fmt(row["score_median"]),
                    fmt(row["score_p90"]),
                    fmt(row["entry_spacing_mean"]),
                    fmt(row["bars_per_signal_median"]),
                    fmt(row["max_signal_run_mean"]),
                ]
                for row in lookback_rows
            ],
        ),
        "",
        "## Strong Indicator + Lookback Slices",
        "",
        md_table(
            ["Indicator/lookback", "Attempts", "High60 %", "High70 %", "Median", "P90", "Spacing", "Max run"],
            [
                [
                    row["key"],
                    row["attempts"],
                    fmt((row["high60_rate"] or 0) * 100),
                    fmt((row["high70_rate"] or 0) * 100),
                    fmt(row["score_median"]),
                    fmt(row["score_p90"]),
                    fmt(row["entry_spacing_mean"]),
                    fmt(row["max_signal_run_mean"]),
                ]
                for row in best_lookbacks
            ],
        ),
        "",
        "## Raw Files",
        "",
        "- full36_entry_indicator_rows.csv",
        "- full36_entry_indicator_summary.csv",
        "- full36_entry_lookback_summary.csv",
        "- full36_entry_indicator_lookback_summary.csv",
        "- full36_entry_signature_summary.csv",
    ]
    (OUT / "entry_indicator_analysis.md").write_text("\n".join(notes) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "entry_rows": len(entry_rows),
                "entry_indicators": len(indicator_summary),
                "lookback_signatures": len(lookback_summary),
                "out": str(OUT / "entry_indicator_analysis.md"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
