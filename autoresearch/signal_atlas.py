from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .fuzzfolio import CliError, FuzzfolioCli
from .indicator_atlas import (
    DEFAULT_ATLAS_DIRNAME,
    build_indicator_atlas,
    load_indicator_catalog,
)


SCHEMA_VERSION = "signal_atlas_v1"
DEFAULT_SIGNAL_ATLAS_DIRNAME = "signal-atlas"
DEFAULT_SIGNAL_ROLE = "trigger"
DEFAULT_INSTRUMENTS = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD")
DEFAULT_TIMEFRAMES = ("M5", "M15")
DEFAULT_BAR_LIMIT = 5000
DEFAULT_REPLAY_SOURCE = "system"
SIGNAL_EPSILON = 1e-9
RATE_LIMIT_MARKERS = (
    "429",
    "hourly signal replay limit",
    "rate limit",
    "rate-limit",
    "too many requests",
)


@dataclass(frozen=True)
class SignalAtlasBuildResult:
    atlas_path: Path
    csv_path: Path
    summary_path: Path
    issues_path: Path
    request_manifest_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "signal_atlas_json": str(self.atlas_path),
            "signal_atlas_csv": str(self.csv_path),
            "signal_atlas_summary_json": str(self.summary_path),
            "signal_atlas_issues_csv": str(self.issues_path),
            "request_manifest_json": str(self.request_manifest_path),
            "summary": self.summary,
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_tokens(values: list[str] | tuple[str, ...] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = _clean_upper(value)
        if token and token not in seen:
            cleaned.append(token)
            seen.add(token)
    return cleaned


def _cell_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _clean_upper(row.get("indicator_id")),
        _clean_upper(row.get("timeframe")),
        _clean_upper(row.get("instrument")),
    )


def _cell_sort_key(order: dict[tuple[str, str, str], int], row: dict[str, Any]) -> tuple[int, str, str, str]:
    key = _cell_key(row)
    return (order.get(key, 10_000_000), *key)


def _error_type(message: str) -> str:
    lowered = str(message or "").lower()
    if any(marker in lowered for marker in RATE_LIMIT_MARKERS):
        return "rate_limited"
    if "timed out" in lowered:
        return "timeout"
    if "profiles create" in lowered or "profiles" in lowered and "create" in lowered:
        return "profile_create_failed"
    if "missing_catalog_item" in lowered:
        return "missing_catalog_item"
    return "cli_error"


def _is_rate_limit_error(message: str) -> bool:
    return _error_type(message) == "rate_limited"


def _indicator_catalog_by_id(catalog_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for item in _as_list(catalog_payload.get("indicators")):
        if not isinstance(item, dict):
            continue
        indicator_id = _clean_upper(_as_dict(item.get("meta")).get("id"))
        if indicator_id:
            by_id[indicator_id] = item
    return by_id


def _atlas_rows_by_id(static_atlas_path: Path) -> dict[str, dict[str, Any]]:
    payload = _load_json(static_atlas_path)
    rows = _as_list(_as_dict(payload).get("indicators"))
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        indicator_id = _clean_upper(row.get("id"))
        if indicator_id:
            by_id[indicator_id] = row
    return by_id


def _load_resumable_rows(
    atlas_path: Path,
    *,
    expected_bar_limit: int,
    candidate_keys: set[tuple[str, str, str]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    if not atlas_path.exists():
        return {}
    try:
        payload = _load_json(atlas_path)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    previous_bar_limit = (
        _as_dict(_as_dict(payload.get("summary")).get("selection")).get("bar_limit")
    )
    if previous_bar_limit is not None and int(previous_bar_limit) != int(expected_bar_limit):
        return {}
    rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _as_list(payload.get("rows")):
        if not isinstance(row, dict) or row.get("status") != "ok":
            continue
        key = _cell_key(row)
        if key not in candidate_keys:
            continue
        row = dict(row)
        row["resumed_from_existing"] = True
        rows_by_key[key] = row
    return rows_by_key


def _select_indicator_ids(
    rows_by_id: dict[str, dict[str, Any]],
    *,
    indicator_ids: list[str] | None,
    signal_role: str | None,
    max_indicators: int | None,
) -> list[str]:
    explicit = _normalize_tokens(indicator_ids)
    if explicit:
        selected = [indicator_id for indicator_id in explicit if indicator_id in rows_by_id]
    else:
        role = _clean_token(signal_role or DEFAULT_SIGNAL_ROLE).lower()
        selected = [
            indicator_id
            for indicator_id, row in rows_by_id.items()
            if not role or _clean_token(row.get("signal_role")).lower() == role
        ]
        selected.sort(
            key=lambda indicator_id: (
                -float(rows_by_id[indicator_id].get("static_prior_score") or 0.0),
                indicator_id,
            )
        )
    if max_indicators is not None and max_indicators >= 0:
        selected = selected[: int(max_indicators)]
    return selected


def _profile_document_for_indicator(
    catalog_item: dict[str, Any],
    *,
    indicator_id: str,
    timeframe: str,
    instruments: list[str],
    label_prefix: str,
) -> dict[str, Any]:
    config = dict(_as_dict(catalog_item.get("config")))
    config["timeframe"] = timeframe
    config["isActive"] = True
    if not config.get("label"):
        config["label"] = indicator_id
    if not config.get("lookbackBars"):
        config["lookbackBars"] = 1
    if not config.get("talibConfig"):
        config["talibConfig"] = []
    if not config.get("ranges"):
        config["ranges"] = {"buy": [0, 1], "sell": [0, 1]}
    if "weight" not in config:
        config["weight"] = 1.0
    if "useFormingBar" not in config:
        config["useFormingBar"] = False
    if "normalizationMode" not in config:
        config["normalizationMode"] = "none"
    if "scale" not in config:
        config["scale"] = 1.0

    instance_id = f"signal-atlas-{indicator_id.lower().replace('_', '-')}-{timeframe.lower()}"
    return {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {
            "name": f"{label_prefix} {indicator_id} {timeframe}",
            "description": "Temporary AutoResearch signal-atlas profile.",
            "directionMode": "both",
            "indicators": [
                {
                    "meta": {
                        "id": indicator_id,
                        "instanceId": instance_id,
                    },
                    "config": config,
                }
            ],
            "instruments": instruments,
            "isActive": False,
            "notificationThreshold": 80,
            "version": "v1",
        },
    }


def _created_profile_id(payload: dict[str, Any]) -> str:
    data = _as_dict(payload.get("data"))
    profile_id = _clean_token(data.get("id"))
    if not profile_id:
        raise CliError("profiles create did not return data.id")
    return profile_id


def _run_cli_json(cli: FuzzfolioCli, args: list[str], *, timeout_seconds: int | None = None) -> dict[str, Any]:
    result = cli.run(args, timeout_seconds=timeout_seconds)
    payload = result.parsed_json
    if not isinstance(payload, dict):
        raise CliError(f"Command did not return JSON: {' '.join(result.argv)}")
    return payload


def _numeric_series(values: Any) -> list[float]:
    series: list[float] = []
    for value in _as_list(values):
        if isinstance(value, bool):
            series.append(1.0 if value else 0.0)
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            series.append(0.0)
            continue
        series.append(number if math.isfinite(number) else 0.0)
    return series


def _active_indexes(series: list[float], *, threshold: float = SIGNAL_EPSILON) -> list[int]:
    return [index for index, value in enumerate(series) if value > threshold]


def _run_lengths(active_flags: list[bool]) -> list[int]:
    lengths: list[int] = []
    current = 0
    for active in active_flags:
        if active:
            current += 1
        elif current:
            lengths.append(current)
            current = 0
    if current:
        lengths.append(current)
    return lengths


def _run_starts(active_flags: list[bool]) -> list[int]:
    starts: list[int] = []
    previous = False
    for index, active in enumerate(active_flags):
        if active and not previous:
            starts.append(index)
        previous = active
    return starts


def _median(values: list[float] | list[int]) -> float | None:
    if not values:
        return None
    return round(float(statistics.median(values)), 4)


def _mean(values: list[float] | list[int]) -> float | None:
    if not values:
        return None
    return round(float(statistics.fmean(values)), 4)


def compute_signal_metrics(
    long_score: list[float],
    short_score: list[float],
    *,
    timestamps: list[Any] | None = None,
) -> dict[str, Any]:
    bars = min(len(long_score), len(short_score))
    long_score = long_score[:bars]
    short_score = short_score[:bars]
    long_flags = [value > SIGNAL_EPSILON for value in long_score]
    short_flags = [value > SIGNAL_EPSILON for value in short_score]
    either_flags = [long_flags[index] or short_flags[index] for index in range(bars)]

    long_runs = _run_lengths(long_flags)
    short_runs = _run_lengths(short_flags)
    either_runs = _run_lengths(either_flags)
    either_starts = _run_starts(either_flags)
    gaps = [
        abs(either_starts[index] - either_starts[index - 1])
        for index in range(1, len(either_starts))
    ]
    long_active = sum(long_flags)
    short_active = sum(short_flags)
    either_active = sum(either_flags)
    total_directional_active = long_active + short_active
    event_count = len(either_starts)
    active_percent = (either_active / bars * 100.0) if bars else 0.0

    if bars == 0:
        density_bucket = "no_data"
    elif either_active == 0:
        density_bucket = "flat"
    elif event_count < 3 or active_percent < 0.20:
        density_bucket = "very_sparse"
    elif active_percent < 1.0:
        density_bucket = "sparse"
    elif active_percent <= 20.0:
        density_bucket = "usable"
    elif active_percent <= 50.0:
        density_bucket = "dense"
    else:
        density_bucket = "saturated"

    if total_directional_active == 0:
        balance_bucket = "flat"
    else:
        long_ratio = long_active / total_directional_active
        if long_active == 0 or short_active == 0:
            balance_bucket = "one_sided"
        elif 0.35 <= long_ratio <= 0.65:
            balance_bucket = "balanced"
        elif long_ratio > 0.65:
            balance_bucket = "long_biased"
        else:
            balance_bucket = "short_biased"

    return {
        "bars": bars,
        "first_timestamp": timestamps[-1] if timestamps else None,
        "last_timestamp": timestamps[0] if timestamps else None,
        "long_active_bars": long_active,
        "short_active_bars": short_active,
        "either_active_bars": either_active,
        "long_event_count": len(_run_starts(long_flags)),
        "short_event_count": len(_run_starts(short_flags)),
        "event_count": event_count,
        "active_percent": round(active_percent, 4),
        "long_active_percent": round((long_active / bars * 100.0) if bars else 0.0, 4),
        "short_active_percent": round((short_active / bars * 100.0) if bars else 0.0, 4),
        "long_share_of_active": round(long_active / total_directional_active, 4)
        if total_directional_active
        else None,
        "avg_persistence_bars": _mean(either_runs),
        "median_persistence_bars": _median(either_runs),
        "max_persistence_bars": max(either_runs) if either_runs else 0,
        "median_bars_between_events": _median(gaps),
        "mean_bars_between_events": _mean(gaps),
        "max_long_score": round(max(long_score), 6) if long_score else None,
        "max_short_score": round(max(short_score), 6) if short_score else None,
        "mean_long_score": _mean(long_score),
        "mean_short_score": _mean(short_score),
        "density_bucket": density_bucket,
        "balance_bucket": balance_bucket,
    }


def _data_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _simulate_signal(
    cli: FuzzfolioCli,
    *,
    profile_id: str,
    instrument: str,
    timeframe: str,
    bar_limit: int,
    replay_source: str | None,
    out_path: Path,
    timeout_seconds: int | None,
) -> dict[str, Any]:
    args = [
        "replay",
        "simulate",
        "--profile-ref",
        profile_id,
        "--instrument",
        instrument,
        "--timeframe",
        timeframe,
        "--bar-limit",
        str(int(bar_limit)),
    ]
    if _clean_token(replay_source):
        args.extend(["--source", _clean_token(replay_source)])
    args.extend(["--full", "--out", str(out_path), "--quiet", "--pretty"])
    cli.run(args, timeout_seconds=timeout_seconds)
    payload = _load_json(out_path)
    data = _data_payload(payload)
    return compute_signal_metrics(
        _numeric_series(data.get("long_score")),
        _numeric_series(data.get("short_score")),
        timestamps=_as_list(data.get("timestamp")),
    )


def _aggregate_indicator_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [row for row in rows if row.get("status") == "ok"]
    if not successful:
        return {
            "status": "failed",
            "runs": len(rows),
            "successful_runs": 0,
            "density_bucket": "no_data",
            "balance_bucket": "no_data",
        }
    total_bars = sum(int(row.get("bars") or 0) for row in successful)
    either_active = sum(int(row.get("either_active_bars") or 0) for row in successful)
    long_active = sum(int(row.get("long_active_bars") or 0) for row in successful)
    short_active = sum(int(row.get("short_active_bars") or 0) for row in successful)
    events = sum(int(row.get("event_count") or 0) for row in successful)
    density_counts: dict[str, int] = {}
    balance_counts: dict[str, int] = {}
    for row in successful:
        density = str(row.get("density_bucket") or "unknown")
        balance = str(row.get("balance_bucket") or "unknown")
        density_counts[density] = density_counts.get(density, 0) + 1
        balance_counts[balance] = balance_counts.get(balance, 0) + 1

    total_directional = long_active + short_active
    active_percent = (either_active / total_bars * 100.0) if total_bars else 0.0
    if either_active == 0:
        aggregate_bucket = "flat"
    elif active_percent < 0.20 or events < max(3, len(successful)):
        aggregate_bucket = "very_sparse"
    elif active_percent < 1.0:
        aggregate_bucket = "sparse"
    elif active_percent <= 20.0:
        aggregate_bucket = "usable"
    elif active_percent <= 50.0:
        aggregate_bucket = "dense"
    else:
        aggregate_bucket = "saturated"

    return {
        "status": "ok",
        "runs": len(rows),
        "successful_runs": len(successful),
        "failed_runs": len(rows) - len(successful),
        "bars": total_bars,
        "event_count": events,
        "either_active_bars": either_active,
        "active_percent": round(active_percent, 4),
        "long_active_bars": long_active,
        "short_active_bars": short_active,
        "long_share_of_active": round(long_active / total_directional, 4)
        if total_directional
        else None,
        "density_bucket": aggregate_bucket,
        "density_bucket_counts": dict(sorted(density_counts.items())),
        "balance_bucket_counts": dict(sorted(balance_counts.items())),
    }


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "indicator_id",
        "signal_role",
        "strategy_role",
        "instrument",
        "timeframe",
        "status",
        "bars",
        "event_count",
        "long_event_count",
        "short_event_count",
        "active_percent",
        "long_active_percent",
        "short_active_percent",
        "long_share_of_active",
        "avg_persistence_bars",
        "median_persistence_bars",
        "median_bars_between_events",
        "density_bucket",
        "balance_bucket",
        "profile_id",
        "error_type",
        "error",
        "resumed_from_existing",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _write_issues_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "indicator_id",
        "issue",
        "severity",
        "detail",
        "runs",
        "active_percent",
        "event_count",
    ]
    issue_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("status") != "ok":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "simulation_failed",
                    "severity": "high",
                    "detail": row.get("error"),
                    "runs": 1,
                    "active_percent": None,
                    "event_count": None,
                }
            )
            continue
        density = str(row.get("density_bucket") or "")
        balance = str(row.get("balance_bucket") or "")
        if density in {"flat", "very_sparse", "saturated"}:
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": f"density_{density}",
                    "severity": "medium" if density != "flat" else "high",
                    "detail": f"{row.get('instrument')} {row.get('timeframe')}",
                    "runs": 1,
                    "active_percent": row.get("active_percent"),
                    "event_count": row.get("event_count"),
                }
            )
        if balance == "one_sided":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "one_sided_signal",
                    "severity": "low",
                    "detail": f"{row.get('instrument')} {row.get('timeframe')}",
                    "runs": 1,
                    "active_percent": row.get("active_percent"),
                    "event_count": row.get("event_count"),
                }
            )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(issue_rows)


def build_signal_atlas(
    config: AppConfig,
    *,
    indicator_ids: list[str] | None = None,
    signal_role: str | None = DEFAULT_SIGNAL_ROLE,
    instruments: list[str] | None = None,
    timeframes: list[str] | None = None,
    bar_limit: int = DEFAULT_BAR_LIMIT,
    max_indicators: int | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    refresh_static_atlas: bool = False,
    keep_profiles: bool = False,
    replay_source: str | None = DEFAULT_REPLAY_SOURCE,
    timeout_seconds: int | None = None,
    progress_callback: Any | None = None,
) -> SignalAtlasBuildResult:
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_SIGNAL_ATLAS_DIRNAME
    )
    raw_dir = target_dir / "raw"
    profile_dir = target_dir / "profiles"
    raw_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    static_atlas_result = None
    static_atlas_path = config.derived_root / DEFAULT_ATLAS_DIRNAME / "indicator-atlas.json"
    if refresh_static_atlas or not static_atlas_path.exists():
        static_atlas_result = build_indicator_atlas(
            config,
            workspace_root=workspace_root,
            catalog_path=catalog_path,
        )
        static_atlas_path = static_atlas_result.atlas_path

    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    catalog_by_id = _indicator_catalog_by_id(catalog_payload)
    rows_by_id = _atlas_rows_by_id(static_atlas_path)
    selected_indicator_ids = _select_indicator_ids(
        rows_by_id,
        indicator_ids=indicator_ids,
        signal_role=signal_role,
        max_indicators=max_indicators,
    )
    instrument_panel = _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS)
    timeframe_panel = _normalize_tokens(timeframes) or list(DEFAULT_TIMEFRAMES)
    bar_limit = max(10, min(5000, int(bar_limit or DEFAULT_BAR_LIMIT)))

    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()

    rows: list[dict[str, Any]] = []
    profile_records: list[dict[str, Any]] = []
    total_calls = len(selected_indicator_ids) * len(timeframe_panel) * len(instrument_panel)
    completed_calls = 0
    run_started = datetime.now(timezone.utc).isoformat()

    for indicator_id in selected_indicator_ids:
        catalog_item = catalog_by_id.get(indicator_id)
        atlas_row = rows_by_id.get(indicator_id, {})
        if not catalog_item:
            for timeframe in timeframe_panel:
                for instrument in instrument_panel:
                    completed_calls += 1
                    rows.append(
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
            profile_id: str | None = None
            try:
                create_payload = _run_cli_json(
                    cli,
                    [
                        "profiles",
                        "create",
                        "--file",
                        str(profile_path),
                        "--pretty",
                    ],
                    timeout_seconds=timeout_seconds,
                )
                profile_id = _created_profile_id(create_payload)
                profile_records.append(
                    {
                        "indicator_id": indicator_id,
                        "timeframe": timeframe,
                        "profile_id": profile_id,
                        "profile_path": str(profile_path),
                        "created": True,
                        "deleted": False,
                    }
                )
                for instrument in instrument_panel:
                    raw_path = raw_dir / f"{indicator_id.lower()}-{timeframe.lower()}-{instrument.lower()}.json"
                    row = {
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
                    try:
                        metrics = _simulate_signal(
                            cli,
                            profile_id=profile_id,
                            instrument=instrument,
                            timeframe=timeframe,
                            bar_limit=bar_limit,
                            replay_source=replay_source,
                            out_path=raw_path,
                            timeout_seconds=timeout_seconds,
                        )
                        row.update(metrics)
                        row["status"] = "ok"
                    except Exception as exc:
                        row["status"] = "failed"
                        row["error_type"] = _error_type(str(exc))
                        row["error"] = str(exc)[:500]
                    rows.append(row)
                    completed_calls += 1
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "completed": completed_calls,
                                "total": total_calls,
                                "indicator_id": indicator_id,
                                "instrument": instrument,
                                "timeframe": timeframe,
                                "status": row.get("status"),
                            }
                        )
            except Exception as exc:
                for instrument in instrument_panel:
                    completed_calls += 1
                    rows.append(
                        {
                            "indicator_id": indicator_id,
                            "signal_role": atlas_row.get("signal_role"),
                            "strategy_role": atlas_row.get("strategy_role"),
                            "instrument": instrument,
                            "timeframe": timeframe,
                            "profile_id": profile_id,
                            "status": "failed",
                            "error_type": _error_type(str(exc)),
                            "error": str(exc)[:500],
                        }
                    )
            finally:
                if profile_id and not keep_profiles:
                    try:
                        cli.run(
                            [
                                "profiles",
                                "delete",
                                "--profile-ref",
                                profile_id,
                                "--pretty",
                            ],
                            timeout_seconds=timeout_seconds,
                        )
                        for record in profile_records:
                            if record.get("profile_id") == profile_id:
                                record["deleted"] = True
                    except Exception as exc:
                        for record in profile_records:
                            if record.get("profile_id") == profile_id:
                                record["delete_error"] = str(exc)[:500]

    by_indicator: dict[str, dict[str, Any]] = {}
    for indicator_id in selected_indicator_ids:
        indicator_rows = [row for row in rows if row.get("indicator_id") == indicator_id]
        aggregate = _aggregate_indicator_rows(indicator_rows)
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
    density_counts: dict[str, int] = {}
    balance_counts: dict[str, int] = {}
    for row in successful_rows:
        density = str(row.get("density_bucket") or "unknown")
        balance = str(row.get("balance_bucket") or "unknown")
        density_counts[density] = density_counts.get(density, 0) + 1
        balance_counts[balance] = balance_counts.get(balance, 0) + 1

    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_started_at": run_started,
        "source": {
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
            "static_atlas_path": str(static_atlas_path),
            "static_atlas_refreshed": static_atlas_result is not None,
        },
        "selection": {
            "signal_role": signal_role,
            "indicator_ids": selected_indicator_ids,
            "indicator_count": len(selected_indicator_ids),
            "instruments": instrument_panel,
            "timeframes": timeframe_panel,
            "bar_limit": bar_limit,
            "replay_source": _clean_token(replay_source) or None,
            "total_requested_calls": total_calls,
        },
        "result_counts": {
            "successful_calls": len(successful_rows),
            "failed_calls": len(failed_rows),
            "density_bucket_counts": dict(sorted(density_counts.items())),
            "balance_bucket_counts": dict(sorted(balance_counts.items())),
        },
        "indicator_rollups": by_indicator,
        "profiles": profile_records,
    }

    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
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
            "profiles": profile_records,
            "replay_source": _clean_token(replay_source) or None,
            "raw_dir": str(raw_dir),
            "profile_dir": str(profile_dir),
        },
    )
    _write_rows_csv(csv_path, rows)
    _write_issues_csv(issues_path, rows)

    return SignalAtlasBuildResult(
        atlas_path=atlas_path,
        csv_path=csv_path,
        summary_path=summary_path,
        issues_path=issues_path,
        request_manifest_path=request_manifest_path,
        summary=summary,
    )
