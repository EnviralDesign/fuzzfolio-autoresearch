from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from .config import AppConfig, load_config
from .fuzzfolio import FuzzfolioCli
from .ledger import (
    append_attempt,
    attempts_path_for_run_dir,
    make_attempt_record,
    write_run_metadata,
)
from .plotting import render_progress_artifacts
from .scoring import build_attempt_score, load_sensitivity_snapshot


console = Console(safe_box=True)

TRIGGER_ID_TOKENS = (
    "CROSS",
    "REJECTION",
    "REENTRY",
    "FIRST_CLOSE",
    "BREAKOUT",
    "RECLAIM",
)

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
PLAY_HAND_SWEEP_PERMUTATION_LIMIT = 625

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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _append_event(ctx: PlayHandContext, phase: str, status: str, **payload: Any) -> None:
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


def _render_dealt_hand(
    *,
    indicators: list[str],
    instrument_deal: dict[str, Any],
    timeframe: str,
    max_sweep_permutations: int,
    screen_months: int,
    scrutiny_months: int,
    coarse_mode: str,
) -> None:
    table = Table(title="Play-hand dealt", show_header=False, show_lines=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Instrument", f"{instrument_deal['primary_instrument']} ({instrument_deal['source']})")
    table.add_row("Timeframe", timeframe)
    table.add_row("Indicators", ", ".join(indicators))
    table.add_row("Screen", f"{screen_months}mo")
    table.add_row("Scrutiny", f"{scrutiny_months}mo")
    table.add_row("Coarse mode", coarse_mode)
    table.add_row("Sweep cap", str(max_sweep_permutations))
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
) -> None:
    title = f"{stage.prefix} {phase} sweep" if stage is not None else f"{phase} sweep"
    table = Table(title=title, show_header=False, show_lines=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Mode", mode)
    table.add_row(
        "Permutations",
        f"{permutation_count}"
        + (f" / {original_permutations} original" if original_permutations != permutation_count else ""),
    )
    table.add_row("Cap", str(max_permutations))
    table.add_row("Large sweep", "yes" if permutation_count > SWEEP_PERMUTATION_HARD_LIMIT else "no")
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
        table.add_row("Dropped", "; ".join(dropped_bits))
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
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        indicator_id = str(meta.get("id") or "").upper()
        role = str(meta.get("signalRole") or meta.get("preferredTimeframeRole") or "").lower()
        if role == "trigger" or any(token in indicator_id for token in TRIGGER_ID_TOKENS):
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
    axes: list[str] = []
    for index, item in enumerate(profile.get("indicators") or []):
        if not isinstance(item, dict):
            continue
        config = item.get("config") if isinstance(item.get("config"), dict) else {}
        if config.get("isActive") is False:
            continue
        values = _lookback_values_for_timeframe(str(config.get("timeframe") or ""))
        axes.append(
            f"indicator[{index}].config.lookbackBars="
            + ",".join(str(value) for value in values)
        )
    return axes


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


def build_coarse_axes(profile_payload: dict[str, Any], *, max_axes: int = 6) -> list[str]:
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
            if len(axes) >= max_axes:
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


def fit_axes_to_permutation_budget(
    axes: list[str],
    *,
    max_permutations: int = SWEEP_PERMUTATION_HARD_LIMIT,
) -> tuple[list[str], list[str], int]:
    selected: list[str] = []
    dropped: list[str] = []
    selected_count = 1
    original_count = _permutation_count(axes)
    for axis in axes:
        next_count = selected_count * _axis_cardinality(axis)
        if next_count <= max_permutations:
            selected.append(axis)
            selected_count = next_count
        else:
            dropped.append(axis)
    return selected, dropped, original_count


def _refine_values(values: list[Any], best_value: Any) -> list[Any]:
    if not isinstance(best_value, (int, float)) or isinstance(best_value, bool):
        return [best_value] if best_value not in (None, "") else values[:1]
    numeric_values = sorted(
        float(value)
        for value in values
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    )
    best = float(best_value)
    diffs = [
        abs(right - left)
        for left, right in zip(numeric_values, numeric_values[1:])
        if abs(right - left) > 0
    ]
    step = min(diffs) / 2.0 if diffs else max(abs(best) * 0.1, 1.0)
    if float(best_value).is_integer() and all(float(v).is_integer() for v in numeric_values):
        int_step = max(1, int(round(step)))
        candidates = [max(1, int(round(best)) + offset * int_step) for offset in (-2, -1, 0, 1, 2)]
        return sorted(set(candidates))
    candidates = [round(max(0.000001, best + offset * step), 6) for offset in (-2, -1, 0, 1, 2)]
    return sorted(set(candidates))


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


def _seed_hand(config: AppConfig, cli: FuzzfolioCli, run_dir: Path) -> list[str]:
    seed_path = run_dir / "seed-prompt.json"
    result = cli.seed_prompt(seed_path)
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else _load_json(seed_path)
    indicators = payload.get("indicators") if isinstance(payload, dict) else None
    return [str(item).strip() for item in indicators or [] if str(item).strip()]


def _register_profile(ctx: PlayHandContext, profile_path: Path) -> str:
    if ctx.dry_run:
        return f"dry-{profile_path.stem}"
    result = ctx.cli.run(["profiles", "create", "--file", str(profile_path), "--pretty"])
    payload = result.parsed_json if isinstance(result.parsed_json, dict) else {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    profile_ref = str(data.get("id") or "").strip()
    if not profile_ref:
        raise RuntimeError(f"profiles create did not return an id for {profile_path}")
    return profile_ref


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
    return out_path


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
) -> dict[str, Any]:
    out_dir = (ctx.evals_dir / f"eval_{phase}_{_utc_stamp()}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if ctx.dry_run:
        score = None
        _append_event(ctx, phase, "dry_run", stage=stage, artifact_dir=str(out_dir), score=score)
        return {"artifact_dir": str(out_dir), "score": score, "profile_ref": profile_ref}
    args = [
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
    for instrument in instruments:
        args.extend(["--instrument", instrument])
    args.append("--pretty")
    ctx.cli.run(args, timeout_seconds=900)
    compare_payload = ctx.cli.score_artifact(out_dir)
    snapshot_path = out_dir / "sensitivity-response.json"
    snapshot = load_sensitivity_snapshot(out_dir) if snapshot_path.exists() else None
    attempt_score = build_attempt_score(compare_payload, snapshot)
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
    )
    append_attempt(ctx.attempts_path, record)
    from .ledger import load_attempts

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
    )
    return {
        "artifact_dir": str(out_dir),
        "attempt_id": record.attempt_id,
        "score": record.composite_score,
        "score_basis": record.score_basis,
        "profile_ref": profile_ref,
        "profile_path": str(profile_path),
    }


def _run_sweep(
    ctx: PlayHandContext,
    *,
    stage: PlayHandStage,
    phase: str,
    profile_ref: str,
    instruments: list[str],
    axes: list[str],
    mode: str,
    evolutionary_budget: str,
    max_permutations: int,
) -> dict[str, Any]:
    original_axes = list(axes)
    axes, dropped_axes, original_permutations = fit_axes_to_permutation_budget(
        original_axes,
        max_permutations=max_permutations,
    )
    selected_permutations = _permutation_count(axes)
    if dropped_axes:
        _append_event(
            ctx,
            phase,
            "budgeted",
            stage=stage,
            original_axes=original_axes,
            selected_axes=axes,
            dropped_axes=dropped_axes,
            original_permutations=original_permutations,
            selected_permutations=selected_permutations,
            max_permutations=max_permutations,
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
        )
        return {"artifact_dir": None, "result": result, "axes": []}
    _render_sweep_plan(
        stage=stage,
        phase=phase,
        axes=axes,
        dropped_axes=dropped_axes,
        permutation_count=selected_permutations,
        original_permutations=original_permutations,
        max_permutations=max_permutations,
        mode=mode,
    )
    out_dir = (ctx.evals_dir / f"sweep_{phase}_{_utc_stamp()}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if ctx.dry_run:
        result = {
            "sweep_id": f"dry-{phase}",
            "mode": mode,
            "ranked_permutations": [],
            "parameter_importance": [],
            "axes": axes,
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
        )
        return {"artifact_dir": str(out_dir), "result": result, "axes": axes}
    args = [
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
    if mode == "evolutionary":
        if evolutionary_budget == "low":
            args.extend(["--population-size", "20", "--max-generations", "5"])
        elif evolutionary_budget == "high":
            args.extend(["--population-size", "40", "--max-generations", "12"])
        else:
            args.extend(["--population-size", "30", "--max-generations", "10"])
    for instrument in instruments:
        args.extend(["--instrument", instrument])
    for axis in axes:
        args.extend(["--axis", axis])
    if selected_permutations > SWEEP_PERMUTATION_HARD_LIMIT:
        args.append("--allow-large-sweep")
    args.append("--pretty")

    def heartbeat(elapsed: float) -> None:
        elapsed_text = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
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
                        "progress_text": f"{selected_permutations} planned permutations",
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )
        console.print(
            f"{stage.prefix} [cyan]{phase}[/] [bold]running[/] "
            f"{selected_permutations} planned permutations elapsed={elapsed_text}"
        )

    result = ctx.cli.run_with_heartbeat(
        args,
        timeout_seconds=1800,
        heartbeat_seconds=30,
        heartbeat=heartbeat,
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
        mode=mode,
        permutation_count=selected_permutations,
        max_permutations=max_permutations,
        allow_large_sweep=selected_permutations > SWEEP_PERMUTATION_HARD_LIMIT,
        top_score=_top_sweep_score(payload),
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
    data = _sweep_data(payload)
    ranked = data.get("ranked_permutations") or data.get("ranked") or []
    if isinstance(ranked, list) and ranked:
        first = ranked[0]
        if isinstance(first, dict) and isinstance(first.get("parameters"), dict):
            return dict(first["parameters"])
    best = data.get("best")
    if isinstance(best, dict) and isinstance(best.get("parameters"), dict):
        return dict(best["parameters"])
    return {}


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
    )
    return output_path, profile_ref


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


def cmd_play_hand(
    *,
    instrument: list[str] | None = None,
    instrument_pool: list[str] | None = None,
    timeframe: str,
    max_sweep_permutations: int,
    max_indicators: int,
    seed: int | None,
    screen_months: int,
    scrutiny_months: int,
    coarse_mode: str,
    evolutionary_budget: str,
    dry_run: bool,
    as_json: bool,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
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
    )
    profiles_dir.mkdir(parents=True, exist_ok=True)
    evals_dir.mkdir(parents=True, exist_ok=True)

    hand = _seed_hand(config, cli, run_dir) if not dry_run else [
        "RSI_CROSSBACK",
        "STOCH_CROSSOVER",
        "MA_SLOPE_TREND",
        "ADX",
        "WICK_REJECTION",
    ]
    rng = random.Random(seed)
    shuffled = list(hand)
    rng.shuffle(shuffled)
    dealt = shuffled[: max(1, int(max_indicators))]
    instrument_deal = deal_instruments(
        instrument=instrument,
        instrument_pool=instrument_pool,
        rng=rng,
    )
    instruments = list(instrument_deal["instruments"])
    timeframe = str(timeframe or "M5").strip().upper() or "M5"
    max_sweep_permutations = max(1, int(max_sweep_permutations or PLAY_HAND_SWEEP_PERMUTATION_LIMIT))

    metadata = {
        "run_id": run_id,
        "runner": "play_hand_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "canonical_score_lab_version": "score_lab_v2_3",
        "seed": seed,
        "instrument_source": instrument_deal["source"],
        "primary_instrument": instrument_deal["primary_instrument"],
        "instrument_pool": instrument_deal["instrument_pool"],
        "instruments": instruments,
        "timeframe": timeframe,
        "dealt_indicator_ids": dealt,
        "screen_months": screen_months,
        "scrutiny_months": scrutiny_months,
        "coarse_mode": coarse_mode,
        "evolutionary_budget": evolutionary_budget,
        "max_sweep_permutations": max_sweep_permutations,
        "dry_run": dry_run,
    }
    write_run_metadata(run_dir, metadata)
    _render_dealt_hand(
        indicators=dealt,
        instrument_deal=instrument_deal,
        timeframe=timeframe,
        max_sweep_permutations=max_sweep_permutations,
        screen_months=screen_months,
        scrutiny_months=scrutiny_months,
        coarse_mode=coarse_mode,
    )
    stage_total = 7
    stages = {
        "deal": PlayHandStage(1, stage_total, "Deal hand"),
        "scaffold": PlayHandStage(2, stage_total, "Scaffold profile"),
        "baseline": PlayHandStage(3, stage_total, "Baseline screen"),
        "lookback": PlayHandStage(4, stage_total, "Lookback timing sweep"),
        "coarse": PlayHandStage(5, stage_total, "Coarse parameter sweep"),
        "focused": PlayHandStage(6, stage_total, "Focused refinement sweep"),
        "scrutiny": PlayHandStage(7, stage_total, "Final scrutiny"),
    }
    _append_event(
        ctx,
        "deal",
        "dealt",
        stage=stages["deal"],
        indicators=dealt,
        instrument_source=instrument_deal["source"],
        primary_instrument=instrument_deal["primary_instrument"],
        instrument_pool=instrument_deal["instrument_pool"],
        instruments=instruments,
        timeframe=timeframe,
        seed=seed,
    )

    phase_rows: list[dict[str, Any]] = []
    profile_path = _scaffold_profile(ctx, dealt, instruments, timeframe, "hand_base")
    profile_payload = _load_json(profile_path)
    default_changes = apply_play_hand_profile_defaults(profile_payload, rng=rng)
    if default_changes:
        _write_json(profile_path, profile_payload)
        _append_event(
            ctx,
            "scaffold",
            "defaults_applied",
            stage=stages["scaffold"],
            profile_path=str(profile_path),
            changes=default_changes,
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

    baseline = _evaluate_profile(
        ctx,
        stage=stages["baseline"],
        phase="baseline_3mo",
        profile_ref=profile_ref,
        profile_path=profile_path,
        instruments=instruments,
        timeframe=timeframe,
        lookback_months=screen_months,
    )
    phase_rows.append({"phase": "baseline", "status": "evaluated", "score": baseline.get("score"), "detail": profile_ref})

    current_profile_path = profile_path
    current_profile_ref = profile_ref
    last_sweep_payload: dict[str, Any] | None = None
    last_sweep_axes: list[str] = []

    lookback_axes = build_lookback_axes(profile_payload)
    if lookback_axes:
        sweep = _run_sweep(
            ctx,
            stage=stages["lookback"],
            phase="lookback_timing",
            profile_ref=current_profile_ref,
            instruments=instruments,
            axes=lookback_axes,
            mode="deterministic",
            evolutionary_budget=evolutionary_budget,
            max_permutations=max_sweep_permutations,
        )
        last_sweep_payload = sweep["result"]
        last_sweep_axes = list(sweep.get("axes") or lookback_axes)
        params = _best_sweep_parameters(last_sweep_payload)
        if params:
            current_profile_path, current_profile_ref = _materialize_and_register(
                ctx,
                stage=stages["lookback"],
                source_profile_path=current_profile_path,
                parameters=params,
                phase="lookback_timing",
            )
            result = _evaluate_profile(
                ctx,
                stage=stages["lookback"],
                phase="lookback_timing_top_3mo",
                profile_ref=current_profile_ref,
                profile_path=current_profile_path,
                instruments=instruments,
                timeframe=timeframe,
                lookback_months=screen_months,
            )
            phase_rows.append({"phase": "lookback", "status": "top evaluated", "score": result.get("score"), "detail": ", ".join(lookback_axes)})
    else:
        _append_event(ctx, "lookback_timing", "skipped", stage=stages["lookback"], reason="no active indicators available")

    coarse_axes = build_coarse_axes(_load_json(current_profile_path), max_axes=6)
    if coarse_axes:
        sweep = _run_sweep(
            ctx,
            stage=stages["coarse"],
            phase="coarse",
            profile_ref=current_profile_ref,
            instruments=instruments,
            axes=coarse_axes,
            mode=coarse_mode,
            evolutionary_budget=evolutionary_budget,
            max_permutations=max_sweep_permutations,
        )
        last_sweep_payload = sweep["result"]
        last_sweep_axes = list(sweep.get("axes") or coarse_axes)
        params = _best_sweep_parameters(last_sweep_payload)
        if params:
            current_profile_path, current_profile_ref = _materialize_and_register(
                ctx,
                stage=stages["coarse"],
                source_profile_path=current_profile_path,
                parameters=params,
                phase="coarse",
            )
            result = _evaluate_profile(
                ctx,
                stage=stages["coarse"],
                phase="coarse_top_3mo",
                profile_ref=current_profile_ref,
                profile_path=current_profile_path,
                instruments=instruments,
                timeframe=timeframe,
                lookback_months=screen_months,
            )
            phase_rows.append({"phase": "coarse", "status": "top evaluated", "score": result.get("score"), "detail": f"{len(coarse_axes)} axes"})
    else:
        _append_event(ctx, "coarse", "skipped", stage=stages["coarse"], reason="no numeric talib axes found")

    focused_axes = (
        build_focused_axes(_parameter_importance(last_sweep_payload or {}), last_sweep_axes)
        if last_sweep_payload
        else []
    )
    if focused_axes:
        sweep = _run_sweep(
            ctx,
            stage=stages["focused"],
            phase="focused",
            profile_ref=current_profile_ref,
            instruments=instruments,
            axes=focused_axes,
            mode="deterministic",
            evolutionary_budget=evolutionary_budget,
            max_permutations=max_sweep_permutations,
        )
        params = _best_sweep_parameters(sweep["result"])
        if params:
            current_profile_path, current_profile_ref = _materialize_and_register(
                ctx,
                stage=stages["focused"],
                source_profile_path=current_profile_path,
                parameters=params,
                phase="focused",
            )
            result = _evaluate_profile(
                ctx,
                stage=stages["focused"],
                phase="focused_top_3mo",
                profile_ref=current_profile_ref,
                profile_path=current_profile_path,
                instruments=instruments,
                timeframe=timeframe,
                lookback_months=screen_months,
            )
            phase_rows.append({"phase": "focused", "status": "top evaluated", "score": result.get("score"), "detail": ", ".join(focused_axes)})
    else:
        _append_event(ctx, "focused", "skipped", stage=stages["focused"], reason="no high-impact axes available from previous sweep")

    scrutiny = _evaluate_profile(
        ctx,
        stage=stages["scrutiny"],
        phase="final_36mo",
        profile_ref=current_profile_ref,
        profile_path=current_profile_path,
        instruments=instruments,
        timeframe=timeframe,
        lookback_months=scrutiny_months,
    )
    phase_rows.append({"phase": "scrutiny", "status": "evaluated", "score": scrutiny.get("score"), "detail": f"{scrutiny_months}mo"})

    summary = {
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "runner": "play_hand_v1",
        "dealt_indicator_ids": dealt,
        "instrument_source": instrument_deal["source"],
        "primary_instrument": instrument_deal["primary_instrument"],
        "instrument_pool": instrument_deal["instrument_pool"],
        "instruments": instruments,
        "timeframe": timeframe,
        "max_sweep_permutations": max_sweep_permutations,
        "final_profile_ref": current_profile_ref,
        "final_profile_path": str(current_profile_path.resolve()),
        "final_score": scrutiny.get("score"),
        "events_path": str(ctx.events_path.resolve()),
        "attempts_path": str(ctx.attempts_path.resolve()),
        "phase_rows": phase_rows,
    }
    _write_json(ctx.summary_path, summary)
    if as_json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
    else:
        _render_phase_table(phase_rows)
        console.print(f"Run dir: {run_dir}")
    return 0
