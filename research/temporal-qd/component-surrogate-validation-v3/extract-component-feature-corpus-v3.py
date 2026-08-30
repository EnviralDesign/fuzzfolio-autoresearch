"""Extract the sealed, outcome-free V3 component feature corpus.

This is intentionally the only bulk calculation path.  It accepts no outcome
input, imports no graph runtime, and reads bars only from the prior verified
archive-recovery report.  Its stable JSON payload omits transient filesystem
roots so independent clean-process runs can compare byte-for-byte.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import math
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from component_projection_support_v3 import (
    calculate_component,
    canonical_bytes,
    full_series,
    load_recovered_window_bars,
    normalize_raw_event,
    read_self_hashed_json,
    sha256_file,
    sha256_prefixed,
)


HISTORICAL_ENGINE_COMMIT = "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2"
EXPECTED_PRIMARY_ROW_COUNT = 19 * 12 * 2
FORWARD_HORIZONS = (1, 3, 6, 12, 24)


def git_text(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args]).decode("utf-8").strip()


def finite_or_none(value: Any) -> float | None:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    return candidate if math.isfinite(candidate) else None


def expected_manifest(manifest: dict[str, Any]) -> None:
    if len(manifest.get("contexts", [])) != 41:
        raise RuntimeError("component census does not contain 41 frozen contexts")
    if manifest.get("cohortCounts", {}).get("uniqueComponentIdentities") != 19:
        raise RuntimeError("component census does not contain 19 frozen component identities")


def component_projection_key(context: dict[str, Any]) -> dict[str, Any]:
    component = context["component"]
    return {
        "indicatorId": component["indicatorId"],
        "baseIndicatorId": component["baseIndicatorId"],
        "fullConfiguration": component["fullConfiguration"],
        "eventOutputs": component["eventOutputs"],
        "timeframe": component["timeframe"],
        "lookbackBars": component["lookbackBars"],
        "signalPersistence": component["signalPersistence"],
    }


def component_representatives(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for context in manifest["contexts"]:
        grouped.setdefault(context["componentIdentity"], []).append(context)
    representatives: list[dict[str, Any]] = []
    for component_identity, contexts in sorted(grouped.items()):
        ordered = sorted(contexts, key=lambda value: value["componentContextIdentity"])
        baseline = canonical_bytes(component_projection_key(ordered[0]))
        if any(canonical_bytes(component_projection_key(context)) != baseline for context in ordered[1:]):
            raise RuntimeError(f"component identity does not resolve one projection: {component_identity}")
        representative = ordered[0]
        representatives.append({
            "componentIdentity": component_identity,
            "representative": representative,
            "contextCount": len(ordered),
            "contextIdentitySha256s": [context["componentContextIdentity"] for context in ordered],
            "projectionKeySha256": sha256_prefixed(baseline),
        })
    if len(representatives) != 19:
        raise RuntimeError(f"component representative count drift: {len(representatives)}")
    return representatives


def raw_series(
    values: Any,
    *,
    index: pd.DatetimeIndex,
    label: str,
) -> tuple[pd.Series, int]:
    normalized, missing = normalize_raw_event(values, expected_length=len(index), label=label)
    return pd.Series(normalized, index=index).sort_index(), missing


def run_lengths(events: np.ndarray) -> list[int]:
    result: list[int] = []
    current = 0
    for event in events.tolist():
        if event:
            current += 1
        elif current:
            result.append(current)
            current = 0
    if current:
        result.append(current)
    return result


def forward_responses(*, close: np.ndarray, high: np.ndarray, low: np.ndarray, starts: np.ndarray, direction: str) -> list[dict[str, Any]]:
    sign = 1.0 if direction == "long" else -1.0
    rows: list[dict[str, Any]] = []
    for horizon in FORWARD_HORIZONS:
        samples = [int(index) for index in starts if int(index) + horizon < len(close)]
        if not samples:
            rows.append({
                "horizonBars": horizon,
                "sampleCount": 0,
                "directionalHitRate": None,
                "meanMFE": None,
                "meanMAE": None,
                "meanMFEminusMAE": None,
                "unavailableReason": "no event start has a complete forward horizon",
            })
            continue
        returns = []
        mfes = []
        maes = []
        for index in samples:
            entry = float(close[index])
            if not math.isfinite(entry) or entry == 0.0:
                continue
            highs = high[index + 1 : index + horizon + 1]
            lows = low[index + 1 : index + horizon + 1]
            directional_return = sign * (float(close[index + horizon]) - entry) / entry
            if direction == "long":
                mfe = (float(np.nanmax(highs)) - entry) / entry
                mae = (float(np.nanmin(lows)) - entry) / entry
            else:
                mfe = (entry - float(np.nanmin(lows))) / entry
                mae = (entry - float(np.nanmax(highs))) / entry
            if all(math.isfinite(value) for value in (directional_return, mfe, mae)):
                returns.append(directional_return)
                mfes.append(mfe)
                maes.append(mae)
        if not returns:
            rows.append({
                "horizonBars": horizon,
                "sampleCount": 0,
                "directionalHitRate": None,
                "meanMFE": None,
                "meanMAE": None,
                "meanMFEminusMAE": None,
                "unavailableReason": "forward bars contain no finite price sample",
            })
            continue
        rows.append({
            "horizonBars": horizon,
            "sampleCount": len(returns),
            "directionalHitRate": float(sum(value > 0.0 for value in returns) / len(returns)),
            "meanMFE": float(statistics.fmean(mfes)),
            "meanMAE": float(statistics.fmean(maes)),
            "meanMFEminusMAE": float(statistics.fmean(mfes) - statistics.fmean(maes)),
            "unavailableReason": None,
        })
    return rows


def processed_diagnostics(processed: dict[str, Any], *, expected_length: int, raw_long: pd.Series, raw_short: pd.Series) -> dict[str, Any]:
    long_values = full_series(processed.get("long"), expected_length=expected_length, label="processed long")
    short_values = full_series(processed.get("short"), expected_length=expected_length, label="processed short")
    def summary(values: np.ndarray, raw: pd.Series) -> dict[str, int]:
        numeric = np.asarray(values, dtype=float)
        comparable = np.isfinite(numeric)
        raw_values = raw.to_numpy(dtype=float)
        exact = comparable & (numeric == raw_values)
        return {
            "finiteScoreBarCount": int(comparable.sum()),
            "exactRawEqualityBarCount": int(exact.sum()),
            "nonEqualFiniteScoreBarCount": int((comparable & ~exact).sum()),
        }
    return {"long": summary(long_values, raw_long), "short": summary(short_values, raw_short)}


def feature_row(
    *,
    component: dict[str, Any],
    window: dict[str, Any],
    direction: str,
    events: pd.Series,
    price_frame: pd.DataFrame,
    representation: str,
    raw_missing_coerced_false: int,
    processed: dict[str, Any],
    raw_long: pd.Series,
    raw_short: pd.Series,
) -> dict[str, Any]:
    start = pd.Timestamp(window["binding"]["request"]["data_start"])
    analysis_start = pd.Timestamp(window["analysisWindowStart"])
    analysis_end = pd.Timestamp(window["analysisWindowEnd"])
    analysis_events = events.loc[(events.index >= analysis_start) & (events.index < analysis_end)]
    analysis_prices = price_frame.loc[(price_frame.index >= analysis_start) & (price_frame.index < analysis_end)].sort_index()
    if not analysis_events.index.equals(analysis_prices.index):
        raise RuntimeError("component event and decision price indexes diverged")
    event_values = analysis_events.to_numpy(dtype=bool)
    prior = np.concatenate(([False], event_values[:-1]))
    starts = event_values & ~prior
    active_positions = np.flatnonzero(event_values)
    start_positions = np.flatnonzero(starts)
    persistence = run_lengths(event_values)
    spacing = np.diff(start_positions) if len(start_positions) > 1 else np.array([], dtype=int)
    active_count = int(event_values.sum())
    forward = forward_responses(
        close=analysis_prices["close"].to_numpy(dtype=float),
        high=analysis_prices["high"].to_numpy(dtype=float),
        low=analysis_prices["low"].to_numpy(dtype=float),
        starts=start_positions,
        direction=direction,
    )
    first_forward = next(item for item in forward if item["horizonBars"] == 1)
    payload = {
        "componentIdentitySha256": component["componentIdentity"],
        "componentProjectionKeySha256": component["projectionKeySha256"],
        "componentContextIdentitySha256s": component["contextIdentitySha256s"],
        "indicatorId": component["representative"]["component"]["indicatorId"],
        "direction": direction,
        "panelId": window["panelId"],
        "windowId": window["windowId"],
        "frozenWindowSemanticSha256": window["binding"]["window_semantic_sha256"],
        "analysisWindowStart": window["analysisWindowStart"],
        "analysisWindowEnd": window["analysisWindowEnd"],
        "representation": representation,
        "sourceTimeframe": component["representative"]["component"]["timeframe"],
        "barCount": len(analysis_events),
        "activeBarCount": active_count,
        "activeFraction": float(active_count / len(analysis_events)) if len(analysis_events) else None,
        "freshEventBarCount": active_count,
        "freshEventFraction": float(active_count / len(analysis_events)) if len(analysis_events) else None,
        "freshEventAvailability": float(active_count / len(analysis_events)) if len(analysis_events) else None,
        "eventStartCount": int(starts.sum()),
        "eventStartShareOfActiveBars": float(starts.sum() / active_count) if active_count else None,
        "eventStartsPer1000Bars": float(starts.sum() * 1000.0 / len(analysis_events)) if len(analysis_events) else None,
        "eventStartsPer10000Bars": float(starts.sum() * 10000.0 / len(analysis_events)) if len(analysis_events) else None,
        "meanPersistenceBars": float(statistics.fmean(persistence)) if persistence else None,
        "medianPersistenceBars": float(statistics.median(persistence)) if persistence else None,
        "maxPersistenceBars": int(max(persistence)) if persistence else None,
        "meanBarsBetweenStarts": float(statistics.fmean(spacing)) if len(spacing) else None,
        "medianBarsBetweenStarts": float(statistics.median(spacing)) if len(spacing) else None,
        "configuredLookbackBars": int(component["representative"]["component"]["lookbackBars"]),
        "signalPersistence": component["representative"]["component"]["signalPersistence"],
        "rawMissingCoercedFalseBarCount": raw_missing_coerced_false,
        "forwardResponseByHorizon": forward,
        "forwardSampleCount": first_forward["sampleCount"],
        "forwardDirectionalHitRate": first_forward["directionalHitRate"],
        "forwardMeanMFE": first_forward["meanMFE"],
        "forwardMeanMAE": first_forward["meanMAE"],
        "forwardMeanMFEminusMAE": first_forward["meanMFEminusMAE"],
        "forwardSummaryHorizonBars": 1,
        "forwardVolNormalized": None,
        "forwardVolNormalizedUnavailableReason": "the protocol did not establish identical forward_response_atlas_v1 volatility semantics",
        "processedDiagnostics": processed_diagnostics(
            processed,
            expected_length=len(raw_long),
            raw_long=raw_long,
            raw_short=raw_short,
        ),
        "warmupDataStart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    payload["rawFeatureRowSha256"] = sha256_prefixed(canonical_bytes(payload))
    return payload


def component_window_rows(component: dict[str, Any], window: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recovered_root = Path(window["isolatedRoot"])
    artifacts = window["archiveArtifacts"]
    context = component["representative"]
    source_timeframe = context["component"]["timeframe"]
    source_frame, source_checks = load_recovered_window_bars(
        root=recovered_root,
        archive_artifacts=artifacts,
        timeframe=source_timeframe,
        pair="EURUSD",
    )
    raw_outputs, processed, instance = calculate_component(source_frame, context)
    event_outputs = context["component"]["eventOutputs"]
    raw_long, missing_long = raw_series(
        raw_outputs[event_outputs["longOutput"]],
        index=source_frame.index,
        label=f"{component['componentIdentity']}:long",
    )
    raw_short, missing_short = raw_series(
        raw_outputs[event_outputs["shortOutput"]],
        index=source_frame.index,
        label=f"{component['componentIdentity']}:short",
    )
    source_path = Path(inspect.getsourcefile(type(instance)) or "").resolve()
    component["implementation"] = {
        "class": type(instance).__name__,
        "sourceSha256": sha256_file(source_path),
        "requiredColumns": sorted(str(value) for value in getattr(instance, "required_columns", set())),
    }
    primary_rows: list[dict[str, Any]] = []
    sensitivity_rows: list[dict[str, Any]] = []
    if source_timeframe == "M5":
        decision_frame = source_frame.sort_index()
        for direction, series, missing in (("long", raw_long, missing_long), ("short", raw_short, missing_short)):
            primary_rows.append(feature_row(
                component=component,
                window=window,
                direction=direction,
                events=series,
                price_frame=decision_frame,
                representation="m5_completed_decision_clock",
                raw_missing_coerced_false=missing,
                processed=processed,
                raw_long=raw_long,
                raw_short=raw_short,
            ))
    elif source_timeframe == "M15":
        decision_frame, decision_checks = load_recovered_window_bars(
            root=recovered_root,
            archive_artifacts=artifacts,
            timeframe="M5",
            pair="EURUSD",
        )
        decision_frame = decision_frame.sort_index()
        shifted_long = raw_long.copy()
        shifted_long.index = shifted_long.index + pd.Timedelta(minutes=10)
        shifted_short = raw_short.copy()
        shifted_short.index = shifted_short.index + pd.Timedelta(minutes=10)
        decision_long = shifted_long.reindex(decision_frame.index, fill_value=False).fillna(False).astype(bool)
        decision_short = shifted_short.reindex(decision_frame.index, fill_value=False).fillna(False).astype(bool)
        for direction, series, missing in (("long", decision_long, missing_long), ("short", decision_short, missing_short)):
            primary_rows.append(feature_row(
                component=component,
                window=window,
                direction=direction,
                events=series,
                price_frame=decision_frame,
                representation="m15_to_m5_completed_decision_clock",
                raw_missing_coerced_false=missing,
                processed=processed,
                raw_long=raw_long,
                raw_short=raw_short,
            ))
        source_frame = source_frame.sort_index()
        for direction, series, missing in (("long", raw_long, missing_long), ("short", raw_short, missing_short)):
            sensitivity_rows.append(feature_row(
                component=component,
                window=window,
                direction=direction,
                events=series,
                price_frame=source_frame,
                representation="m15_native_completed_clock_sensitivity",
                raw_missing_coerced_false=missing,
                processed=processed,
                raw_long=raw_long,
                raw_short=raw_short,
            ))
        source_checks.extend(decision_checks)
    else:
        raise RuntimeError(f"unsupported frozen component timeframe: {source_timeframe}")
    return primary_rows, sensitivity_rows


def run(args: argparse.Namespace) -> dict[str, Any]:
    engine_root = args.engine_root.resolve()
    if git_text(engine_root, "rev-parse", "HEAD") != HISTORICAL_ENGINE_COMMIT:
        raise RuntimeError("component extractor historical engine commit drift")
    manifest = read_self_hashed_json(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    expected_manifest(manifest)
    authority = read_self_hashed_json(args.authority_addendum.resolve(), "authorityAddendumCanonicalPayloadSha256")
    recovery = read_self_hashed_json(args.recovery_report.resolve(), "recoveryCanonicalPayloadSha256")
    if not recovery.get("passes") or not recovery.get("allWindowSemanticsMatchFrozen") or recovery.get("usedCurrentLakeFallback"):
        raise RuntimeError("component extraction refuses an unverified or fallback recovery report")
    if authority["authority"]["extractionCensusCanonicalPayloadSha256"] != manifest["manifestCanonicalPayloadSha256"]:
        raise RuntimeError("projection authority is bound to a different census")
    windows = sorted(recovery["windows"], key=lambda row: (row["panelId"], row["windowId"]))
    if len(windows) != 12:
        raise RuntimeError("component extraction requires all 12 recovered windows")
    representatives = component_representatives(manifest)
    primary_rows: list[dict[str, Any]] = []
    sensitivity_rows: list[dict[str, Any]] = []
    for component in representatives:
        for window in windows:
            primary, sensitivity = component_window_rows(component, window)
            primary_rows.extend(primary)
            sensitivity_rows.extend(sensitivity)
    primary_rows.sort(key=lambda row: (row["componentIdentitySha256"], row["panelId"], row["windowId"], row["direction"]))
    sensitivity_rows.sort(key=lambda row: (row["componentIdentitySha256"], row["panelId"], row["windowId"], row["direction"]))
    if len(primary_rows) != EXPECTED_PRIMARY_ROW_COUNT:
        raise RuntimeError(f"primary row count drift: {len(primary_rows)} != {EXPECTED_PRIMARY_ROW_COUNT}")
    expected_sensitivity_rows = sum(
        component["representative"]["component"]["timeframe"] == "M15"
        for component in representatives
    ) * len(windows) * 2
    if len(sensitivity_rows) != expected_sensitivity_rows:
        raise RuntimeError(
            f"M15 native sensitivity row count drift: {len(sensitivity_rows)} != {expected_sensitivity_rows}"
        )
    if any("temporal_graph" in name for name in sys.modules):
        raise RuntimeError("component extractor imported a TemporalGraph module")
    blocked_network_modules = ("requests", "httpx", "urllib3")
    if any(name == prefix or name.startswith(prefix + ".") for name in sys.modules for prefix in blocked_network_modules):
        raise RuntimeError("component extractor imported a network client module")
    if any("outcome" in name.lower() for name in sys.modules):
        raise RuntimeError("component extractor imported an outcome module")
    implementation_rows = [
        {
            "componentIdentitySha256": component["componentIdentity"],
            "indicatorId": component["representative"]["component"]["indicatorId"],
            "contextCount": component["contextCount"],
            "implementation": component["implementation"],
        }
        for component in representatives
    ]
    return {
        "schemaVersion": "temporal_qd_component_surrogate_feature_corpus_v3",
        "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
        "authorityAddendumCanonicalPayloadSha256": authority["authorityAddendumCanonicalPayloadSha256"],
        "historicalExecutionEngineCommit": HISTORICAL_ENGINE_COMMIT,
        "windowIdentities": [
            {
                "panelId": window["panelId"],
                "windowId": window["windowId"],
                "frozenWindowSemanticSha256": window["binding"]["window_semantic_sha256"],
                "request": window["binding"]["request"],
                "archiveRawSha256s": sorted(artifact["rawSha256"] for artifact in window["archiveArtifacts"]),
            }
            for window in windows
        ],
        "componentImplementations": implementation_rows,
        "primaryRows": primary_rows,
        "m15NativeSensitivityRows": sensitivity_rows,
        "componentOnlyBoundary": {
            "outcomePathArgument": False,
            "outcomeModuleImported": False,
            "temporalGraphImported": False,
            "networkClientImported": False,
            "allBarReadsFromRecoveredIsolatedRoots": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--authority-addendum", type=Path, required=True)
    parser.add_argument("--recovery-report", type=Path, required=True)
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite component feature corpus: {args.output}")
    payload = run(args)
    payload["featureCorpusCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    encoded = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(encoded, encoding="utf-8")
    print(json.dumps({
        "featureCorpusCanonicalPayloadSha256": payload["featureCorpusCanonicalPayloadSha256"],
        "primaryRowCount": len(payload["primaryRows"]),
        "m15NativeSensitivityRowCount": len(payload["m15NativeSensitivityRows"]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
