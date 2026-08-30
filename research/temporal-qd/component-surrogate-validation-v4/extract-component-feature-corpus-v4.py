"""Sterile V4 component-feature extraction from accepted recovered windows."""

from __future__ import annotations

import argparse
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

HERE = Path(__file__).resolve().parent
V3_ROOT = HERE.parent / "component-surrogate-validation-v3"
sys.path.insert(0, str(V3_ROOT))

from component_projection_support_v3 import (  # noqa: E402
    calculate_component,
    canonical_bytes,
    full_series,
    load_recovered_window_bars,
    normalize_raw_event,
    read_self_hashed_json,
    sha256_file,
    sha256_prefixed,
)
from component_surrogate_v4_metrics import (  # noqa: E402
    event_series_payload,
    event_start_mask,
    forward_response_by_horizon,
)


HISTORICAL_ENGINE_COMMIT = "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2"
EXPECTED_PRIMARY_ROW_COUNT = 19 * 12 * 2


def git_text(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args]).decode().strip()


def component_key(context: dict[str, Any]) -> dict[str, Any]:
    component = context["component"]
    return {key: component[key] for key in ("indicatorId", "baseIndicatorId", "fullConfiguration", "eventOutputs", "timeframe", "lookbackBars", "signalPersistence")}


def representatives(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for context in manifest["contexts"]:
        groups.setdefault(context["componentIdentity"], []).append(context)
    result = []
    for identity, contexts in sorted(groups.items()):
        ordered = sorted(contexts, key=lambda row: row["componentContextIdentity"])
        key = canonical_bytes(component_key(ordered[0]))
        if any(canonical_bytes(component_key(row)) != key for row in ordered[1:]):
            raise RuntimeError(f"component identity resolves more than one projection: {identity}")
        result.append({"componentIdentity": identity, "representative": ordered[0], "contextCount": len(ordered), "contextIdentitySha256s": [row["componentContextIdentity"] for row in ordered], "projectionKeySha256": sha256_prefixed(key)})
    if len(result) != 19:
        raise RuntimeError("V4 requires exactly 19 component identities")
    return result


def raw_series(values: Any, index: pd.DatetimeIndex, label: str) -> tuple[pd.Series, int]:
    normalized, missing = normalize_raw_event(values, expected_length=len(index), label=label)
    return pd.Series(normalized, index=index).sort_index(), missing


def processed_diagnostics(processed: dict[str, Any], raw_long: pd.Series, raw_short: pd.Series) -> dict[str, Any]:
    def summary(values: Any, raw: pd.Series, label: str) -> dict[str, int]:
        numeric = np.asarray(full_series(values, expected_length=len(raw), label=label), dtype=float)
        finite = np.isfinite(numeric)
        exact = finite & (numeric == raw.to_numpy(dtype=float))
        return {"finiteScoreBarCount": int(finite.sum()), "exactRawEqualityBarCount": int(exact.sum()), "nonEqualFiniteScoreBarCount": int((finite & ~exact).sum())}
    return {"long": summary(processed.get("long"), raw_long, "processed long"), "short": summary(processed.get("short"), raw_short, "processed short")}


def run_lengths(events: np.ndarray) -> list[int]:
    lengths: list[int] = []
    current = 0
    for active in np.asarray(events, dtype=bool):
        if active:
            current += 1
        elif current:
            lengths.append(current)
            current = 0
    if current:
        lengths.append(current)
    return lengths


def feature_row(component: dict[str, Any], window: dict[str, Any], direction: str, events: pd.Series, prices: pd.DataFrame, representation: str, missing: int, processed: dict[str, Any], raw_long: pd.Series, raw_short: pd.Series) -> dict[str, Any]:
    start, end = pd.Timestamp(window["analysisWindowStart"]), pd.Timestamp(window["analysisWindowEnd"])
    analysis_events = events.loc[(events.index >= start) & (events.index < end)]
    analysis_prices = prices.loc[(prices.index >= start) & (prices.index < end)].sort_index()
    if not analysis_events.index.equals(analysis_prices.index) or analysis_events.empty:
        raise RuntimeError("V4 event/price analysis clock mismatch")
    preceding = events.loc[events.index < start]
    if preceding.empty:
        raise RuntimeError("V4 event-start boundary lacks a recovered warm-up predecessor")
    values = analysis_events.to_numpy(dtype=bool)
    prior_event = bool(preceding.iloc[-1])
    starts = event_start_mask(values, prior_event=prior_event)
    start_positions = np.flatnonzero(starts)
    forward = forward_response_by_horizon(close=analysis_prices["close"].to_numpy(float), high=analysis_prices["high"].to_numpy(float), low=analysis_prices["low"].to_numpy(float), starts=start_positions, direction=direction)
    first = forward[0]
    active_count = int(values.sum())
    persistence = run_lengths(values)
    spacing = np.diff(start_positions) if len(start_positions) > 1 else np.array([], dtype=int)
    series_payload = event_series_payload(analysis_events.index, values)
    payload = {
        "componentIdentitySha256": component["componentIdentity"], "componentProjectionKeySha256": component["projectionKeySha256"], "componentContextIdentitySha256s": component["contextIdentitySha256s"], "indicatorId": component["representative"]["component"]["indicatorId"],
        "direction": direction, "panelId": window["panelId"], "windowId": window["windowId"], "frozenWindowSemanticSha256": window["binding"]["window_semantic_sha256"], "analysisWindowStart": window["analysisWindowStart"], "analysisWindowEnd": window["analysisWindowEnd"], "representation": representation, "sourceTimeframe": component["representative"]["component"]["timeframe"],
        "barCount": len(values), "activeBarCount": active_count, "activeFraction": active_count / len(values), "freshEventBarCount": active_count, "freshEventFraction": active_count / len(values), "freshEventAvailability": active_count / len(values),
        "eventStartCount": int(starts.sum()), "eventStartShareOfActiveBars": float(starts.sum() / active_count) if active_count else None, "eventStartsPer1000Bars": float(starts.sum() * 1000 / len(values)), "eventStartsPer10000Bars": float(starts.sum() * 10000 / len(values)),
        "eventStartBoundary": {"precedingCompletedEventTimestamp": preceding.index[-1].strftime("%Y-%m-%dT%H:%M:%SZ"), "precedingCompletedEventActive": prior_event, "firstAnalysisTimestamp": analysis_events.index[0].strftime("%Y-%m-%dT%H:%M:%SZ"), "firstAnalysisEventActive": bool(values[0]), "firstAnalysisBarIsStart": bool(starts[0])},
        "freshEventSeriesCanonicalPayloadSha256": sha256_prefixed(canonical_bytes(series_payload)), "freshEventSeriesBarCount": len(values), "freshEventSeriesActiveBarCount": active_count,
        "meanPersistenceBars": float(statistics.fmean(persistence)) if persistence else None, "medianPersistenceBars": float(statistics.median(persistence)) if persistence else None, "maxPersistenceBars": int(max(persistence)) if persistence else None, "meanBarsBetweenStarts": float(statistics.fmean(spacing)) if len(spacing) else None, "medianBarsBetweenStarts": float(statistics.median(spacing)) if len(spacing) else None,
        "configuredLookbackBars": int(component["representative"]["component"]["lookbackBars"]), "signalPersistence": component["representative"]["component"]["signalPersistence"], "rawMissingCoercedFalseBarCount": missing,
        "forwardResponseByHorizon": forward, "forwardSummaryHorizonBars": 1, "forwardSampleCount": first["sampleCount"], "forwardMeanDirectionalReturn": first["meanDirectionalReturn"], "forwardMedianDirectionalReturn": first["medianDirectionalReturn"], "forwardDirectionalHitRate": first["directionalHitRate"], "forwardMeanMFE": first["meanMFE"], "forwardMeanMAE": first["meanMAE"], "forwardMeanMFEminusMAE": first["meanMFEminusMAE"], "forwardVolNormalized": first["meanVolatilityNormalizedDirectionalReturn"], "forwardVolNormalizedUnavailableReason": None if first["meanVolatilityNormalizedDirectionalReturn"] is not None else "no complete event has a finite positive Atlas-style pre-event volatility",
        "processedDiagnostics": processed_diagnostics(processed, raw_long, raw_short), "warmupDataStart": str(window["binding"]["request"]["data_start"]),
    }
    payload["rawFeatureRowSha256"] = sha256_prefixed(canonical_bytes(payload))
    return payload


def component_window_rows(component: dict[str, Any], window: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    root, context = Path(window["isolatedRoot"]), component["representative"]
    timeframe = context["component"]["timeframe"]
    source, _ = load_recovered_window_bars(root=root, archive_artifacts=window["archiveArtifacts"], timeframe=timeframe, pair="EURUSD")
    raw_outputs, processed, instance = calculate_component(source, context)
    outputs = context["component"]["eventOutputs"]
    raw_long, miss_long = raw_series(raw_outputs[outputs["longOutput"]], source.index, f"{component['componentIdentity']}:long")
    raw_short, miss_short = raw_series(raw_outputs[outputs["shortOutput"]], source.index, f"{component['componentIdentity']}:short")
    source_path = Path(inspect.getsourcefile(type(instance)) or "").resolve()
    component["implementation"] = {"class": type(instance).__name__, "sourceSha256": sha256_file(source_path), "requiredColumns": sorted(map(str, getattr(instance, "required_columns", set())))}
    if timeframe == "M5":
        prices = source.sort_index()
        return ([feature_row(component, window, side, series, prices, "m5_completed_decision_clock", missing, processed, raw_long, raw_short) for side, series, missing in (("long", raw_long, miss_long), ("short", raw_short, miss_short))], [])
    if timeframe != "M15":
        raise RuntimeError(f"unsupported V4 component timeframe: {timeframe}")
    m5, _ = load_recovered_window_bars(root=root, archive_artifacts=window["archiveArtifacts"], timeframe="M5", pair="EURUSD")
    m5 = m5.sort_index()
    shifted = []
    for series in (raw_long, raw_short):
        value = series.copy(); value.index = value.index + pd.Timedelta(minutes=10); shifted.append(value.reindex(m5.index, fill_value=False).fillna(False).astype(bool))
    primary = [feature_row(component, window, side, series, m5, "m15_to_m5_completed_decision_clock", missing, processed, raw_long, raw_short) for side, series, missing in (("long", shifted[0], miss_long), ("short", shifted[1], miss_short))]
    native_prices = source.sort_index()
    sensitivity = [feature_row(component, window, side, series, native_prices, "m15_native_completed_clock_sensitivity", missing, processed, raw_long, raw_short) for side, series, missing in (("long", raw_long, miss_long), ("short", raw_short, miss_short))]
    return primary, sensitivity


def primary_event_projection(component: dict[str, Any], window: dict[str, Any], direction: str) -> tuple[pd.Series, pd.DataFrame]:
    """Reconstruct one sealed primary event series for S2 hash-verified joining."""
    root, context = Path(window["isolatedRoot"]), component["representative"]
    timeframe = context["component"]["timeframe"]
    source, _ = load_recovered_window_bars(root=root, archive_artifacts=window["archiveArtifacts"], timeframe=timeframe, pair="EURUSD")
    raw_outputs, _, _ = calculate_component(source, context)
    output = context["component"]["eventOutputs"]["longOutput" if direction == "long" else "shortOutput"]
    events, _ = raw_series(raw_outputs[output], source.index, f"{component['componentIdentity']}:{direction}")
    if timeframe == "M5":
        prices = source.sort_index()
    elif timeframe == "M15":
        prices, _ = load_recovered_window_bars(root=root, archive_artifacts=window["archiveArtifacts"], timeframe="M5", pair="EURUSD")
        prices = prices.sort_index(); events = events.copy(); events.index = events.index + pd.Timedelta(minutes=10); events = events.reindex(prices.index, fill_value=False).fillna(False).astype(bool)
    else:
        raise RuntimeError(f"unsupported V4 component timeframe: {timeframe}")
    start, end = pd.Timestamp(window["analysisWindowStart"]), pd.Timestamp(window["analysisWindowEnd"])
    events, prices = events.loc[(events.index >= start) & (events.index < end)], prices.loc[(prices.index >= start) & (prices.index < end)].sort_index()
    if not events.index.equals(prices.index): raise RuntimeError("S2 primary event/price clock mismatch")
    return events, prices


def run(args: argparse.Namespace) -> dict[str, Any]:
    if git_text(args.engine_root.resolve(), "rev-parse", "HEAD") != HISTORICAL_ENGINE_COMMIT:
        raise RuntimeError("V4 historical engine commit drift")
    protocol = read_self_hashed_json(args.correction_protocol.resolve(), "correctionProtocolCanonicalPayloadSha256")
    manifest = read_self_hashed_json(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    authority = read_self_hashed_json(args.authority_addendum.resolve(), "authorityAddendumCanonicalPayloadSha256")
    recovery = read_self_hashed_json(args.recovery_report.resolve(), "recoveryCanonicalPayloadSha256")
    if protocol["acceptedV3"]["primaryFeatureRowCount"] != EXPECTED_PRIMARY_ROW_COUNT or len(manifest.get("contexts", [])) != 41 or not recovery.get("allWindowSemanticsMatchFrozen") or recovery.get("usedCurrentLakeFallback"):
        raise RuntimeError("V4 accepted authority/recovery contract drift")
    if authority["authority"]["extractionCensusCanonicalPayloadSha256"] != manifest["manifestCanonicalPayloadSha256"]:
        raise RuntimeError("V4 projection authority/census drift")
    windows = sorted(recovery["windows"], key=lambda row: (row["panelId"], row["windowId"]))
    if len(windows) != 12:
        raise RuntimeError("V4 requires all 12 recovered windows")
    primary: list[dict[str, Any]] = []; sensitivity: list[dict[str, Any]] = []
    components = representatives(manifest)
    for component in components:
        for window in windows:
            a, b = component_window_rows(component, window); primary.extend(a); sensitivity.extend(b)
    primary.sort(key=lambda row: (row["componentIdentitySha256"], row["panelId"], row["windowId"], row["direction"]))
    sensitivity.sort(key=lambda row: (row["componentIdentitySha256"], row["panelId"], row["windowId"], row["direction"]))
    expected_sensitivity = sum(row["representative"]["component"]["timeframe"] == "M15" for row in components) * len(windows) * 2
    if len(primary) != EXPECTED_PRIMARY_ROW_COUNT or len(sensitivity) != expected_sensitivity:
        raise RuntimeError("V4 feature row count drift")
    if any("temporal_graph" in name or "outcome" in name.lower() for name in sys.modules) or any(name == prefix or name.startswith(prefix + ".") for name in sys.modules for prefix in ("requests", "httpx", "urllib3")):
        raise RuntimeError("V4 sterile extractor imported a prohibited module")
    return {"schemaVersion": "temporal_qd_component_surrogate_feature_corpus_v4", "correctionProtocolCanonicalPayloadSha256": protocol["correctionProtocolCanonicalPayloadSha256"], "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"], "authorityAddendumCanonicalPayloadSha256": authority["authorityAddendumCanonicalPayloadSha256"], "historicalExecutionEngineCommit": HISTORICAL_ENGINE_COMMIT, "windowIdentities": [{"panelId": w["panelId"], "windowId": w["windowId"], "frozenWindowSemanticSha256": w["binding"]["window_semantic_sha256"], "request": w["binding"]["request"], "archiveRawSha256s": sorted(a["rawSha256"] for a in w["archiveArtifacts"])} for w in windows], "componentImplementations": [{"componentIdentitySha256": c["componentIdentity"], "indicatorId": c["representative"]["component"]["indicatorId"], "contextCount": c["contextCount"], "implementation": c["implementation"]} for c in components], "primaryRows": primary, "m15NativeSensitivityRows": sensitivity, "componentOnlyBoundary": {"outcomePathArgument": False, "outcomeModuleImported": False, "temporalGraphImported": False, "networkClientImported": False, "allBarReadsFromRecoveredIsolatedRoots": True}, "eventSeriesRetention": "identity-only; S2 must recompute and hash-match this sealed raw event series before joining retained parent entries"}


def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("manifest", "authority_addendum", "recovery_report", "correction_protocol", "engine_root", "output"):
        parser.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists(): raise RuntimeError(f"refusing to overwrite V4 feature corpus: {args.output}")
    payload = run(args); payload["featureCorpusCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"featureCorpusCanonicalPayloadSha256": payload["featureCorpusCanonicalPayloadSha256"], "primaryRowCount": len(payload["primaryRows"]), "m15NativeSensitivityRowCount": len(payload["m15NativeSensitivityRows"])}, sort_keys=True))
    return 0


if __name__ == "__main__": raise SystemExit(main())
