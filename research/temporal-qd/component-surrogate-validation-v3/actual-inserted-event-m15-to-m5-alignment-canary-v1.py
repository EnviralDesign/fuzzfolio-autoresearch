"""Project one frozen M15 event component to the historical M5 decision clock.

This is a narrowly-scoped, read-only component canary.  It intentionally does
not import the TemporalGraph runtime or inspect any evaluated outcome.  The
only inputs are the outcome-value-free V3 census, pinned historical indicator
code, and checksum-verified isolated P3 bars.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import inspect
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from fuzzfolio_core.models.common import INDICATORS_CONFIG
from fuzzfolio_core.models.indicator import Indicator, IndicatorConfig, IndicatorMeta
from fuzzfolio_core.scoring_engine.indicators.indicator_factory import get_indicator_instance


EXPECTED_WINDOW_SEMANTIC = "sha256:fce37ff4b2469a0cdc9eeca306e6e98667a8b074f9eee07771f201f4effcc478"
SOURCE_AUDIT_CANONICAL_PAYLOAD = "sha256:9c3261e460b5a202c4ff31ff5c390cd80e62efc358c669e7daabc637c3c82b52"
ANALYSIS_START = pd.Timestamp("2022-07-01T00:00:00Z")
ANALYSIS_END = pd.Timestamp("2022-10-01T00:00:00Z")
M5_MINUTES = 5
M15_MINUTES = 15
AVAILABILITY_SHIFT_MINUTES = M15_MINUTES - M5_MINUTES
MONTHS = tuple((2022, month) for month in range(5, 10))


def canonical_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_prefixed(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_timestamp(value: pd.Timestamp | None) -> str | None:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ") if value is not None and not pd.isna(value) else None


def number_or_none(value: float) -> float | None:
    return float(value) if math.isfinite(float(value)) else None


def verified_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = payload.pop("manifestCanonicalPayloadSha256", None)
    actual = sha256_prefixed(canonical_bytes(payload))
    if expected != actual:
        raise RuntimeError("extraction census self-hash mismatch")
    payload["manifestCanonicalPayloadSha256"] = expected
    return payload


def selected_context(manifest: dict[str, Any]) -> dict[str, Any]:
    selected_id = manifest["selectionRules"]["m15Canary"]["selectedComponentContextIdentity"]
    matches = [item for item in manifest["contexts"] if item["componentContextIdentity"] == selected_id]
    if len(matches) != 1:
        raise RuntimeError("M15 canary census selection is not unique")
    context = matches[0]
    component = context["component"]
    if component["indicatorId"] != "MARKET_MODE_TRANSITION" or component["timeframe"] != "M15":
        raise RuntimeError("M15 canary selection is not the frozen MARKET_MODE_TRANSITION context")
    if bool(component["fullConfiguration"].get("useFormingBar")):
        raise RuntimeError("M15 canary selection unexpectedly permits a forming bar")
    if context["candidateProfileBinding"].get("clockRequirement") != "clock.completed_bar":
        raise RuntimeError("M15 canary selection is not on the completed-bar clock")
    return context


def catalog_indicator(context: dict[str, Any]) -> tuple[Indicator, dict[str, Any]]:
    component = context["component"]
    catalog = INDICATORS_CONFIG.get("indicators")
    if not isinstance(catalog, list):
        raise RuntimeError("pinned indicator catalog has no indicators list")
    matches = [
        item for item in catalog
        if isinstance(item, dict) and isinstance(item.get("meta"), dict) and item["meta"].get("id") == component["indicatorId"]
    ]
    if len(matches) != 1:
        raise RuntimeError("pinned catalog cannot resolve the frozen indicator exactly once")
    catalog_item = matches[0]
    meta = dict(catalog_item["meta"])
    for key in ("baseIndicatorId", "signalRole", "signalPersistence"):
        if meta.get(key) != component[key]:
            raise RuntimeError(f"pinned catalog {key} differs from frozen context")
    meta["instanceId"] = component["indicatorInstanceId"]
    return Indicator(meta=IndicatorMeta(**meta), docs=catalog_item.get("docs"), config=IndicatorConfig(**component["fullConfiguration"])), catalog_item


def load_isolated_bars(root: Path, evidence_path: Path, timeframe: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    expected_hashes = evidence["archiveRecovery"]["rawArtifactSha256"][timeframe]
    chunks: list[pd.DataFrame] = []
    checks: list[dict[str, Any]] = []
    for year, month in MONTHS:
        partition = root / "bars" / "pair=EURUSD" / f"timeframe={timeframe}" / f"year={year:04d}" / f"month={month:02d}"
        files = sorted(partition.glob("*.parquet")) if partition.is_dir() else []
        if len(files) != 1:
            raise RuntimeError(f"expected one isolated {timeframe} file in {partition}, found {len(files)}")
        path = files[0].resolve()
        if root not in path.parents:
            raise RuntimeError(f"refusing non-isolated bar read: {path}")
        raw_sha = sha256_file(path)
        expected_sha = str(expected_hashes[f"{year:04d}-{month:02d}"])
        if raw_sha != expected_sha:
            raise RuntimeError(f"archive raw hash mismatch: {path}")
        table = pq.ParquetFile(path).read(columns=["bar_start_s", "open", "high", "low", "close", "volume"])
        chunks.append(table.to_pandas())
        checks.append({
            "timeframe": timeframe,
            "relativePath": path.relative_to(root).as_posix(),
            "rawSha256": raw_sha,
            "expectedArchiveRawSha256": expected_sha,
            "archiveRawSha256Matches": True,
        })
    frame = pd.concat(chunks, ignore_index=True)
    frame["bar_start"] = pd.to_datetime(frame["bar_start_s"], unit="s", utc=True)
    if frame["bar_start"].duplicated().any():
        raise RuntimeError(f"isolated {timeframe} archive has duplicate bar starts")
    frame = frame.sort_values("bar_start", ascending=False).set_index("bar_start")
    if frame.empty or not frame.index.is_monotonic_decreasing:
        raise RuntimeError(f"isolated {timeframe} frame is empty or not newest-first")
    return frame[["open", "high", "low", "close", "volume"]].copy(), checks


def visual_event_bools(values: Any, *, expected_length: int, label: str) -> tuple[np.ndarray, int]:
    raw = np.asarray(values).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    normalized: list[bool] = []
    warmup_missing = 0
    for value in raw.tolist():
        if isinstance(value, (bool, np.bool_)):
            normalized.append(bool(value))
        elif isinstance(value, (int, float, np.integer, np.floating)) and math.isfinite(float(value)) and float(value) in {0.0, 1.0}:
            normalized.append(bool(int(value)))
        elif value is None or pd.isna(value):
            # Historical aligned_scoring._build_output_series turns a missing
            # visual event sample into False before EventAdapter validates its
            # Boolean/exact-0-or-1 event series.  Preserve that exact source
            # boundary rather than treating indicator warm-up as an event.
            normalized.append(False)
            warmup_missing += 1
        else:
            raise RuntimeError(f"{label} is not a Boolean/exact-0-or-1 event output")
    return np.asarray(normalized, dtype=bool), warmup_missing


def score_values(values: Any, *, expected_length: int, label: str) -> np.ndarray:
    raw = np.asarray(values, dtype=float).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    return raw


def raw_event_alignment(source: pd.Series, m5_index: pd.DatetimeIndex, *, shift_minutes: int) -> pd.Series:
    shifted = source.copy()
    shifted.index = shifted.index + pd.Timedelta(minutes=shift_minutes)
    reindexed = shifted.sort_index().reindex(m5_index)
    return pd.Series(
        [bool(value) if value is not None and not pd.isna(value) else False for value in reindexed.tolist()],
        index=m5_index,
        dtype=bool,
    )


def continuous_score_alignment(source: pd.Series, m5_index: pd.DatetimeIndex, *, shift_minutes: int) -> pd.Series:
    shifted = source.copy()
    shifted.index = shifted.index + pd.Timedelta(minutes=shift_minutes)
    return shifted.sort_index().reindex(m5_index, method="ffill").fillna(0.0).astype(float)


def source_timestamp_alignment(native_index: pd.DatetimeIndex, m5_index: pd.DatetimeIndex) -> pd.Series:
    shifted = pd.Series(native_index, index=native_index + pd.Timedelta(minutes=AVAILABILITY_SHIFT_MINUTES))
    return shifted.sort_index().reindex(m5_index)


def validate_adversarial_alignment(native: pd.DataFrame, m5_index: pd.DatetimeIndex, aligned_long: pd.Series, aligned_short: pd.Series) -> dict[str, Any]:
    active = native[(native["rawBoundLongOutput"] | native["rawBoundShortOutput"]) & (native.index >= ANALYSIS_START) & (native.index < ANALYSIS_END)]
    testable: list[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, bool, bool]] = []
    m5_set = set(m5_index)
    for source_timestamp, row in active.iterrows():
        before = source_timestamp + pd.Timedelta(minutes=M5_MINUTES)
        first_allowed = source_timestamp + pd.Timedelta(minutes=AVAILABILITY_SHIFT_MINUTES)
        shifted_allowed = source_timestamp + pd.Timedelta(minutes=AVAILABILITY_SHIFT_MINUTES + M5_MINUTES)
        if before in m5_set and first_allowed in m5_set and shifted_allowed in m5_set:
            testable.append((source_timestamp, before, first_allowed, bool(row["rawBoundLongOutput"]), bool(row["rawBoundShortOutput"])))
    if not testable:
        raise RuntimeError("no active M15 raw event could be tested on the isolated M5 decision clock")
    before_violations = sum(
        int(bool(aligned_long.loc[before]) or bool(aligned_short.loc[before]))
        for _, before, _, _, _ in testable
    )
    first_allowed_mismatches = sum(
        int(bool(aligned_long.loc[first_allowed]) != long_event or bool(aligned_short.loc[first_allowed]) != short_event)
        for _, _, first_allowed, long_event, short_event in testable
    )
    shifted_long = raw_event_alignment(native["rawBoundLongOutput"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES + M5_MINUTES)
    shifted_short = raw_event_alignment(native["rawBoundShortOutput"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES + M5_MINUTES)
    shifted_difference_rows = int(((shifted_long != aligned_long) | (shifted_short != aligned_short)).sum())
    if before_violations or first_allowed_mismatches or shifted_difference_rows <= 0:
        raise RuntimeError("M15-to-M5 timing adversarial test failed")
    return {
        "activeNativeEventRows": int(len(active)),
        "testableActiveNativeEventRows": int(len(testable)),
        "m5BeforeM15CompletionCanSeeItViolations": before_violations,
        "firstAllowedM5ObservationMismatches": first_allowed_mismatches,
        "shiftedSourceTimestampMinutes": M5_MINUTES,
        "shiftedSourceChangesAlignedProjectionRows": shifted_difference_rows,
        "passes": True,
    }


def project(m15_frame: pd.DataFrame, m5_frame: pd.DataFrame, context: dict[str, Any], indicator: Indicator) -> tuple[dict[str, Any], dict[str, Any]]:
    component = context["component"]
    instance = get_indicator_instance(indicator)
    raw_outputs = asyncio.run(instance.calculate(m15_frame.copy(), return_full_array=True))
    processed = asyncio.run(instance.process(m15_frame.copy(), return_full_array=True))
    long_key = component["eventOutputs"]["longOutput"]
    short_key = component["eventOutputs"]["shortOutput"]
    for key in (long_key, short_key):
        if key not in raw_outputs:
            raise RuntimeError(f"frozen bound raw output is missing: {key}")
    native = pd.DataFrame(index=m15_frame.index.copy())
    native["rawBoundLongOutput"], long_warmup_missing = visual_event_bools(raw_outputs[long_key], expected_length=len(native), label=long_key)
    native["rawBoundShortOutput"], short_warmup_missing = visual_event_bools(raw_outputs[short_key], expected_length=len(native), label=short_key)
    native["processedLongScore"] = score_values(processed["long"], expected_length=len(native), label="processed long")
    native["processedShortScore"] = score_values(processed["short"], expected_length=len(native), label="processed short")
    native = native.sort_index(ascending=True)
    m5_index = m5_frame.index.sort_values()
    aligned = pd.DataFrame(index=m5_index)
    aligned["rawBoundLongOutput"] = raw_event_alignment(native["rawBoundLongOutput"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES)
    aligned["rawBoundShortOutput"] = raw_event_alignment(native["rawBoundShortOutput"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES)
    aligned["processedLongScore"] = continuous_score_alignment(native["processedLongScore"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES)
    aligned["processedShortScore"] = continuous_score_alignment(native["processedShortScore"], m5_index, shift_minutes=AVAILABILITY_SHIFT_MINUTES)
    aligned["componentEventLong"] = aligned["rawBoundLongOutput"]
    aligned["componentEventShort"] = aligned["rawBoundShortOutput"]
    aligned["componentEventSourceM15BarStart"] = source_timestamp_alignment(native.index, m5_index)
    native_analysis = native.loc[(native.index >= ANALYSIS_START) & (native.index < ANALYSIS_END)]
    aligned_analysis = aligned.loc[(aligned.index >= ANALYSIS_START) & (aligned.index < ANALYSIS_END)]
    if native_analysis.empty or aligned_analysis.empty:
        raise RuntimeError("isolated analysis interval has no native M15 or aligned M5 rows")
    canary = validate_adversarial_alignment(native, aligned_analysis.index, aligned_analysis["componentEventLong"], aligned_analysis["componentEventShort"])
    native_rows = [
        {
            "m15ComponentBarStart": iso_timestamp(timestamp),
            "rawBoundLongOutput": bool(row["rawBoundLongOutput"]),
            "rawBoundShortOutput": bool(row["rawBoundShortOutput"]),
            "processedLongScore": number_or_none(row["processedLongScore"]),
            "processedShortScore": number_or_none(row["processedShortScore"]),
        }
        for timestamp, row in native_analysis.iterrows()
    ]
    aligned_rows = [
        {
            "m5DecisionBarStart": iso_timestamp(timestamp),
            "componentEventSourceM15BarStart": iso_timestamp(row["componentEventSourceM15BarStart"]),
            "rawBoundLongOutput": bool(row["rawBoundLongOutput"]),
            "rawBoundShortOutput": bool(row["rawBoundShortOutput"]),
            "processedLongScore": number_or_none(row["processedLongScore"]),
            "processedShortScore": number_or_none(row["processedShortScore"]),
            "componentEventLong": bool(row["componentEventLong"]),
            "componentEventShort": bool(row["componentEventShort"]),
        }
        for timestamp, row in aligned_analysis.iterrows()
    ]
    return {
        "nativeM15Rows": native_rows,
        "alignedM5Rows": aligned_rows,
        "summary": {
            "boundLongOutput": long_key,
            "boundShortOutput": short_key,
            "eventBindingId": context["insertion"]["exactBindingId"],
            "eventBindingSide": context["side"],
            "sourceClock": "M15.completed_bar_start",
            "decisionClock": "M5.completed_bar_start",
            "availabilityShiftMinutes": AVAILABILITY_SHIFT_MINUTES,
            "rawEventAlignment": "exact_reindex_without_forward_fill",
            "processedScoreAlignment": "forward_fill_after_same_availability_shift",
            "rawIndicatorWarmupMissingSamplesCoercedFalseAtHistoricalVisualBoundary": {
                "long": long_warmup_missing,
                "short": short_warmup_missing,
            },
            "componentEventLongCount": int(aligned_analysis["componentEventLong"].sum()),
            "componentEventShortCount": int(aligned_analysis["componentEventShort"].sum()),
            "canary": canary,
        },
    }, {"instance": instance}


def run(args: argparse.Namespace) -> dict[str, Any]:
    manifest = verified_manifest(args.manifest.resolve())
    context = selected_context(manifest)
    m15_frame, m15_checks = load_isolated_bars(args.isolated_root.resolve(), args.archive_evidence.resolve(), "M15")
    m5_frame, m5_checks = load_isolated_bars(args.isolated_root.resolve(), args.archive_evidence.resolve(), "M5")
    indicator, catalog_item = catalog_indicator(context)
    projection, runtime = project(m15_frame, m5_frame, context, indicator)
    class_source = inspect.getsourcefile(type(runtime["instance"]))
    if not class_source:
        raise RuntimeError("cannot identify historical indicator implementation source")
    source_files = [
        args.engine_root / "shared/constants/indicators.json",
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/compute/aligned_scoring.py",
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/indicator_factory.py",
        Path(class_source).resolve(),
    ]
    if not all(path.is_file() for path in source_files):
        raise RuntimeError("one or more pinned canary sources are missing")
    projection_payload = {
        "schemaVersion": "temporal_qd_actual_inserted_event_m15_to_m5_alignment_canary_projection_v1",
        "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
        "historicalEventProjectionAndClockAuditCanonicalPayloadSha256": SOURCE_AUDIT_CANONICAL_PAYLOAD,
        "context": context,
        "catalogIndicatorMetaSha256": sha256_prefixed(canonical_bytes(catalog_item["meta"])),
        "window": {
            "dataStart": "2022-05-11T00:00:00Z",
            "dataEnd": "2022-10-01T00:00:00Z",
            "analysisStart": iso_timestamp(ANALYSIS_START),
            "analysisEnd": iso_timestamp(ANALYSIS_END),
            "sourceTimeframe": "M15",
            "decisionTimeframe": "M5",
            "completedBarOnly": True,
            "frozenWindowSemanticSha256": EXPECTED_WINDOW_SEMANTIC,
        },
        "rawArchiveChecks": m15_checks + m5_checks,
        "projection": projection,
    }
    projection_hash = sha256_prefixed(canonical_bytes(projection_payload))
    compressed = gzip.compress(canonical_bytes(projection_payload), compresslevel=9, mtime=0)
    args.projection_output.parent.mkdir(parents=True, exist_ok=True)
    args.projection_output.write_bytes(compressed)
    result = {
        "schemaVersion": "temporal_qd_actual_inserted_event_m15_to_m5_alignment_canary_result_v1",
        "historicalEngineCommit": "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2",
        "projectionCanonicalPayloadSha256": projection_hash,
        "projectionGzipSha256": sha256_prefixed(compressed),
        "nativeM15ProjectionRows": len(projection["nativeM15Rows"]),
        "alignedM5ProjectionRows": len(projection["alignedM5Rows"]),
        "componentImplementationSources": [{"path": str(path), "sha256": "sha256:" + sha256_file(path)} for path in source_files],
        "allBarReadsFromIsolatedRoot": True,
        "allArchiveRawHashesMatch": all(item["archiveRawSha256Matches"] for item in m15_checks + m5_checks),
        "noTemporalGraphImported": not any("temporal_graph" in name for name in sys.modules),
        "noOutcomePathArgument": True,
        "noStrategyOrEconomicExecution": True,
        "timingCanary": projection["summary"]["canary"],
        "passes": True,
    }
    result["resultCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(result))
    return {"runId": args.run_id, "result": result}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--archive-evidence", type=Path, required=True)
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--projection-output", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    args = parser.parse_args()
    if args.projection_output.exists() or args.result_output.exists():
        raise RuntimeError("refusing to overwrite a canary output")
    report = run(args)
    args.result_output.parent.mkdir(parents=True, exist_ok=True)
    args.result_output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"passes": report["result"]["passes"], "projectionCanonicalPayloadSha256": report["result"]["projectionCanonicalPayloadSha256"], "resultCanonicalPayloadSha256": report["result"]["resultCanonicalPayloadSha256"], "resultOutput": str(args.result_output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
