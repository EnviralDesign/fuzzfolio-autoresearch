"""Project one actual frozen V38 inserted M5 event without running a graph.

Inputs are limited to the outcome-value-free V3 census, the pinned historical
engine/catalog, the isolated P3 archive, and archive-hash evidence.  This is a
component projection, not a profile, graph, strategy, or outcome calculation.
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
ANALYSIS_START = pd.Timestamp("2022-07-01T00:00:00Z")
ANALYSIS_END = pd.Timestamp("2022-10-01T00:00:00Z")
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


def verified_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = payload.pop("manifestCanonicalPayloadSha256", None)
    actual = sha256_prefixed(canonical_bytes(payload))
    if expected != actual:
        raise RuntimeError("extraction census self-hash mismatch")
    payload["manifestCanonicalPayloadSha256"] = expected
    return payload


def selected_context(manifest: dict[str, Any]) -> dict[str, Any]:
    selection = manifest["selectionRules"]["m5Canary"]
    selected_id = selection["selectedComponentContextIdentity"]
    contexts = [item for item in manifest["contexts"] if item["componentContextIdentity"] == selected_id]
    if len(contexts) != 1:
        raise RuntimeError("M5 canary census selection is not unique")
    context = contexts[0]
    component = context["component"]
    if component["timeframe"] != "M5":
        raise RuntimeError("M5 canary selection does not have M5 timeframe")
    if bool(component["fullConfiguration"].get("useFormingBar")):
        raise RuntimeError("M5 canary selection unexpectedly permits a forming bar")
    if context["candidateProfileBinding"].get("clockRequirement") != "clock.completed_bar":
        raise RuntimeError("M5 canary selection is not on the completed-bar clock")
    return context


def catalog_indicator(context: dict[str, Any]) -> tuple[Indicator, dict[str, Any]]:
    component = context["component"]
    catalog = INDICATORS_CONFIG.get("indicators")
    if not isinstance(catalog, list):
        raise RuntimeError("pinned indicator catalog has no indicators list")
    matches = [
        item for item in catalog
        if isinstance(item, dict)
        and isinstance(item.get("meta"), dict)
        and item["meta"].get("id") == component["indicatorId"]
    ]
    if len(matches) != 1:
        raise RuntimeError("pinned catalog cannot resolve the frozen indicator exactly once")
    catalog_item = matches[0]
    meta = dict(catalog_item["meta"])
    if meta.get("baseIndicatorId") != component["baseIndicatorId"]:
        raise RuntimeError("pinned catalog base indicator differs from frozen context")
    if meta.get("signalRole") != component["signalRole"]:
        raise RuntimeError("pinned catalog signal role differs from frozen context")
    if meta.get("signalPersistence") != component["signalPersistence"]:
        raise RuntimeError("pinned catalog persistence differs from frozen context")
    meta["instanceId"] = component["indicatorInstanceId"]
    indicator = Indicator(
        meta=IndicatorMeta(**meta),
        docs=catalog_item.get("docs"),
        config=IndicatorConfig(**component["fullConfiguration"]),
    )
    return indicator, catalog_item


def load_isolated_m5(root: Path, evidence_path: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    expected_hashes = evidence["archiveRecovery"]["rawArtifactSha256"]["M5"]
    chunks: list[pd.DataFrame] = []
    checks: list[dict[str, Any]] = []
    for year, month in MONTHS:
        partition = root / "bars" / "pair=EURUSD" / "timeframe=M5" / f"year={year:04d}" / f"month={month:02d}"
        files = sorted(partition.glob("*.parquet")) if partition.is_dir() else []
        if len(files) != 1:
            raise RuntimeError(f"expected one isolated M5 file in {partition}, found {len(files)}")
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
            "relativePath": path.relative_to(root).as_posix(),
            "rawSha256": raw_sha,
            "expectedArchiveRawSha256": expected_sha,
            "archiveRawSha256Matches": True,
        })
    frame = pd.concat(chunks, ignore_index=True)
    frame["bar_start"] = pd.to_datetime(frame["bar_start_s"], unit="s", utc=True)
    if frame["bar_start"].duplicated().any():
        raise RuntimeError("isolated M5 archive has duplicate bar starts")
    frame = frame.sort_values("bar_start", ascending=False).set_index("bar_start")
    if frame.empty or not frame.index.is_monotonic_decreasing:
        raise RuntimeError("isolated M5 frame is empty or not newest-first")
    return frame[["open", "high", "low", "close", "volume"]].copy(), checks


def event_bools(values: Any, *, expected_length: int, label: str) -> np.ndarray:
    raw = np.asarray(values).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    normalized: list[bool] = []
    for value in raw.tolist():
        if isinstance(value, (bool, np.bool_)):
            normalized.append(bool(value))
            continue
        if isinstance(value, (int, float, np.integer, np.floating)) and math.isfinite(float(value)) and float(value) in {0.0, 1.0}:
            normalized.append(bool(int(value)))
            continue
        raise RuntimeError(f"{label} is not a Boolean/exact-0-or-1 event output")
    return np.asarray(normalized, dtype=bool)


def score_values(values: Any, *, expected_length: int, label: str) -> np.ndarray:
    raw = np.asarray(values, dtype=float).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    return raw


def number_or_none(value: float) -> float | None:
    return float(value) if math.isfinite(float(value)) else None


def comparison(raw_events: pd.Series, scores: pd.Series) -> dict[str, int]:
    comparable = scores.notna()
    expected = raw_events.astype(float)
    equality = pd.Series(False, index=raw_events.index)
    equality.loc[comparable] = np.isclose(scores.loc[comparable], expected.loc[comparable], rtol=0.0, atol=0.0)
    return {
        "rows": int(len(raw_events)),
        "comparableRows": int(comparable.sum()),
        "exactEqualRows": int(equality.sum()),
        "mismatchRows": int((comparable & ~equality).sum()),
        "unavailableScoreRows": int((~comparable).sum()),
    }


def project(frame: pd.DataFrame, context: dict[str, Any], indicator: Indicator) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = context["insertion"]
    component = context["component"]
    instance = get_indicator_instance(indicator)
    raw_outputs = asyncio.run(instance.calculate(frame.copy(), return_full_array=True))
    processed = asyncio.run(instance.process(frame.copy(), return_full_array=True))
    long_key = component["eventOutputs"]["longOutput"]
    short_key = component["eventOutputs"]["shortOutput"]
    for key in (long_key, short_key):
        if key not in raw_outputs:
            raise RuntimeError(f"frozen bound raw output is missing: {key}")
    for key in ("long", "short"):
        if key not in processed:
            raise RuntimeError(f"frozen processed score is missing: {key}")
    work = pd.DataFrame(index=frame.index.copy())
    work["rawBoundLongOutput"] = event_bools(raw_outputs[long_key], expected_length=len(work), label=long_key)
    work["rawBoundShortOutput"] = event_bools(raw_outputs[short_key], expected_length=len(work), label=short_key)
    work["processedLongScore"] = score_values(processed["long"], expected_length=len(work), label="processed long")
    work["processedShortScore"] = score_values(processed["short"], expected_length=len(work), label="processed short")
    # Historical source audit: M5-to-M5 availability shift is zero, and raw
    # event output is not forward-filled. The component event is therefore the
    # aligned named raw binding output at this same completed-bar timestamp.
    work["componentEventLong"] = work["rawBoundLongOutput"]
    work["componentEventShort"] = work["rawBoundShortOutput"]
    work = work.loc[(work.index >= ANALYSIS_START) & (work.index < ANALYSIS_END)].sort_index(ascending=True)
    if work.empty:
        raise RuntimeError("isolated analysis interval has no M5 rows")
    rows = [
        {
            "barStart": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rawBoundLongOutput": bool(row["rawBoundLongOutput"]),
            "rawBoundShortOutput": bool(row["rawBoundShortOutput"]),
            "processedLongScore": number_or_none(row["processedLongScore"]),
            "processedShortScore": number_or_none(row["processedShortScore"]),
            "componentEventLong": bool(row["componentEventLong"]),
            "componentEventShort": bool(row["componentEventShort"]),
        }
        for timestamp, row in work.iterrows()
    ]
    summary = {
        "boundLongOutput": long_key,
        "boundShortOutput": short_key,
        "eventBindingId": binding["exactBindingId"],
        "eventBindingSide": context["side"],
        "rawBoundLongVsProcessedLong": comparison(work["rawBoundLongOutput"], work["processedLongScore"]),
        "rawBoundShortVsProcessedShort": comparison(work["rawBoundShortOutput"], work["processedShortScore"]),
        "componentEventLongEqualsRawBoundLong": bool((work["componentEventLong"] == work["rawBoundLongOutput"]).all()),
        "componentEventShortEqualsRawBoundShort": bool((work["componentEventShort"] == work["rawBoundShortOutput"]).all()),
        "componentEventLongCount": int(work["componentEventLong"].sum()),
        "componentEventShortCount": int(work["componentEventShort"].sum()),
    }
    return {"rows": rows, "summary": summary}, {"instance": instance, "rawOutputs": raw_outputs, "processed": processed}


def run(args: argparse.Namespace) -> dict[str, Any]:
    manifest = verified_manifest(args.manifest.resolve())
    context = selected_context(manifest)
    frame, raw_checks = load_isolated_m5(args.isolated_root.resolve(), args.archive_evidence.resolve())
    indicator, catalog_item = catalog_indicator(context)
    projection, runtime = project(frame, context, indicator)
    class_source = inspect.getsourcefile(type(runtime["instance"]))
    if not class_source:
        raise RuntimeError("cannot identify historical indicator implementation source")
    class_source_path = Path(class_source).resolve()
    source_files = [
        args.engine_root / "shared/constants/indicators.json",
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/indicator_factory.py",
        class_source_path,
    ]
    if not all(path.is_file() for path in source_files):
        raise RuntimeError("one or more pinned canary sources are missing")
    projection_payload = {
        "schemaVersion": "temporal_qd_actual_inserted_event_m5_canary_projection_v1",
        "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
        "context": context,
        "catalogIndicatorMetaSha256": sha256_prefixed(canonical_bytes(catalog_item["meta"])),
        "window": {
            "dataStart": "2022-05-11T00:00:00Z",
            "dataEnd": "2022-10-01T00:00:00Z",
            "analysisStart": ANALYSIS_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "analysisEnd": ANALYSIS_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sourceTimeframe": "M5",
            "completedBarOnly": True,
            "frozenWindowSemanticSha256": EXPECTED_WINDOW_SEMANTIC,
        },
        "rawArchiveChecks": raw_checks,
        "projection": projection,
    }
    projection_hash = sha256_prefixed(canonical_bytes(projection_payload))
    compressed = gzip.compress(canonical_bytes(projection_payload), compresslevel=9, mtime=0)
    args.projection_output.parent.mkdir(parents=True, exist_ok=True)
    args.projection_output.write_bytes(compressed)
    result = {
        "schemaVersion": "temporal_qd_actual_inserted_event_m5_canary_result_v1",
        "historicalEngineCommit": "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2",
        "projectionCanonicalPayloadSha256": projection_hash,
        "projectionGzipSha256": "sha256:" + hashlib.sha256(compressed).hexdigest(),
        "projectionRows": len(projection["rows"]),
        "componentImplementationSources": [
            {"path": str(path), "sha256": "sha256:" + sha256_file(path)}
            for path in source_files
        ],
        "allBarReadsFromIsolatedRoot": True,
        "allArchiveRawHashesMatch": all(item["archiveRawSha256Matches"] for item in raw_checks),
        "noTemporalGraphImported": not any("temporal_graph" in name for name in sys.modules),
        "noOutcomePathArgument": True,
        "noStrategyOrEconomicExecution": True,
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
