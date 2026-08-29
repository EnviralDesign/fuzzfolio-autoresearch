"""Run one frozen V38 indicator/event projection without profile execution.

This command reads only one frozen screening task, its isolated M5 archive
files, and the archive-hash evidence supplied as arguments.  It intentionally
has no outcome-path argument and does not import a graph, replay, or outcome
module.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import math
from datetime import UTC
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from fuzzfolio_core.models.indicator import Indicator, IndicatorConfig, IndicatorMeta
from fuzzfolio_core.scoring_engine.indicators.indicator_factory import get_indicator_instance


EXPECTED_TASK_ID = "temporal-search-6667c9b783c2363417e6b38b4784e3d0"
EXPECTED_CANDIDATE_ID = "qd_28dba1f812d0cb5716ffe871a6ce"
EXPECTED_WINDOW_SEMANTIC = "sha256:fce37ff4b2469a0cdc9eeca306e6e98667a8b074f9eee07771f201f4effcc478"
EXPECTED_TASK_SOURCE_SHA256 = "32c4cd57b99cd00ee77da129b6299149d148a174cdef78e64bbe2b3dd21384b2"
EXPECTED_INSTANCE_ID = "long_trend_trigger"
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


def numeric_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def task_component(task_path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if sha256_file(task_path) != EXPECTED_TASK_SOURCE_SHA256:
        raise RuntimeError("frozen task source hash drift")
    with task_path.open(encoding="utf-8") as handle:
        first_line = handle.readline()
    if not first_line:
        raise RuntimeError("frozen task source is empty")
    task = json.loads(first_line)
    payload = task["payload"]
    if task.get("task_id") != EXPECTED_TASK_ID:
        raise RuntimeError(f"frozen task order drift: {task.get('task_id')!r}")
    if payload.get("candidate_id") != EXPECTED_CANDIDATE_ID:
        raise RuntimeError(f"frozen candidate drift: {payload.get('candidate_id')!r}")
    if payload.get("lake_window_semantic_sha256") != EXPECTED_WINDOW_SEMANTIC:
        raise RuntimeError("frozen window semantic drift")
    profile = payload["inline_profile_snapshot"]
    component = next(
        item for item in profile["indicators"]
        if item["meta"].get("instanceId") == EXPECTED_INSTANCE_ID
    )
    bindings = profile["graph"]["eventBindings"]
    if not isinstance(bindings, list):
        raise RuntimeError("frozen event bindings must be a list")
    binding = next(
        (item for item in bindings if item.get("indicatorInstanceId") == EXPECTED_INSTANCE_ID),
        None,
    )
    if binding is None:
        raise RuntimeError("frozen event binding drift")
    return task, component, binding


def load_isolated_m5(root: Path, evidence_path: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    expected_hashes = evidence["archiveRecovery"]["rawArtifactSha256"]["M5"]
    chunks: list[pd.DataFrame] = []
    checks: list[dict[str, Any]] = []
    for year, month in MONTHS:
        partition = root / "bars" / "pair=EURUSD" / "timeframe=M5" / f"year={year:04d}" / f"month={month:02d}"
        files = sorted(partition.glob("*.parquet")) if partition.is_dir() else []
        if len(files) != 1:
            raise RuntimeError(f"expected exactly one isolated M5 file in {partition}, found {len(files)}")
        path = files[0].resolve()
        if root not in path.parents:
            raise RuntimeError(f"refusing non-isolated bar read: {path}")
        raw_sha = sha256_file(path)
        expected_sha = str(expected_hashes[f"{year:04d}-{month:02d}"])
        if raw_sha != expected_sha:
            raise RuntimeError(f"archive raw hash mismatch: {path}")
        table = pq.ParquetFile(path).read(columns=["bar_start_s", "open", "high", "low", "close", "volume"])
        frame = table.to_pandas()
        chunks.append(frame)
        checks.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "rawSha256": raw_sha,
                "expectedArchiveRawSha256": expected_sha,
                "archiveRawSha256Matches": True,
            }
        )
    frame = pd.concat(chunks, ignore_index=True)
    frame["bar_start"] = pd.to_datetime(frame["bar_start_s"], unit="s", utc=True)
    if frame["bar_start"].duplicated().any():
        raise RuntimeError("isolated M5 archive contains duplicate bar starts")
    frame = frame.sort_values("bar_start", ascending=False).set_index("bar_start")
    if frame.empty or not frame.index.is_monotonic_decreasing:
        raise RuntimeError("isolated M5 frame is empty or not newest-first")
    return frame[["open", "high", "low", "close", "volume"]].copy(), checks


def projections(
    frame: pd.DataFrame,
    raw_outputs: dict[str, Any],
    scores: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    work = pd.DataFrame(index=frame.index.copy())
    for name, output_key in (("bullish", "bullish"), ("bearish", "bearish"), ("longScore", "long"), ("shortScore", "short")):
        values = np.asarray((raw_outputs if output_key in {"bullish", "bearish"} else scores)[output_key], dtype=float).reshape(-1)
        if len(values) != len(work):
            raise RuntimeError(f"indicator array length drift for {name}: {len(values)} != {len(work)}")
        work[name] = values
    work = work.loc[(work.index >= ANALYSIS_START) & (work.index < ANALYSIS_END)].sort_index(ascending=True)
    if work.empty:
        raise RuntimeError("frozen analysis interval has no isolated M5 rows")
    summaries: dict[str, dict[str, Any]] = {}
    for direction, score_column in (("long", "longScore"), ("short", "shortScore")):
        active = work[score_column].fillna(0.0).gt(1e-9)
        starts = active & ~active.shift(1, fill_value=False)
        work[f"{direction}Active"] = active
        work[f"{direction}EventStart"] = starts
        active_count = int(active.sum())
        start_count = int(starts.sum())
        summaries[direction] = {
            "barCount": int(len(work)),
            "activeBarCount": active_count,
            "activeFraction": active_count / len(work),
            "eventStartCount": start_count,
            "eventStartsPer1000Bars": start_count * 1000 / len(work),
            "eventStartShareOfActiveBars": (start_count / active_count) if active_count else None,
            "freshEventAvailability": None,
            "freshEventAvailabilityUnavailableReason": "runtime fresh_event is unavailable in a component-only projection",
        }
    rows: list[dict[str, Any]] = []
    for timestamp, row in work.iterrows():
        rows.append(
            {
                "barStart": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "bullish": numeric_or_none(row["bullish"]),
                "bearish": numeric_or_none(row["bearish"]),
                "longScore": numeric_or_none(row["longScore"]),
                "shortScore": numeric_or_none(row["shortScore"]),
                "longActive": bool(row["longActive"]),
                "shortActive": bool(row["shortActive"]),
                "longEventStart": bool(row["longEventStart"]),
                "shortEventStart": bool(row["shortEventStart"]),
            }
        )
    return rows, summaries


def run(args: argparse.Namespace) -> dict[str, Any]:
    task, component, binding = task_component(args.task_path.resolve())
    frame, raw_checks = load_isolated_m5(args.isolated_root.resolve(), args.archive_evidence.resolve())
    indicator = Indicator(
        meta=IndicatorMeta(**component["meta"]),
        docs=component.get("docs"),
        config=IndicatorConfig(**component["config"]),
    )
    instance = get_indicator_instance(indicator)
    raw_outputs = asyncio.run(instance.calculate(frame.copy(), return_full_array=True))
    scores = asyncio.run(instance.process(frame.copy(), return_full_array=True))
    if set(("bullish", "bearish")).difference(raw_outputs) or set(("long", "short")).difference(scores):
        raise RuntimeError("frozen directional output contract is unavailable")
    rows, summaries = projections(frame, raw_outputs, scores)
    component_payload = {
        "meta": component["meta"],
        "config": component["config"],
        "eventBinding": binding,
    }
    code_sources = [
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/trigger_indicators.py",
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/indicator_factory.py",
        args.engine_root / "shared/python/fuzzfolio_core/fuzzfolio_core/talib_runtime.py",
    ]
    for source in code_sources:
        if not source.is_file():
            raise RuntimeError(f"missing pinned indicator implementation source: {source}")
    projection_payload = {
        "schemaVersion": "temporal_qd_component_only_canary_projection_v1",
        "task": {
            "taskId": task["task_id"],
            "candidateId": task["payload"]["candidate_id"],
            "frozenWindowSemanticSha256": task["payload"]["lake_window_semantic_sha256"],
            "sourceSha256": "sha256:" + sha256_file(args.task_path.resolve()),
        },
        "componentCanonicalPayloadSha256": sha256_prefixed(canonical_bytes(component_payload)),
        "component": component_payload,
        "window": {
            "dataStart": "2022-05-11T00:00:00Z",
            "dataEnd": "2022-10-01T00:00:00Z",
            "analysisStart": ANALYSIS_START.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "analysisEnd": ANALYSIS_END.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sourceTimeframe": "M5",
            "completedBarOnly": True,
        },
        "rawArchiveChecks": raw_checks,
        "directionSummaries": summaries,
        "rows": rows,
    }
    projection_hash = sha256_prefixed(canonical_bytes(projection_payload))
    compressed = gzip.compress(canonical_bytes(projection_payload), compresslevel=9, mtime=0)
    args.projection_output.parent.mkdir(parents=True, exist_ok=True)
    args.projection_output.write_bytes(compressed)
    result_payload = {
        "schemaVersion": "temporal_qd_component_only_canary_result_v1",
        "historicalEngineCommit": "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2",
        "isolatedRoot": str(args.isolated_root.resolve()),
        "componentImplementationSources": [
            {"path": str(source), "sha256": "sha256:" + sha256_file(source)}
            for source in code_sources
        ],
        "projectionCanonicalPayloadSha256": projection_hash,
        "projectionGzipSha256": "sha256:" + hashlib.sha256(compressed).hexdigest(),
        "projectionRows": len(rows),
        "allBarReadsFromIsolatedRoot": True,
        "allArchiveRawHashesMatch": all(item["archiveRawSha256Matches"] for item in raw_checks),
        "noTemporalGraphImported": not any("temporal_graph" in name for name in __import__("sys").modules),
        "noOutcomePathArgument": True,
        "noStrategyOrEconomicExecution": True,
        "passes": True,
    }
    result_payload["resultCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(result_payload))
    return {"runId": args.run_id, "result": result_payload}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-path", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--archive-evidence", type=Path, required=True)
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--projection-output", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args)
    args.result_output.parent.mkdir(parents=True, exist_ok=True)
    args.result_output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"resultOutput": str(args.result_output), "passes": report["result"]["passes"], "projectionCanonicalPayloadSha256": report["result"]["projectionCanonicalPayloadSha256"], "resultCanonicalPayloadSha256": report["result"]["resultCanonicalPayloadSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
