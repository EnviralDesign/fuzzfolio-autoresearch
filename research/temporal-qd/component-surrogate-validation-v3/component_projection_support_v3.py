"""Small shared primitives for the outcome-free V3 component projection tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from fuzzfolio_core.models.common import INDICATORS_CONFIG
from fuzzfolio_core.models.indicator import Indicator, IndicatorConfig, IndicatorMeta
from fuzzfolio_core.scoring_engine.indicators.indicator_factory import get_indicator_instance


REQUIRED_BAR_COLUMNS = ("open", "high", "low", "close", "volume")


def canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_prefixed(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_self_hashed_json(path: Path, field: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = payload.pop(field, None)
    actual = sha256_prefixed(canonical_bytes(payload))
    if expected != actual:
        raise RuntimeError(f"self-hash mismatch for {path.name}: {expected!r} != {actual!r}")
    payload[field] = expected
    return payload


def catalog_indicator(context: dict[str, Any]) -> Indicator:
    component = context["component"]
    catalog = INDICATORS_CONFIG.get("indicators")
    if not isinstance(catalog, list):
        raise RuntimeError("pinned historical indicator catalog is malformed")
    matches = [
        item
        for item in catalog
        if isinstance(item, dict)
        and isinstance(item.get("meta"), dict)
        and item["meta"].get("id") == component["indicatorId"]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"historical catalog cannot resolve {component['indicatorId']!r} exactly once")
    catalog_item = matches[0]
    meta = dict(catalog_item["meta"])
    meta["instanceId"] = component["indicatorInstanceId"]
    return Indicator(
        meta=IndicatorMeta(**meta),
        docs=catalog_item.get("docs"),
        config=IndicatorConfig(**component["fullConfiguration"]),
    )


def load_isolated_bars(
    *,
    root: Path,
    archive_evidence: Path,
    pair: str,
    timeframe: str,
    months: tuple[tuple[int, int], ...],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    root = root.resolve()
    evidence = json.loads(archive_evidence.read_text(encoding="utf-8"))
    expected_hashes = evidence["archiveRecovery"]["rawArtifactSha256"][timeframe]
    chunks: list[pd.DataFrame] = []
    checks: list[dict[str, Any]] = []
    for year, month in months:
        partition = (
            root
            / "bars"
            / f"pair={pair}"
            / f"timeframe={timeframe}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )
        files = sorted(partition.glob("*.parquet")) if partition.is_dir() else []
        if len(files) != 1:
            raise RuntimeError(f"expected one {timeframe} archive file in {partition}, found {len(files)}")
        path = files[0].resolve()
        if root not in path.parents:
            raise RuntimeError(f"refusing non-isolated bar read: {path}")
        expected = "sha256:" + str(expected_hashes[f"{year:04d}-{month:02d}"])
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(f"archive raw hash mismatch: {path}")
        table = pq.ParquetFile(path).read(columns=["bar_start_s", *REQUIRED_BAR_COLUMNS])
        chunks.append(table.to_pandas())
        checks.append({
            "relativePath": path.relative_to(root).as_posix(),
            "archiveRawSha256": actual,
            "archiveRawSha256Matches": True,
        })
    frame = pd.concat(chunks, ignore_index=True)
    frame["bar_start"] = pd.to_datetime(frame.pop("bar_start_s"), unit="s", utc=True)
    if frame["bar_start"].duplicated().any():
        raise RuntimeError(f"isolated {timeframe} frame has duplicate bar starts")
    frame = frame.sort_values("bar_start", ascending=False).set_index("bar_start")
    if frame.empty or not frame.index.is_monotonic_decreasing or frame.index.tz is None:
        raise RuntimeError(f"isolated {timeframe} frame is not a nonempty newest-first UTC frame")
    for column in REQUIRED_BAR_COLUMNS:
        if not pd.api.types.is_numeric_dtype(frame[column]):
            raise RuntimeError(f"isolated {timeframe} {column} is not numeric")
    # Input metadata is explicit rather than inferred from an ambient loader.
    frame["pair"] = pair
    return frame[[*REQUIRED_BAR_COLUMNS, "pair"]].copy(), checks


def normalize_raw_event(values: Any, *, expected_length: int, label: str) -> tuple[np.ndarray, int]:
    raw = np.asarray(values).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    normalized: list[bool] = []
    missing_count = 0
    for value in raw.tolist():
        if value is None or (isinstance(value, (float, np.floating)) and math.isnan(float(value))):
            # This is the exact historical visual-alignment boundary rule.
            normalized.append(False)
            missing_count += 1
            continue
        if isinstance(value, (bool, np.bool_)):
            normalized.append(bool(value))
            continue
        if isinstance(value, (int, float, np.integer, np.floating)) and math.isfinite(float(value)) and float(value) in {0.0, 1.0}:
            normalized.append(bool(int(value)))
            continue
        raise RuntimeError(f"{label} has non-Boolean/non-exact-0-or-1 raw event value: {value!r}")
    return np.asarray(normalized, dtype=bool), missing_count


def full_series(values: Any, *, expected_length: int, label: str) -> np.ndarray:
    raw = np.asarray(values).reshape(-1)
    if len(raw) != expected_length:
        raise RuntimeError(f"{label} length drift: {len(raw)} != {expected_length}")
    return raw


def calculate_component(
    frame: pd.DataFrame,
    context: dict[str, Any],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], Any]:
    instance = get_indicator_instance(catalog_indicator(context))
    raw_outputs = asyncio.run(instance.calculate(frame.copy(), return_full_array=True))
    processed = asyncio.run(instance.process(frame.copy(), return_full_array=True))
    if not isinstance(raw_outputs, dict) or not isinstance(processed, dict):
        raise RuntimeError("historical component calculation did not return mappings")
    return raw_outputs, processed, instance
