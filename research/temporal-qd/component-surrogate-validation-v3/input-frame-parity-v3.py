"""Exercise every frozen component against the explicit isolated P3 input frame."""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from pathlib import Path
from typing import Any

from component_projection_support_v3 import (
    calculate_component,
    canonical_bytes,
    full_series,
    load_isolated_bars,
    normalize_raw_event,
    read_self_hashed_json,
    sha256_prefixed,
)


MONTHS = tuple((2022, month) for month in range(5, 10))
PAIR = "EURUSD"
TIMEFRAMES = ("M5", "M15")


def frame_summary(frame: Any, raw_checks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "barCount": len(frame),
        "columns": list(frame.columns),
        "dtypes": {column: str(dtype) for column, dtype in frame.dtypes.items()},
        "indexTimezone": str(frame.index.tz),
        "indexNewestFirst": bool(frame.index.is_monotonic_decreasing),
        "duplicateBarStarts": bool(frame.index.has_duplicates),
        "rawArchiveChecks": raw_checks,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    manifest = read_self_hashed_json(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    authority = read_self_hashed_json(
        args.authority_addendum.resolve(),
        "authorityAddendumCanonicalPayloadSha256",
    )
    if authority["authority"]["extractionCensusCanonicalPayloadSha256"] != manifest["manifestCanonicalPayloadSha256"]:
        raise RuntimeError("input-frame parity manifest does not match its projection authority")
    frames: dict[str, Any] = {}
    frame_checks: dict[str, Any] = {}
    for timeframe in TIMEFRAMES:
        frame, raw_checks = load_isolated_bars(
            root=args.isolated_root,
            archive_evidence=args.archive_evidence,
            pair=PAIR,
            timeframe=timeframe,
            months=MONTHS,
        )
        frames[timeframe] = frame
        frame_checks[timeframe] = frame_summary(frame, raw_checks)

    components: list[dict[str, Any]] = []
    for context in sorted(manifest["contexts"], key=lambda value: value["componentContextIdentity"]):
        component = context["component"]
        frame = frames[component["timeframe"]]
        raw_outputs, processed, instance = calculate_component(frame, context)
        long_key = component["eventOutputs"]["longOutput"]
        short_key = component["eventOutputs"]["shortOutput"]
        if long_key not in raw_outputs or short_key not in raw_outputs:
            raise RuntimeError(f"bound raw output is absent for {component['indicatorId']}")
        raw_long, raw_long_missing = normalize_raw_event(
            raw_outputs[long_key], expected_length=len(frame), label=f"{component['indicatorId']}:{long_key}"
        )
        raw_short, raw_short_missing = normalize_raw_event(
            raw_outputs[short_key], expected_length=len(frame), label=f"{component['indicatorId']}:{short_key}"
        )
        processed_long = full_series(
            processed.get("long"), expected_length=len(frame), label=f"{component['indicatorId']}:processed-long"
        )
        processed_short = full_series(
            processed.get("short"), expected_length=len(frame), label=f"{component['indicatorId']}:processed-short"
        )
        required_columns = sorted(str(value) for value in getattr(instance, "required_columns", set()))
        if not set(required_columns).issubset(frame.columns):
            raise RuntimeError(f"historical input frame misses declared columns for {component['indicatorId']}")
        source = Path(inspect.getsourcefile(type(instance)) or "").resolve()
        components.append({
            "componentContextIdentity": context["componentContextIdentity"],
            "indicatorId": component["indicatorId"],
            "timeframe": component["timeframe"],
            "implementationClass": type(instance).__name__,
            "implementationSource": str(source),
            "requiredColumns": required_columns,
            "rawBoundLongOutput": long_key,
            "rawBoundShortOutput": short_key,
            "rawLongMissingCoercedFalse": raw_long_missing,
            "rawShortMissingCoercedFalse": raw_short_missing,
            "rawLongActiveBarCount": int(raw_long.sum()),
            "rawShortActiveBarCount": int(raw_short.sum()),
            "processedLongLength": len(processed_long),
            "processedShortLength": len(processed_short),
        })
    if len(components) != 41:
        raise RuntimeError(f"input-frame parity ran {len(components)} contexts instead of 41")
    if len({row["indicatorId"] for row in components}) != 19:
        raise RuntimeError("input-frame parity did not cover all 19 component identities")
    if any("temporal_graph" in module_name for module_name in sys.modules):
        raise RuntimeError("component input parity imported a TemporalGraph module")
    return {
        "schemaVersion": "temporal_qd_component_surrogate_input_frame_parity_v3",
        "manifestCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
        "authorityAddendumCanonicalPayloadSha256": authority["authorityAddendumCanonicalPayloadSha256"],
        "isolatedRoot": str(args.isolated_root.resolve()),
        "pair": PAIR,
        "months": [f"{year:04d}-{month:02d}" for year, month in MONTHS],
        "frames": frame_checks,
        "components": components,
        "allBarReadsFromIsolatedRoot": True,
        "allArchiveRawHashesMatch": True,
        "noTemporalGraphImported": True,
        "noOutcomePathArgument": True,
        "passes": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--authority-addendum", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--archive-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite input-frame parity output: {args.output}")
    report = run(args)
    report["canonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(report))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "componentContextCount": len(report["components"]),
        "canonicalPayloadSha256": report["canonicalPayloadSha256"],
        "passes": report["passes"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
