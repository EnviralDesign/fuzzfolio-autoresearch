"""Calibration check for the play-hand calendar robustness gate.

Loads runs/derived/attempt-catalog.csv, dedupes to one row per strategy family
with an existing 36mo calendar curve, applies compute_calendar_robustness to
each curve, and prints keep-rates at the default gate thresholds.

Usage:
    uv run python scripts/calibration/check_calendar_gate_against_corpus.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from autoresearch.calendar_robustness import (  # noqa: E402
    DEFAULT_MAX_RECENT_THIRD_SHARE,
    DEFAULT_MIN_SEGMENTS_POSITIVE,
    compute_calendar_robustness,
    evaluate_calendar_gate,
)

CATALOG_PATH = REPO_ROOT / "runs" / "derived" / "attempt-catalog.csv"


def _curve_points(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    curve = payload.get("curve") if isinstance(payload.get("curve"), dict) else {}
    points = curve.get("points") if isinstance(curve, dict) else None
    return [point for point in points if isinstance(point, dict)] if isinstance(points, list) else []


def main() -> int:
    if not CATALOG_PATH.exists():
        print(f"attempt catalog not found: {CATALOG_PATH}")
        return 1

    family_rows: dict[str, Path] = {}
    with CATALOG_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            raw_path = (row.get("full_backtest_calendar_curve_path_36m") or "").strip()
            if not raw_path:
                continue
            curve_path = Path(raw_path)
            if not curve_path.exists():
                continue
            family_id = (row.get("strategy_family_id") or row.get("run_id") or "").strip()
            if not family_id or family_id in family_rows:
                continue
            family_rows[family_id] = curve_path

    total = len(family_rows)
    if total == 0:
        print("no families with existing 36mo calendar curves found")
        return 1

    insufficient = 0
    pass_segments = 0
    pass_recency = 0
    pass_combined = 0
    for curve_path in family_rows.values():
        robustness = compute_calendar_robustness(_curve_points(curve_path))
        decision = evaluate_calendar_gate(robustness)
        if not robustness.sufficient:
            insufficient += 1
            # Insufficient curves pass the gate with a warning by design.
            pass_segments += 1
            pass_recency += 1
            pass_combined += 1
            continue
        segments_ok = robustness.segments_positive >= DEFAULT_MIN_SEGMENTS_POSITIVE
        recency_ok = (
            robustness.recent_third_share is None
            or robustness.recent_third_share <= DEFAULT_MAX_RECENT_THIRD_SHARE
        )
        if segments_ok:
            pass_segments += 1
        if recency_ok:
            pass_recency += 1
        if decision.passed:
            pass_combined += 1

    def rate(count: int) -> str:
        return f"{count}/{total} ({count / total:.1%})"

    print(f"deduped families with existing 36mo calendar curve: {total}")
    print(f"insufficient curves (pass with warning): {insufficient}")
    print(
        f"keep-rate segments>={DEFAULT_MIN_SEGMENTS_POSITIVE} alone: "
        f"{rate(pass_segments)}"
    )
    print(
        f"keep-rate recent_third_share<={DEFAULT_MAX_RECENT_THIRD_SHARE} alone: "
        f"{rate(pass_recency)}"
    )
    print(f"keep-rate combined (default gate): {rate(pass_combined)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
