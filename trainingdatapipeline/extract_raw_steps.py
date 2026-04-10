"""Extract replay-oriented raw step records from historical runs."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from . import PIPELINE_VERSION
from .discover_runs import discover_runs
from .replay_controller_state import replay_run_steps


def _default_runs_root() -> Path:
    return Path(__file__).resolve().parents[1] / "runs"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract replay-oriented raw step records from autoresearch runs."
    )
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        help="Run corpus root or a specific run directory. Repeatable.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output JSONL path for extracted raw steps.",
    )
    parser.add_argument(
        "--summary-out",
        type=Path,
        help="Optional JSON summary report path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    roots = [Path(item) for item in args.roots] if args.roots else [_default_runs_root()]
    runs = discover_runs(roots)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    phase_counts = Counter()
    marker_counts = Counter()
    extracted_steps = 0
    processed_runs = 0

    with args.out.open("w", encoding="utf-8") as handle:
        for run in runs:
            if not run.artifact_inventory.required_present.get("controller-log.jsonl"):
                continue
            records = replay_run_steps(run)
            if not records:
                continue
            processed_runs += 1
            for record in records:
                extracted_steps += 1
                phase_counts[str(record.get("phase") or "")] += 1
                markers = (
                    record.get("trace_markers")
                    if isinstance(record.get("trace_markers"), dict)
                    else {}
                )
                for key, value in markers.items():
                    if value is True:
                        marker_counts[key] += 1
                handle.write(json.dumps(record, ensure_ascii=True) + "\n")

    if args.summary_out is not None:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)
        summary = {
            "pipeline_version": PIPELINE_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "roots": [str(path.resolve()) for path in roots],
            "processed_runs": processed_runs,
            "extracted_steps": extracted_steps,
            "phase_counts": dict(phase_counts),
            "marker_counts": dict(marker_counts),
            "output_path": str(args.out.resolve()),
        }
        args.summary_out.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
