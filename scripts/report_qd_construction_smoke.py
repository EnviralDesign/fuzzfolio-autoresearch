"""Write a compact diversity, CPU, and RAM report for a QD smoke."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from autoresearch.temporal_qd_smoke_report import (
    build_qd_construction_smoke_report,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generation-root", required=True, type=Path)
    parser.add_argument("--top-phase-limit", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build_qd_construction_smoke_report(
        args.generation_root,
        top_phase_limit=args.top_phase_limit,
    )
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    output = args.output or (
        args.generation_root / "performance" / "construction-smoke-report.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(encoded, encoding="utf-8", newline="\n")
    print(encoded, end="")


if __name__ == "__main__":
    main()
