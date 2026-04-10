"""Build run-level train/val/test splits with holdout runs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _first_tool(record: dict[str, Any]) -> str:
    actions = record.get("target_actions")
    if not isinstance(actions, list) or not actions:
        return "none"
    first = actions[0]
    if not isinstance(first, dict):
        return "unknown"
    return str(first.get("tool") or "unknown")


def _run_stats(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "total_records": 0,
            "base_keep_records": 0,
            "recovery_keep_records": 0,
            "phases": Counter(),
            "first_tools": Counter(),
            "first_timestamp": None,
            "last_timestamp": None,
        }
    )
    for record in records:
        run_id = str(record.get("run_id") or "")
        if not run_id:
            continue
        row = stats[run_id]
        row["total_records"] += 1
        phase = str(record.get("phase") or "unknown")
        row["phases"][phase] += 1
        row["first_tools"][_first_tool(record)] += 1
        quality = (
            record.get("quality_labels")
            if isinstance(record.get("quality_labels"), dict)
            else {}
        )
        if quality.get("keep_for_base_sft"):
            row["base_keep_records"] += 1
        if quality.get("keep_for_recovery"):
            row["recovery_keep_records"] += 1
        timestamp = record.get("timestamp")
        if isinstance(timestamp, str) and timestamp:
            if row["first_timestamp"] is None or timestamp < row["first_timestamp"]:
                row["first_timestamp"] = timestamp
            if row["last_timestamp"] is None or timestamp > row["last_timestamp"]:
                row["last_timestamp"] = timestamp
    for run_id, row in stats.items():
        total = int(row["total_records"])
        base_keep = int(row["base_keep_records"])
        recovery = int(row["recovery_keep_records"])
        phase_counter = Counter(row["phases"])
        first_tool_counter = Counter(row["first_tools"])
        row["base_rate"] = (base_keep / total) if total else 0.0
        row["recovery_ratio"] = (recovery / total) if total else 0.0
        row["dominant_phase"] = phase_counter.most_common(1)[0][0] if phase_counter else None
        row["dominant_first_tool"] = (
            first_tool_counter.most_common(1)[0][0] if first_tool_counter else None
        )
        row["has_any_base_keep"] = bool(base_keep)
        row["has_any_recovery_keep"] = bool(recovery)
        row["hash"] = _stable_hash(run_id)
        row["phases"] = dict(row["phases"])
        row["first_tools"] = dict(row["first_tools"])
    return dict(stats)


def _select_recovery_holdout_runs(
    stats: dict[str, dict[str, Any]],
    holdout_run_count: int,
    *,
    min_records: int,
    min_recovery_rate: float,
) -> set[str]:
    threshold_matches = [
        (run_id, row)
        for run_id, row in stats.items()
        if int(row.get("total_records") or 0) >= min_records
        and float(row.get("recovery_ratio") or 0.0) >= min_recovery_rate
        and int(row.get("recovery_keep_records") or 0) > 0
    ]
    threshold_matches.sort(
        key=lambda item: (
            -float(item[1].get("recovery_ratio") or 0.0),
            -int(item[1].get("recovery_keep_records") or 0),
            item[0],
        )
    )
    if threshold_matches:
        return {run_id for run_id, _row in threshold_matches}
    fallback = [
        (run_id, row)
        for run_id, row in stats.items()
        if int(row.get("recovery_keep_records") or 0) > 0
    ]
    fallback.sort(
        key=lambda item: (
            -int(item[1].get("recovery_keep_records") or 0),
            -float(item[1].get("recovery_ratio") or 0.0),
            -int(item[1].get("total_records") or 0),
            item[1].get("hash") or "",
        )
    )
    return {run_id for run_id, _row in fallback[:holdout_run_count]}


def _select_general_holdout_runs(
    stats: dict[str, dict[str, Any]],
    excluded_runs: set[str],
    holdout_run_count: int,
) -> set[str]:
    candidates = [
        run_id for run_id in stats.keys() if run_id not in excluded_runs
    ]
    candidates.sort(key=_stable_hash)
    return set(candidates[:holdout_run_count])


def _assign_main_splits(
    stats: dict[str, dict[str, Any]],
    excluded_runs: set[str],
    train_ratio: float,
    val_ratio: float,
) -> dict[str, str]:
    candidates = [run_id for run_id in stats.keys() if run_id not in excluded_runs]
    candidates.sort(key=_stable_hash)
    total = len(candidates)
    train_cut = int(total * train_ratio)
    val_cut = train_cut + int(total * val_ratio)
    assignments: dict[str, str] = {}
    for index, run_id in enumerate(candidates):
        if index < train_cut:
            assignments[run_id] = "train"
        elif index < val_cut:
            assignments[run_id] = "val"
        else:
            assignments[run_id] = "test"
    return assignments


def build_splits(
    input_path: Path,
    out_dir: Path,
    *,
    recovery_holdout_runs: int = 12,
    general_holdout_runs: int = 12,
    recovery_holdout_min_records: int = 100,
    recovery_holdout_min_rate: float = 0.40,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
) -> dict[str, Any]:
    records = _load_records(input_path)
    stats = _run_stats(records)
    recovery_runs = _select_recovery_holdout_runs(
        stats,
        recovery_holdout_runs,
        min_records=recovery_holdout_min_records,
        min_recovery_rate=recovery_holdout_min_rate,
    )
    general_runs = _select_general_holdout_runs(stats, recovery_runs, general_holdout_runs)
    main_assignments = _assign_main_splits(
        stats,
        recovery_runs | general_runs,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    split_files = {
        "train": (out_dir / "train.jsonl").open("w", encoding="utf-8"),
        "val": (out_dir / "val.jsonl").open("w", encoding="utf-8"),
        "test": (out_dir / "test.jsonl").open("w", encoding="utf-8"),
        "holdout_general": (out_dir / "holdout_general.jsonl").open("w", encoding="utf-8"),
        "holdout_recovery": (out_dir / "holdout_recovery.jsonl").open("w", encoding="utf-8"),
    }
    split_counts = Counter()
    split_run_counts = Counter()
    seen_run_split: set[tuple[str, str]] = set()
    try:
        for record in records:
            run_id = str(record.get("run_id") or "")
            quality = (
                record.get("quality_labels")
                if isinstance(record.get("quality_labels"), dict)
                else {}
            )
            split_name: str | None = None
            if run_id in recovery_runs:
                if quality.get("keep_for_recovery"):
                    split_name = "holdout_recovery"
            elif run_id in general_runs:
                if quality.get("keep_for_base_sft"):
                    split_name = "holdout_general"
            else:
                assigned = main_assignments.get(run_id)
                if assigned and quality.get("keep_for_base_sft"):
                    split_name = assigned
            if not split_name:
                continue
            emitted = dict(record)
            emitted["split_hint"] = split_name
            split_files[split_name].write(json.dumps(emitted, ensure_ascii=True) + "\n")
            split_counts[split_name] += 1
            key = (split_name, run_id)
            if key not in seen_run_split:
                seen_run_split.add(key)
                split_run_counts[split_name] += 1
    finally:
        for handle in split_files.values():
            handle.close()

    manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "out_dir": str(out_dir.resolve()),
        "config": {
            "recovery_holdout_runs": recovery_holdout_runs,
            "general_holdout_runs": general_holdout_runs,
            "recovery_holdout_min_records": recovery_holdout_min_records,
            "recovery_holdout_min_rate": recovery_holdout_min_rate,
            "train_ratio": train_ratio,
            "val_ratio": val_ratio,
            "test_ratio": max(0.0, 1.0 - train_ratio - val_ratio),
        },
        "split_record_counts": dict(split_counts),
        "split_run_counts": dict(split_run_counts),
        "recovery_holdout_runs": sorted(recovery_runs),
        "general_holdout_runs": sorted(general_runs),
        "main_split_assignments": main_assignments,
        "run_stats": stats,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build run-level train/val/test splits.")
    parser.add_argument("--input", type=Path, required=True, help="Input scored JSONL path.")
    parser.add_argument("--out", type=Path, required=True, help="Output dataset directory.")
    parser.add_argument(
        "--recovery-holdout-runs",
        type=int,
        default=12,
        help="Number of recovery-heavy runs to reserve.",
    )
    parser.add_argument(
        "--general-holdout-runs",
        type=int,
        default=12,
        help="Number of general holdout runs to reserve.",
    )
    parser.add_argument(
        "--recovery-holdout-min-records",
        type=int,
        default=100,
        help="Minimum run size to qualify for recovery holdout thresholding.",
    )
    parser.add_argument(
        "--recovery-holdout-min-rate",
        type=float,
        default=0.40,
        help="Minimum recovery keep ratio to qualify for recovery holdout thresholding.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_splits(
        args.input,
        args.out,
        recovery_holdout_runs=args.recovery_holdout_runs,
        general_holdout_runs=args.general_holdout_runs,
        recovery_holdout_min_records=args.recovery_holdout_min_records,
        recovery_holdout_min_rate=args.recovery_holdout_min_rate,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
