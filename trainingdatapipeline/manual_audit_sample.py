"""Build a stratified manual audit sample from scored examples."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


BASE_QUOTAS: dict[tuple[str, str], int] = {
    ("prepare_profile", "early"): 5,
    ("prepare_profile", "mid"): 2,
    ("prepare_profile", "late"): 1,
    ("validate_profile", "early"): 3,
    ("validate_profile", "mid"): 3,
    ("validate_profile", "late"): 2,
    ("validate_profile", "wrap_up"): 1,
    ("register_profile", "early"): 3,
    ("register_profile", "mid"): 3,
    ("register_profile", "late"): 2,
    ("register_profile", "wrap_up"): 1,
    ("evaluate_candidate", "early"): 3,
    ("evaluate_candidate", "mid"): 3,
    ("evaluate_candidate", "late"): 1,
    ("evaluate_candidate", "wrap_up"): 1,
    ("inspect_artifact", "early"): 2,
    ("inspect_artifact", "mid"): 2,
    ("inspect_artifact", "late"): 1,
    ("inspect_artifact", "wrap_up"): 1,
}

RECOVERY_QUOTAS = {
    "repeated_stall": 8,
    "overuseofread_file": 7,
    "wrongtoolfor_state": 7,
    "invalidjsonshape": 5,
    "missingrequiredfield": 4,
    "timeframerepeatblock": 2,
    "profilereforpathresolution_error": 1,
    "finish_denied": 1,
}

REJECT_QUOTAS = {
    "run_cli_policy_step": 10,
    "browse_only_step": 7,
    "empty_reasoning": 6,
    "controller_blocked": 2,
}

NEAR_MISS_REASONS = set(REJECT_QUOTAS)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _first_tool(row: dict[str, Any]) -> str:
    actions = row.get("target_actions")
    if not isinstance(actions, list) or not actions:
        return "none"
    first = actions[0]
    if not isinstance(first, dict):
        return "unknown"
    return str(first.get("tool") or "unknown")


def _sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("run_id") or ""),
            int(row.get("step") or 0),
            str(row.get("example_id") or ""),
        ),
    )


def _take_unique_run(
    candidates: list[dict[str, Any]],
    quota: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    chosen: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    seen_runs: set[str] = set()
    for row in candidates:
        run_id = str(row.get("run_id") or "")
        if len(chosen) < quota and run_id not in seen_runs:
            chosen.append(row)
            seen_runs.add(run_id)
        else:
            remaining.append(row)
    if len(chosen) < quota:
        for row in remaining:
            if len(chosen) >= quota:
                break
            chosen.append(row)
    return chosen[:quota], remaining


def _base_pool(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        quality = row.get("quality_labels") if isinstance(row.get("quality_labels"), dict) else {}
        if not quality.get("keep_for_base_sft"):
            continue
        buckets[(_first_tool(row), str(row.get("phase") or ""))].append(row)
    return {key: _sort_rows(value) for key, value in buckets.items()}


def _recovery_pool(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        quality = row.get("quality_labels") if isinstance(row.get("quality_labels"), dict) else {}
        if not quality.get("keep_for_recovery"):
            continue
        recovery = row.get("recovery_labels") if isinstance(row.get("recovery_labels"), dict) else {}
        for label in recovery.get("failure_classes") or []:
            buckets[str(label)].append(row)
    return {key: _sort_rows(value) for key, value in buckets.items()}


def _reject_pool(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        quality = row.get("quality_labels") if isinstance(row.get("quality_labels"), dict) else {}
        if quality.get("keep_for_base_sft") or quality.get("keep_for_recovery"):
            continue
        reasons = set(str(item) for item in (row.get("rejection_reasons") or []))
        if not reasons or not reasons.issubset(NEAR_MISS_REASONS):
            continue
        for reason in sorted(reasons):
            if reason in NEAR_MISS_REASONS:
                buckets[reason].append(row)
    return {key: _sort_rows(value) for key, value in buckets.items()}


def build_manual_audit_sample(
    input_path: Path,
    output_path: Path,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    rows = _load_rows(input_path)
    sample: list[dict[str, Any]] = []
    sample_counts: dict[str, dict[str, int]] = {
        "base": {},
        "recovery": {},
        "reject": {},
    }

    base_buckets = _base_pool(rows)
    for bucket, quota in BASE_QUOTAS.items():
        chosen, _remaining = _take_unique_run(base_buckets.get(bucket, []), quota)
        sample_counts["base"][f"{bucket[0]}::{bucket[1]}"] = len(chosen)
        for row in chosen:
            emitted = dict(row)
            emitted["audit_bucket"] = "base"
            emitted["audit_stratum"] = f"{bucket[0]}::{bucket[1]}"
            sample.append(emitted)

    recovery_buckets = _recovery_pool(rows)
    for label, quota in RECOVERY_QUOTAS.items():
        chosen, _remaining = _take_unique_run(recovery_buckets.get(label, []), quota)
        sample_counts["recovery"][label] = len(chosen)
        for row in chosen:
            emitted = dict(row)
            emitted["audit_bucket"] = "recovery"
            emitted["audit_stratum"] = label
            sample.append(emitted)

    reject_buckets = _reject_pool(rows)
    for label, quota in REJECT_QUOTAS.items():
        chosen, _remaining = _take_unique_run(reject_buckets.get(label, []), quota)
        sample_counts["reject"][label] = len(chosen)
        for row in chosen:
            emitted = dict(row)
            emitted["audit_bucket"] = "reject"
            emitted["audit_stratum"] = label
            sample.append(emitted)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in sample:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "sample_count": len(sample),
        "sample_counts": sample_counts,
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a manual audit sample.")
    parser.add_argument("--input", type=Path, required=True, help="Input scored JSONL.")
    parser.add_argument("--out", type=Path, required=True, help="Output audit sample JSONL.")
    parser.add_argument("--summary-out", type=Path, help="Optional summary JSON path.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_manual_audit_sample(args.input, args.out, args.summary_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
