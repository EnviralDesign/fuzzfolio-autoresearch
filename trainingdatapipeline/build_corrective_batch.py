"""Build a focused corrective relabel batch from benchmark prediction failures."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
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


def _classify_issue(prediction: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if not bool(prediction.get("parse_ok")):
        issues.append("formatting_cleanliness")
    validation_errors = [str(item) for item in (prediction.get("validation_errors") or [])]
    if any("evaluate_candidate requires instruments array" in item for item in validation_errors):
        issues.append("missing_evaluate_instruments")
    predicted_tool = str(prediction.get("predicted_first_tool") or "")
    target_tool = str(prediction.get("target_first_tool") or "")
    if predicted_tool and target_tool and predicted_tool != target_tool:
        issues.append("wrong_tool_choice")
    if predicted_tool == "inspect_artifact" and bool(prediction.get("validation_ok")) is False:
        issues.append("inspect_artifact_binding")
    if not issues:
        issues.append("other_contract_failure")
    return issues


def build_corrective_batch(
    predictions_path: Path,
    reference_path: Path,
    output_path: Path,
    *,
    summary_path: Path | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    predictions = _load_jsonl(predictions_path)
    reference_rows = _load_jsonl(reference_path)
    reference_by_example = {
        str(row.get("example_id") or ""): row for row in reference_rows if row.get("example_id")
    }

    selected: list[dict[str, Any]] = []
    issue_counts = Counter()

    for prediction in predictions:
        if bool(prediction.get("parse_ok")) and bool(prediction.get("validation_ok")):
            continue
        example_id = str(prediction.get("example_id") or "")
        reference = reference_by_example.get(example_id)
        if not reference:
            continue
        issues = _classify_issue(prediction)
        for issue in issues:
            issue_counts[issue] += 1
        record = dict(reference)
        record["corrective_focus"] = issues
        record["corrective_metadata"] = {
            "benchmark_prediction_path": str(predictions_path.resolve()),
            "benchmark_reference_path": str(reference_path.resolve()),
            "predicted_first_tool": prediction.get("predicted_first_tool"),
            "target_first_tool": prediction.get("target_first_tool"),
            "parse_ok": prediction.get("parse_ok"),
            "validation_ok": prediction.get("validation_ok"),
            "parse_error": prediction.get("parse_error"),
            "validation_errors": prediction.get("validation_errors"),
            "generated_tokens": prediction.get("generated_tokens"),
            "generated_text": prediction.get("generated_text"),
        }
        selected.append(record)

    selected.sort(
        key=lambda row: (
            str(row.get("run_id") or ""),
            int(row.get("step") or 0),
            str(row.get("example_id") or ""),
        )
    )
    if limit is not None:
        selected = selected[:limit]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as sink:
        for row in selected:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "predictions_path": str(predictions_path.resolve()),
        "reference_path": str(reference_path.resolve()),
        "output_path": str(output_path.resolve()),
        "rows_emitted": len(selected),
        "issue_counts": dict(issue_counts),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a corrective batch from benchmark failures.")
    parser.add_argument("--predictions", type=Path, required=True, help="Benchmark predictions JSONL.")
    parser.add_argument("--reference", type=Path, required=True, help="Benchmark reference JSONL.")
    parser.add_argument("--out", type=Path, required=True, help="Output corrective batch JSONL.")
    parser.add_argument("--summary-out", type=Path, help="Optional summary JSON path.")
    parser.add_argument("--limit", type=int, help="Optional max corrective rows.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_corrective_batch(
        args.predictions,
        args.reference,
        args.out,
        summary_path=args.summary_out,
        limit=args.limit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
