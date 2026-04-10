"""Build a mixed opening-step contrast dataset from positives, corrective failures, and anchors."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION
from .offline_validator import validate_candidate_response

OPENING_ALLOWED_FIELDS = {
    "tool",
    "mode",
    "indicator_ids",
    "instruments",
    "candidate_name",
}

OPENING_REASONING = (
    "Fresh run opening step. Create one seed-guided candidate scaffold now so it can be validated next."
)

ANCHOR_ALLOWED_TOOLS = {
    "validate_profile",
    "register_profile",
    "inspect_artifact",
    "evaluate_candidate",
}


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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as sink:
        for row in rows:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")


def _parse_quota(items: list[str]) -> dict[str, int]:
    quotas: dict[str, int] = {}
    for item in items:
        tool, _, count_text = str(item).partition(":")
        if not tool or not count_text:
            raise ValueError(f"Invalid quota entry: {item!r}")
        quotas[tool.strip()] = int(count_text)
    return quotas


def _sort_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(row.get("run_id") or ""),
        int(row.get("step") or 0),
        str(row.get("example_id") or ""),
    )


def _first_action(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(target, dict):
        return None
    actions = target.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    return first if isinstance(first, dict) else None


def _normalize_opening_target(record: dict[str, Any]) -> dict[str, Any] | None:
    target = record.get("target_response_normalized")
    first = _first_action(target if isinstance(target, dict) else None)
    actions = target.get("actions") if isinstance(target, dict) else None
    if not isinstance(first, dict) or not isinstance(actions, list):
        return None
    if len(actions) != 1:
        return None
    if str(first.get("tool") or "") != "prepare_profile":
        return None
    if str(first.get("mode") or "") != "scaffold_from_seed":
        return None
    normalized_first = {
        key: value
        for key, value in first.items()
        if key in OPENING_ALLOWED_FIELDS
    }
    return {
        "reasoning": OPENING_REASONING,
        "actions": [normalized_first],
    }


def _classify_opening_failure(prediction: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    generated_text = str(prediction.get("generated_text") or "")
    if not bool(prediction.get("parse_ok")):
        issues.append("opening_formatting_cleanliness")
    validation_errors = [str(item) for item in (prediction.get("validation_errors") or [])]
    if any("prepare_profile requires mode" in item for item in validation_errors):
        issues.append("missing_prepare_mode")
    lowered = generated_text.lower()
    if "profile_name" in lowered:
        issues.append("generic_profile_name")
    if "seed_indicators" in lowered:
        issues.append("generic_seed_indicators")
    if any(
        token in lowered
        for token in ('"tool": "validate_profile"', '"tool":"validate_profile"', '"tool": "register_profile"', '"tool":"register_profile"', '"tool": "evaluate_candidate"', '"tool":"evaluate_candidate"')
    ):
        issues.append("opening_action_chaining")
    if not issues:
        issues.append("other_opening_contract_failure")
    deduped: list[str] = []
    for item in issues:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _is_clean_opening_positive(record: dict[str, Any]) -> bool:
    target = _normalize_opening_target(record)
    if target is None:
        return False
    first = _first_action(target)
    if not isinstance(first, dict):
        return False
    forbidden_strings = json.dumps(target, ensure_ascii=True)
    if "profile_name" in forbidden_strings or "seed_indicators" in forbidden_strings:
        return False
    validation = validate_candidate_response(record, target)
    return validation.ok


def _build_corrective_rows(
    opening_rows: list[dict[str, Any]],
    predictions_rows: list[dict[str, Any]],
    *,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], Counter, Counter]:
    opening_by_example = {
        str(row.get("example_id") or ""): row for row in opening_rows if row.get("example_id")
    }
    selected: list[dict[str, Any]] = []
    issue_counts = Counter()
    drop_counts = Counter()
    for prediction in predictions_rows:
        example_id = str(prediction.get("example_id") or "")
        reference = opening_by_example.get(example_id)
        if reference is None:
            drop_counts["missing_reference"] += 1
            continue
        if bool(prediction.get("parse_ok")) and bool(prediction.get("validation_ok")):
            drop_counts["already_valid"] += 1
            continue
        normalized_target = _normalize_opening_target(reference)
        if normalized_target is None:
            drop_counts["reference_not_normalizable"] += 1
            continue
        record = dict(reference)
        record["sourcetype"] = "deterministic"
        record["target_response_normalized"] = normalized_target
        record["target_actions"] = normalized_target["actions"]
        record["targetreasoningshort"] = OPENING_REASONING[:240]
        issues = _classify_opening_failure(prediction)
        record["corrective_focus"] = issues
        record["corrective_metadata"] = {
            "predicted_first_tool": prediction.get("predicted_first_tool"),
            "target_first_tool": prediction.get("target_first_tool"),
            "parse_ok": prediction.get("parse_ok"),
            "validation_ok": prediction.get("validation_ok"),
            "parse_error": prediction.get("parse_error"),
            "validation_errors": prediction.get("validation_errors"),
            "generated_tokens": prediction.get("generated_tokens"),
            "generated_text": prediction.get("generated_text"),
        }
        validation = validate_candidate_response(record, normalized_target)
        if not validation.ok:
            drop_counts["normalized_target_failed_validator"] += 1
            continue
        selected.append(record)
        for issue in issues:
            issue_counts[issue] += 1
    selected.sort(key=_sort_key)
    if limit is not None:
        selected = selected[:limit]
    return selected, issue_counts, drop_counts


def _build_positive_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    selected: list[dict[str, Any]] = []
    counts = Counter()
    for row in rows:
        normalized_target = _normalize_opening_target(row)
        if normalized_target is None:
            counts["not_normalizable"] += 1
            continue
        record = dict(row)
        record["target_response_normalized"] = normalized_target
        record["target_actions"] = normalized_target["actions"]
        record["targetreasoningshort"] = OPENING_REASONING[:240]
        validation = validate_candidate_response(record, normalized_target)
        if not validation.ok:
            counts["validator_failed"] += 1
            continue
        if not _is_clean_opening_positive(record):
            counts["not_clean_opening"] += 1
            continue
        selected.append(record)
        counts["selected"] += 1
    selected.sort(key=_sort_key)
    return selected, counts


def _build_anchor_rows(
    split_rows: list[dict[str, Any]],
    *,
    quotas: dict[str, int],
    exclude_example_ids: set[str],
    preferred_val_per_tool: int = 2,
) -> tuple[list[dict[str, Any]], Counter]:
    train_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    val_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts = Counter()
    for row in split_rows:
        example_id = str(row.get("example_id") or "")
        if example_id in exclude_example_ids:
            counts["exclude_example_id"] += 1
            continue
        if not isinstance(row.get("prompt_state_compact_v2"), dict):
            counts["missing_compact_v2"] += 1
            continue
        grade = str((row.get("quality_labels") or {}).get("grade") or "")
        if grade != "A":
            counts["grade_mismatch"] += 1
            continue
        target = row.get("target_response_normalized")
        first = _first_action(target if isinstance(target, dict) else None)
        tool = str(first.get("tool") or "") if isinstance(first, dict) else ""
        if tool not in quotas or tool not in ANCHOR_ALLOWED_TOOLS:
            counts["tool_not_selected"] += 1
            continue
        if str((row.get("policy_labels") or {}).get("deterministic_followup_target") or "") != tool:
            counts["not_deterministic_match"] += 1
            continue
        validation = validate_candidate_response(row, target)
        if not validation.ok:
            counts["validator_failed"] += 1
            continue
        split_hint = str(row.get("split_hint") or "")
        if split_hint == "val":
            val_buckets[tool].append(dict(row))
        else:
            train_buckets[tool].append(dict(row))
    selected: list[dict[str, Any]] = []
    for tool, limit in quotas.items():
        train_candidates = train_buckets.get(tool, [])
        val_candidates = val_buckets.get(tool, [])
        train_candidates.sort(key=_sort_key)
        val_candidates.sort(key=_sort_key)
        val_limit = min(preferred_val_per_tool, limit, len(val_candidates))
        chosen_val = val_candidates[:val_limit]
        remaining = limit - len(chosen_val)
        chosen_train = train_candidates[:remaining]
        chosen = chosen_val + chosen_train
        selected.extend(chosen)
        counts[f"selected:{tool}"] = len(chosen)
        counts[f"selected_val:{tool}"] = len(chosen_val)
        counts[f"selected_train:{tool}"] = len(chosen_train)
    selected.sort(key=_sort_key)
    return selected, counts


def _split_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    train_rows = [row for row in rows if str(row.get("split_hint") or "") == "train"]
    val_rows = [row for row in rows if str(row.get("split_hint") or "") == "val"]
    train_rows.sort(key=_sort_key)
    val_rows.sort(key=_sort_key)
    return train_rows, val_rows


def _validate_dataset(rows: list[dict[str, Any]], *, opening_only: bool = False) -> Counter:
    counts = Counter()
    for row in rows:
        counts["rows"] += 1
        target = row.get("target_response_normalized")
        try:
            json.dumps(target, ensure_ascii=True)
            counts["json_serializable"] += 1
        except Exception:
            counts["json_invalid"] += 1
        text = json.dumps(target, ensure_ascii=True)
        if "profile_name" in text:
            counts["forbidden_profile_name"] += 1
        if "seed_indicators" in text:
            counts["forbidden_seed_indicators"] += 1
        if "```" in text:
            counts["forbidden_fence"] += 1
        first = _first_action(target if isinstance(target, dict) else None)
        actions = target.get("actions") if isinstance(target, dict) else None
        if opening_only:
            if not isinstance(actions, list) or len(actions) != 1:
                counts["opening_action_count_invalid"] += 1
            if not isinstance(first, dict) or str(first.get("tool") or "") != "prepare_profile":
                counts["opening_tool_invalid"] += 1
        validation = validate_candidate_response(row, target) if isinstance(target, dict) else None
        if validation is not None and validation.ok:
            counts["validator_ok"] += 1
        else:
            counts["validator_failed"] += 1
    return counts


def build_opening_contrast_dataset(
    *,
    opening_rows_path: Path,
    opening_predictions_path: Path,
    anchor_rows_path: Path,
    train_out_path: Path,
    val_out_path: Path,
    summary_out_path: Path | None = None,
    anchor_quotas: dict[str, int],
    corrective_limit: int | None = None,
    preferred_val_anchor_per_tool: int = 2,
) -> dict[str, Any]:
    opening_rows_all = _load_jsonl(opening_rows_path)
    predictions_rows = _load_jsonl(opening_predictions_path)
    anchor_rows_all = _load_jsonl(anchor_rows_path)

    positive_rows, positive_counts = _build_positive_rows(opening_rows_all)
    corrective_rows, issue_counts, corrective_drop_counts = _build_corrective_rows(
        opening_rows_all,
        predictions_rows,
        limit=corrective_limit,
    )
    excluded_for_anchors = {
        str(row.get("example_id") or "")
        for row in positive_rows + corrective_rows
        if row.get("example_id")
    }
    anchor_rows, anchor_counts = _build_anchor_rows(
        anchor_rows_all,
        quotas=anchor_quotas,
        exclude_example_ids=excluded_for_anchors,
        preferred_val_per_tool=preferred_val_anchor_per_tool,
    )

    train_rows: list[dict[str, Any]] = []
    val_rows: list[dict[str, Any]] = []
    train_positive, val_positive = _split_rows(positive_rows)
    train_corrective, val_corrective = _split_rows(corrective_rows)
    train_anchor, val_anchor = _split_rows(anchor_rows)
    train_rows.extend(train_positive)
    train_rows.extend(train_corrective)
    train_rows.extend(train_anchor)
    val_rows.extend(val_positive)
    val_rows.extend(val_corrective)
    val_rows.extend(val_anchor)
    train_rows.sort(key=_sort_key)
    val_rows.sort(key=_sort_key)

    _write_jsonl(train_out_path, train_rows)
    _write_jsonl(val_out_path, val_rows)

    train_opening_only = [row for row in train_rows if _first_action(row.get("target_response_normalized")).get("tool") == "prepare_profile"] if train_rows else []
    val_opening_only = [row for row in val_rows if _first_action(row.get("target_response_normalized")).get("tool") == "prepare_profile"] if val_rows else []

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "opening_rows_path": str(opening_rows_path.resolve()),
        "opening_predictions_path": str(opening_predictions_path.resolve()),
        "anchor_rows_path": str(anchor_rows_path.resolve()),
        "train_out_path": str(train_out_path.resolve()),
        "val_out_path": str(val_out_path.resolve()),
        "anchor_quotas": anchor_quotas,
        "preferred_val_anchor_per_tool": preferred_val_anchor_per_tool,
        "counts": {
            "positive_rows": len(positive_rows),
            "corrective_rows": len(corrective_rows),
            "anchor_rows": len(anchor_rows),
            "train_rows": len(train_rows),
            "val_rows": len(val_rows),
            "train_positive": len(train_positive),
            "val_positive": len(val_positive),
            "train_corrective": len(train_corrective),
            "val_corrective": len(val_corrective),
            "train_anchor": len(train_anchor),
            "val_anchor": len(val_anchor),
        },
        "positive_selection_counts": dict(positive_counts),
        "corrective_issue_counts": dict(issue_counts),
        "corrective_drop_counts": dict(corrective_drop_counts),
        "anchor_selection_counts": dict(anchor_counts),
        "dataset_checks": {
            "train_all": dict(_validate_dataset(train_rows)),
            "val_all": dict(_validate_dataset(val_rows)),
            "train_opening": dict(_validate_dataset(train_opening_only, opening_only=True)),
            "val_opening": dict(_validate_dataset(val_opening_only, opening_only=True)),
        },
    }
    if summary_out_path is not None:
        summary_out_path.parent.mkdir(parents=True, exist_ok=True)
        summary_out_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an opening contrast dataset.")
    parser.add_argument("--opening-rows", type=Path, required=True, help="Combined non-holdout opening rows JSONL.")
    parser.add_argument("--opening-predictions", type=Path, required=True, help="Adapter predictions on non-holdout opening rows.")
    parser.add_argument("--anchor-rows", type=Path, required=True, help="Source split rows for later-step anchors.")
    parser.add_argument("--train-out", type=Path, required=True)
    parser.add_argument("--val-out", type=Path, required=True)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--anchor-quota", action="append", required=True, help="Per-tool anchor quota in TOOL:COUNT form.")
    parser.add_argument("--corrective-limit", type=int)
    parser.add_argument("--preferred-val-anchor-per-tool", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_opening_contrast_dataset(
        opening_rows_path=args.opening_rows,
        opening_predictions_path=args.opening_predictions,
        anchor_rows_path=args.anchor_rows,
        train_out_path=args.train_out,
        val_out_path=args.val_out,
        summary_out_path=args.summary_out,
        anchor_quotas=_parse_quota(args.anchor_quota),
        corrective_limit=args.corrective_limit,
        preferred_val_anchor_per_tool=args.preferred_val_anchor_per_tool,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
