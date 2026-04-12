"""Build a curated exploration-judgment dataset from manual review labels."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION
from .export_chat_format import export_chat_format
from .normalize_state import normalize_model_target
from .offline_validator import validate_candidate_response

DEFAULT_FOLLOWUP_TRAIN_QUOTAS = {
    "validate_profile": 20,
    "register_profile": 20,
    "mutate_profile": 20,
    "evaluate_candidate": 20,
}

DEFAULT_FOLLOWUP_VAL_QUOTAS = {
    "validate_profile": 4,
    "register_profile": 4,
    "mutate_profile": 4,
    "evaluate_candidate": 4,
}

KEEP_DECISIONS = {"keep_gold", "rewrite_action"}
DROP_DECISIONS = {"drop_infra", "drop_mechanical", "drop_ambiguous"}
LEGACY_MODEL_FIELDS = frozenset(
    {"profile_path", "destination_path", "source_profile_path", "metadata_out_path"}
)


def _default_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_final_v4(split_name: str) -> Path:
    return _default_root() / "data" / "training_pipeline" / "final" / "v4" / f"{split_name}.jsonl"


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


def _parse_quota(items: list[str] | None, default: dict[str, int]) -> dict[str, int]:
    if not items:
        return dict(default)
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
        str(row.get("example_id") or row.get("review_id") or ""),
    )


def _first_action(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(target, dict):
        return None
    actions = target.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    return first if isinstance(first, dict) else None


def _first_tool(row: dict[str, Any]) -> str:
    first = _first_action(row.get("target_response_normalized"))
    return str(first.get("tool") or "").strip() if isinstance(first, dict) else ""


def _contains_legacy_fields(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in LEGACY_MODEL_FIELDS:
                return True
            if _contains_legacy_fields(item):
                return True
        return False
    if isinstance(value, list):
        return any(_contains_legacy_fields(item) for item in value)
    if isinstance(value, str):
        lowered = value.lower()
        return any(field in lowered for field in LEGACY_MODEL_FIELDS)
    return False


def _drop_review_only_fields(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(row)
    for key in (
        "review_sheet_line",
        "review_state_summary",
        "review_action_summary",
        "review_result_summary",
    ):
        cleaned.pop(key, None)
    return cleaned


def _build_rewritten_target(
    row: dict[str, Any],
    label: dict[str, Any],
) -> dict[str, Any]:
    reasoning = str(label.get("corrected_reasoning") or "").strip()
    if not reasoning:
        raise ValueError("rewrite_action requires corrected_reasoning.")
    corrected_action = label.get("corrected_action")
    if not isinstance(corrected_action, dict):
        raise ValueError("rewrite_action requires corrected_action as an object.")
    if _contains_legacy_fields(corrected_action):
        raise ValueError("rewrite_action corrected_action must not contain legacy path fields.")
    prompt_state = row.get("prompt_state") if isinstance(row.get("prompt_state"), dict) else {}
    run_info = prompt_state.get("run") if isinstance(prompt_state.get("run"), dict) else {}
    run_dir = str(run_info.get("run_dir") or "")
    normalized_action = normalize_model_target(corrected_action, run_dir)
    target = {
        "reasoning": reasoning,
        "actions": [normalized_action],
    }
    if _contains_legacy_fields(target):
        raise ValueError("rewrite_action target still contains legacy path fields after normalization.")
    return target


def _validated_target(row: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    validation = validate_candidate_response(row, target)
    if not validation.ok or not isinstance(validation.normalized_response, dict):
        raise ValueError("; ".join(validation.errors) or "Validation failed.")
    if _contains_legacy_fields(validation.normalized_response):
        raise ValueError("Validated target still contains legacy path fields.")
    return validation.normalized_response


def _apply_label(row: dict[str, Any], label: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    decision = str(label.get("decision") or "").strip()
    if decision in DROP_DECISIONS:
        return None, decision
    if decision not in KEEP_DECISIONS:
        raise ValueError(f"Unknown review decision {decision!r} for {row.get('review_id')}.")
    if decision == "keep_gold":
        target = row.get("target_response_normalized")
        if not isinstance(target, dict):
            raise ValueError("keep_gold row is missing target_response_normalized.")
    else:
        target = _build_rewritten_target(row, label)
    normalized_target = _validated_target(row, target)
    output = _drop_review_only_fields(row)
    output["target_response_normalized"] = normalized_target
    output["target_actions_normalized"] = list(normalized_target.get("actions") or [])
    output["target_actions"] = list(normalized_target.get("actions") or [])
    output["manual_label"] = {
        "review_id": row.get("review_id"),
        "decision": decision,
        "notes": str(label.get("notes") or "").strip(),
    }
    output["source_bucket"] = f"manual_{decision}"
    return output, decision


def _stable_buckets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = str(row.get("decision_bucket") or "other")
        buckets[key].append(row)
    for bucket_rows in buckets.values():
        bucket_rows.sort(
            key=lambda row: (
                -int(row.get("interest_score") or 0),
                str(row.get("review_id") or ""),
            )
        )
    return buckets


def _round_robin_take(grouped_rows: dict[str, list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    ordered_keys = sorted(grouped_rows)
    while len(selected) < limit:
        progressed = False
        for key in ordered_keys:
            bucket = grouped_rows.get(key) or []
            if not bucket:
                continue
            selected.append(bucket.pop(0))
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


def _split_manual_rows(
    rows: list[dict[str, Any]],
    *,
    benchmark_target: int,
    val_target: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped = _stable_buckets(rows)
    benchmark_rows = _round_robin_take(grouped, benchmark_target)
    val_rows = _round_robin_take(grouped, val_target)
    train_rows: list[dict[str, Any]] = []
    for bucket_rows in grouped.values():
        train_rows.extend(bucket_rows)
    train_rows.sort(key=_sort_key)
    benchmark_rows.sort(key=_sort_key)
    val_rows.sort(key=_sort_key)
    return train_rows, val_rows, benchmark_rows


def _duplicate_train_rows(rows: list[dict[str, Any]], target_count: int) -> tuple[list[dict[str, Any]], Counter]:
    if len(rows) >= target_count or not rows:
        return list(rows), Counter()
    duplicated = list(rows)
    counts = Counter()
    index = 0
    while len(duplicated) < target_count:
        source = rows[index % len(rows)]
        copy = dict(source)
        dup_index = counts[str(source.get("review_id") or source.get("example_id") or "")] + 1
        counts[str(source.get("review_id") or source.get("example_id") or "")] += 1
        copy["example_id"] = f"{source.get('example_id') or source.get('review_id')}::dup{dup_index:02d}"
        copy["manual_label"] = {
            **(source.get("manual_label") if isinstance(source.get("manual_label"), dict) else {}),
            "duplicate_index": dup_index,
        }
        duplicated.append(copy)
        index += 1
    duplicated.sort(key=_sort_key)
    return duplicated, counts


def _opening_anchor_row(row: dict[str, Any]) -> bool:
    target = row.get("target_response_normalized")
    first = _first_action(target if isinstance(target, dict) else None)
    if not isinstance(first, dict):
        return False
    if str(first.get("tool") or "") != "prepare_profile":
        return False
    if str(first.get("mode") or "") != "scaffold_from_seed":
        return False
    if _contains_legacy_fields(target):
        return False
    if not bool((row.get("quality_labels") or {}).get("keep_for_base_sft")):
        return False
    return True


def _select_opening_anchors(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    exclude_example_ids: set[str],
) -> tuple[list[dict[str, Any]], Counter]:
    selected: list[dict[str, Any]] = []
    counts = Counter()
    candidates = sorted(rows, key=_sort_key)
    for row in candidates:
        if len(selected) >= limit:
            break
        example_id = str(row.get("example_id") or "")
        if example_id in exclude_example_ids:
            counts["exclude_example_id"] += 1
            continue
        if not _opening_anchor_row(row):
            counts["opening_filter_fail"] += 1
            continue
        validation = validate_candidate_response(row, row.get("target_response_normalized"))
        if not validation.ok:
            counts["validator_failed"] += 1
            continue
        chosen = _drop_review_only_fields(dict(row))
        chosen["source_bucket"] = "opening_anchor"
        selected.append(chosen)
        counts["selected"] += 1
    return selected, counts


def _followup_anchor_row(row: dict[str, Any], allowed_tools: set[str]) -> bool:
    target = row.get("target_response_normalized")
    first = _first_action(target if isinstance(target, dict) else None)
    if not isinstance(first, dict):
        return False
    tool = str(first.get("tool") or "")
    if tool not in allowed_tools:
        return False
    if _contains_legacy_fields(target):
        return False
    quality = row.get("quality_labels") if isinstance(row.get("quality_labels"), dict) else {}
    policy = row.get("policy_labels") if isinstance(row.get("policy_labels"), dict) else {}
    if not bool(quality.get("keep_for_base_sft")):
        return False
    if not bool(policy.get("controller_admissible", True)):
        return False
    return True


def _select_followup_anchors(
    rows: list[dict[str, Any]],
    *,
    quotas: dict[str, int],
    exclude_example_ids: set[str],
) -> tuple[list[dict[str, Any]], Counter]:
    counts = Counter()
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        example_id = str(row.get("example_id") or "")
        if example_id in exclude_example_ids:
            counts["exclude_example_id"] += 1
            continue
        if not _followup_anchor_row(row, set(quotas)):
            counts["followup_filter_fail"] += 1
            continue
        validation = validate_candidate_response(row, row.get("target_response_normalized"))
        if not validation.ok:
            counts["validator_failed"] += 1
            continue
        tool = _first_tool(row)
        buckets[tool].append(_drop_review_only_fields(dict(row)))
    selected: list[dict[str, Any]] = []
    for tool, limit in quotas.items():
        candidates = sorted(buckets.get(tool, []), key=_sort_key)
        chosen = candidates[:limit]
        for row in chosen:
            row["source_bucket"] = "followup_anchor"
            selected.append(row)
        counts[f"selected:{tool}"] = len(chosen)
    selected.sort(key=_sort_key)
    return selected, counts


def _review_ids(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("review_id") or "") for row in rows if row.get("review_id")}


def _example_ids(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("example_id") or "") for row in rows if row.get("example_id")}


def _load_labels(path: Path) -> dict[str, dict[str, Any]]:
    labels: dict[str, dict[str, Any]] = {}
    for row in _load_jsonl(path):
        review_id = str(row.get("review_id") or "").strip()
        if not review_id:
            continue
        labels[review_id] = row
    return labels


def _materialize_manual_rows(
    review_rows: list[dict[str, Any]],
    labels_by_id: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], Counter, Counter]:
    selected: list[dict[str, Any]] = []
    decision_counts = Counter()
    drop_counts = Counter()
    for row in review_rows:
        review_id = str(row.get("review_id") or "")
        label = labels_by_id.get(review_id)
        if label is None:
            drop_counts["missing_label"] += 1
            continue
        try:
            materialized, decision = _apply_label(row, label)
        except ValueError as exc:
            drop_counts[f"label_error:{review_id}"] += 1
            drop_counts["label_error_total"] += 1
            label_notes = dict(label)
            label_notes["builder_error"] = str(exc)
            labels_by_id[review_id] = label_notes
            continue
        decision_counts[decision] += 1
        if materialized is None:
            drop_counts[decision] += 1
            continue
        selected.append(materialized)
    selected.sort(key=_sort_key)
    return selected, decision_counts, drop_counts


def _build_dataset_rows(
    manual_train_rows: list[dict[str, Any]],
    manual_val_rows: list[dict[str, Any]],
    benchmark_rows: list[dict[str, Any]],
    train_source_rows: list[dict[str, Any]],
    val_source_rows: list[dict[str, Any]],
    *,
    followup_train_quotas: dict[str, int],
    followup_val_quotas: dict[str, int],
    opening_train_target: int,
    opening_val_target: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter, dict[str, Counter]]:
    train_manual_example_ids = _example_ids(manual_train_rows)
    val_manual_example_ids = _example_ids(manual_val_rows)
    benchmark_example_ids = _example_ids(benchmark_rows)
    excluded_train = train_manual_example_ids | benchmark_example_ids
    excluded_val = val_manual_example_ids | benchmark_example_ids

    opening_train, opening_train_counts = _select_opening_anchors(
        train_source_rows,
        limit=opening_train_target,
        exclude_example_ids=excluded_train,
    )
    opening_val, opening_val_counts = _select_opening_anchors(
        val_source_rows,
        limit=opening_val_target,
        exclude_example_ids=excluded_val,
    )
    followup_train, followup_train_counts = _select_followup_anchors(
        train_source_rows,
        quotas=followup_train_quotas,
        exclude_example_ids=excluded_train | _example_ids(opening_train),
    )
    followup_val, followup_val_counts = _select_followup_anchors(
        val_source_rows,
        quotas=followup_val_quotas,
        exclude_example_ids=excluded_val | _example_ids(opening_val),
    )

    train_rows = list(manual_train_rows) + followup_train + opening_train
    val_rows = list(manual_val_rows) + followup_val + opening_val

    for row in train_rows:
        row["split_hint"] = "train"
    for row in val_rows:
        row["split_hint"] = "val"
    for row in benchmark_rows:
        row["split_hint"] = "benchmark"

    train_rows.sort(key=_sort_key)
    val_rows.sort(key=_sort_key)
    benchmark_rows.sort(key=_sort_key)

    source_counts = Counter(str(row.get("source_bucket") or "unknown") for row in train_rows + val_rows)
    diagnostics = {
        "opening_train": opening_train_counts,
        "opening_val": opening_val_counts,
        "followup_train": followup_train_counts,
        "followup_val": followup_val_counts,
    }
    return train_rows, val_rows, source_counts, diagnostics


def build_exploration_judgment_dataset(
    *,
    review_path: Path,
    labels_path: Path,
    train_source_path: Path,
    val_source_path: Path,
    train_out_path: Path,
    val_out_path: Path,
    benchmark_out_path: Path,
    train_chat_out_path: Path,
    val_chat_out_path: Path,
    manifest_path: Path,
    benchmark_chat_out_path: Path | None = None,
    manual_train_target: int = 112,
    manual_benchmark_target: int = 24,
    manual_val_target: int = 12,
    opening_train_target: int = 32,
    opening_val_target: int = 8,
    followup_train_quotas: dict[str, int] | None = None,
    followup_val_quotas: dict[str, int] | None = None,
) -> dict[str, Any]:
    followup_train_quotas = followup_train_quotas or dict(DEFAULT_FOLLOWUP_TRAIN_QUOTAS)
    followup_val_quotas = followup_val_quotas or dict(DEFAULT_FOLLOWUP_VAL_QUOTAS)
    review_rows = _load_jsonl(review_path)
    labels_by_id = _load_labels(labels_path)
    train_source_rows = _load_jsonl(train_source_path)
    val_source_rows = _load_jsonl(val_source_path)

    manual_rows, decision_counts, label_drop_counts = _materialize_manual_rows(review_rows, labels_by_id)
    manual_train_rows, manual_val_rows, benchmark_rows = _split_manual_rows(
        manual_rows,
        benchmark_target=manual_benchmark_target,
        val_target=manual_val_target,
    )
    manual_train_rows, duplicate_counts = _duplicate_train_rows(
        manual_train_rows,
        target_count=manual_train_target,
    )

    train_rows, val_rows, source_counts, diagnostics = _build_dataset_rows(
        manual_train_rows,
        manual_val_rows,
        benchmark_rows,
        train_source_rows,
        val_source_rows,
        followup_train_quotas=followup_train_quotas,
        followup_val_quotas=followup_val_quotas,
        opening_train_target=opening_train_target,
        opening_val_target=opening_val_target,
    )

    _write_jsonl(train_out_path, train_rows)
    _write_jsonl(val_out_path, val_rows)
    _write_jsonl(benchmark_out_path, benchmark_rows)
    train_chat_count = export_chat_format(
        train_out_path,
        train_chat_out_path,
        prompt_variant="compact-v2",
    )
    val_chat_count = export_chat_format(
        val_out_path,
        val_chat_out_path,
        prompt_variant="compact-v2",
    )
    benchmark_chat_count = 0
    if benchmark_chat_out_path is not None:
        benchmark_chat_count = export_chat_format(
            benchmark_out_path,
            benchmark_chat_out_path,
            prompt_variant="compact-v2",
        )

    benchmark_counts = Counter(str(row.get("decision_bucket") or "other") for row in benchmark_rows)
    benchmark_source_counts = Counter(str(row.get("source_bucket") or "unknown") for row in benchmark_rows)
    train_manual_review_ids = _review_ids(manual_train_rows)
    val_manual_review_ids = _review_ids(manual_val_rows)
    benchmark_review_ids = _review_ids(benchmark_rows)

    manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "review_path": str(review_path.resolve()),
            "labels_path": str(labels_path.resolve()),
            "train_source_path": str(train_source_path.resolve()),
            "val_source_path": str(val_source_path.resolve()),
        },
        "outputs": {
            "train_out_path": str(train_out_path.resolve()),
            "val_out_path": str(val_out_path.resolve()),
            "benchmark_out_path": str(benchmark_out_path.resolve()),
            "train_chat_out_path": str(train_chat_out_path.resolve()),
            "val_chat_out_path": str(val_chat_out_path.resolve()),
            "benchmark_chat_out_path": str(benchmark_chat_out_path.resolve())
            if benchmark_chat_out_path is not None
            else None,
        },
        "manual_review_rows": len(review_rows),
        "manual_rows_kept": len(manual_rows),
        "manual_decision_counts": dict(decision_counts),
        "manual_drop_counts": dict(label_drop_counts),
        "manual_split_counts": {
            "train_unique": len(train_manual_review_ids),
            "train_after_duplication": len(manual_train_rows),
            "val": len(manual_val_rows),
            "benchmark": len(benchmark_rows),
        },
        "duplicate_counts": dict(duplicate_counts),
        "anchor_targets": {
            "manual_train_target": manual_train_target,
            "opening_train_target": opening_train_target,
            "opening_val_target": opening_val_target,
            "followup_train_quotas": followup_train_quotas,
            "followup_val_quotas": followup_val_quotas,
        },
        "output_counts": {
            "train_rows": len(train_rows),
            "val_rows": len(val_rows),
            "benchmark_rows": len(benchmark_rows),
            "train_chat_rows": train_chat_count,
            "val_chat_rows": val_chat_count,
            "benchmark_chat_rows": benchmark_chat_count,
        },
        "source_counts": dict(source_counts),
        "benchmark_bucket_counts": dict(benchmark_counts),
        "benchmark_source_counts": dict(benchmark_source_counts),
        "review_coverage": {
            "train_review_ids": len(train_manual_review_ids),
            "val_review_ids": len(val_manual_review_ids),
            "benchmark_review_ids": len(benchmark_review_ids),
        },
        "diagnostics": {
            key: dict(counter)
            for key, counter in diagnostics.items()
        },
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a curated exploration-judgment dataset from manual review labels."
    )
    parser.add_argument("--review-input", type=Path, required=True, help="Review-set JSONL input.")
    parser.add_argument("--labels-input", type=Path, required=True, help="Manual labels JSONL input.")
    parser.add_argument(
        "--train-source",
        type=Path,
        default=_default_final_v4("train"),
        help="Pathless final-v4 train JSONL source for anchors.",
    )
    parser.add_argument(
        "--val-source",
        type=Path,
        default=_default_final_v4("val"),
        help="Pathless final-v4 val JSONL source for anchors.",
    )
    parser.add_argument("--train-out", type=Path, required=True, help="Curated train JSONL output.")
    parser.add_argument("--val-out", type=Path, required=True, help="Curated val JSONL output.")
    parser.add_argument("--benchmark-out", type=Path, required=True, help="Held-out benchmark JSONL output.")
    parser.add_argument("--train-chat-out", type=Path, required=True, help="Compact-v2 train chat export.")
    parser.add_argument("--val-chat-out", type=Path, required=True, help="Compact-v2 val chat export.")
    parser.add_argument(
        "--benchmark-chat-out",
        type=Path,
        help="Optional compact-v2 benchmark chat export.",
    )
    parser.add_argument("--manifest-out", type=Path, required=True, help="Manifest JSON output.")
    parser.add_argument("--manual-train-target", type=int, default=112)
    parser.add_argument("--manual-benchmark-target", type=int, default=24)
    parser.add_argument("--manual-val-target", type=int, default=12)
    parser.add_argument("--opening-train-target", type=int, default=32)
    parser.add_argument("--opening-val-target", type=int, default=8)
    parser.add_argument(
        "--followup-train-quota",
        action="append",
        help="Follow-up train quota in TOOL:COUNT form. Repeatable.",
    )
    parser.add_argument(
        "--followup-val-quota",
        action="append",
        help="Follow-up val quota in TOOL:COUNT form. Repeatable.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_exploration_judgment_dataset(
        review_path=args.review_input,
        labels_path=args.labels_input,
        train_source_path=args.train_source,
        val_source_path=args.val_source,
        train_out_path=args.train_out,
        val_out_path=args.val_out,
        benchmark_out_path=args.benchmark_out,
        train_chat_out_path=args.train_chat_out,
        val_chat_out_path=args.val_chat_out,
        benchmark_chat_out_path=args.benchmark_chat_out,
        manifest_path=args.manifest_out,
        manual_train_target=max(1, int(args.manual_train_target)),
        manual_benchmark_target=max(0, int(args.manual_benchmark_target)),
        manual_val_target=max(0, int(args.manual_val_target)),
        opening_train_target=max(0, int(args.opening_train_target)),
        opening_val_target=max(0, int(args.opening_val_target)),
        followup_train_quotas=_parse_quota(args.followup_train_quota, DEFAULT_FOLLOWUP_TRAIN_QUOTAS),
        followup_val_quotas=_parse_quota(args.followup_val_quota, DEFAULT_FOLLOWUP_VAL_QUOTAS),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
