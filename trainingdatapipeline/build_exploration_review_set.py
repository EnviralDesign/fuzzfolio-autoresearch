"""Build reviewer-oriented exploration-judgment candidates from real runs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION
from .discover_runs import discover_runs
from .normalize_state import normalize_record
from .replay_controller_state import replay_run_steps

DEFAULT_BUCKET_QUOTAS = {
    "mutate_or_simplify": 36,
    "reseed_scaffold": 42,
    "evaluate": 18,
    "inspect_or_compare": 18,
    "advance_candidate": 18,
    "sweep_or_broaden": 12,
    "opening_scaffold": 6,
}


def _default_runs_root() -> Path:
    return Path(__file__).resolve().parents[1] / "runs"


def _parse_quota(items: list[str] | None) -> dict[str, int]:
    if not items:
        return dict(DEFAULT_BUCKET_QUOTAS)
    quotas: dict[str, int] = {}
    for item in items:
        bucket, _, count_text = str(item).partition(":")
        if not bucket or not count_text:
            raise ValueError(f"Invalid quota entry: {item!r}")
        quotas[bucket.strip()] = int(count_text)
    return quotas


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _stable_review_id(run_id: str, step: int, bucket: str) -> str:
    token = _stable_hash(f"{run_id}:{step}:{bucket}")[:10]
    return f"REV-{token.upper()}"


def _sort_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(row.get("run_id") or ""),
        int(row.get("step") or 0),
        str(row.get("example_id") or ""),
    )


def _first_action(record: dict[str, Any]) -> dict[str, Any] | None:
    target = (
        record.get("target_response_normalized")
        if isinstance(record.get("target_response_normalized"), dict)
        else {}
    )
    actions = target.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    return first if isinstance(first, dict) else None


def _first_tool(record: dict[str, Any]) -> str:
    first = _first_action(record)
    return str(first.get("tool") or "").strip() if isinstance(first, dict) else ""


def _prompt_state(record: dict[str, Any]) -> dict[str, Any]:
    return record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}


def _recent_attempts(record: dict[str, Any]) -> list[dict[str, Any]]:
    prompt_state = _prompt_state(record)
    attempts = prompt_state.get("recent_attempts")
    if not isinstance(attempts, list):
        return []
    return [item for item in attempts if isinstance(item, dict)]


def _current_result_facts(record: dict[str, Any]) -> list[dict[str, Any]]:
    facts = record.get("current_result_facts")
    if not isinstance(facts, list):
        return []
    return [item for item in facts if isinstance(item, dict)]


def _tool_results_summary(record: dict[str, Any]) -> list[dict[str, Any]]:
    items = record.get("tool_results_summary")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _result_errors(record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for item in _tool_results_summary(record):
        direct_error = str(item.get("error") or "").strip()
        if direct_error:
            errors.append(direct_error)
        for err in item.get("errors") or []:
            text = str(err or "").strip()
            if text:
                errors.append(text)
    return errors


def _recent_best_score(record: dict[str, Any]) -> float | None:
    scores = []
    for item in _recent_attempts(record):
        value = item.get("composite_score")
        try:
            if value is not None:
                scores.append(float(value))
        except (TypeError, ValueError):
            continue
    if not scores:
        return None
    return max(scores)


def _recent_scored_attempt_count(record: dict[str, Any]) -> int:
    count = 0
    for item in _recent_attempts(record):
        if item.get("composite_score") is not None:
            count += 1
    return count


def _current_step_score(record: dict[str, Any]) -> float | None:
    for fact in _current_result_facts(record):
        value = fact.get("score")
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _current_attempt_id(record: dict[str, Any]) -> str | None:
    for fact in _current_result_facts(record):
        token = str(fact.get("attempt_id") or "").strip()
        if token:
            return token
    return None


def _is_infra_failure(record: dict[str, Any]) -> bool:
    lowered = " ".join(_result_errors(record)).lower()
    needles = (
        "actively refused",
        "tcp connect error",
        "client error (connect)",
        "http request failed",
        "connection could be made",
    )
    return any(item in lowered for item in needles)


def _is_mechanical_failure(record: dict[str, Any]) -> bool:
    lowered = " ".join(_result_errors(record)).lower()
    needles = (
        "requires candidate_name or profile_ref",
        "requires mode",
        "each indicator payload must contain a meta object",
        "unknown instrument",
        "expected key=value pair",
        "missing required",
        "invalid",
        "failed to read json file",
        "array index",
        "path segment",
        "expected an object",
        "profile payload must contain an indicators array",
    )
    return any(item in lowered for item in needles)


def _has_trace_marker(record: dict[str, Any], key: str) -> bool:
    markers = record.get("trace_markers")
    if not isinstance(markers, dict):
        return False
    return bool(markers.get(key))


def _repeat_tool_streak(record: dict[str, Any], depth: int = 3) -> bool:
    tool = _first_tool(record)
    if not tool:
        return False
    prompt_state = _prompt_state(record)
    recent_steps = prompt_state.get("recent_step_window")
    if not isinstance(recent_steps, list) or len(recent_steps) < depth:
        return False
    comparable = recent_steps[-depth:]
    seen_tools: list[str] = []
    for item in comparable:
        if not isinstance(item, dict):
            return False
        sigs = item.get("action_signatures")
        if not isinstance(sigs, list) or not sigs:
            return False
        first = sigs[0]
        if not isinstance(first, dict):
            return False
        seen_tools.append(str(first.get("tool") or "").strip())
    return bool(seen_tools) and all(item == tool for item in seen_tools)


def _decision_bucket(record: dict[str, Any]) -> str:
    tool = _first_tool(record)
    step = int(record.get("step") or 0)
    if tool == "prepare_profile":
        return "opening_scaffold" if step <= 1 else "reseed_scaffold"
    if tool == "mutate_profile":
        return "mutate_or_simplify"
    if tool in {"validate_profile", "register_profile"}:
        return "advance_candidate"
    if tool == "evaluate_candidate":
        return "evaluate"
    if tool in {"inspect_artifact", "compare_artifacts"}:
        return "inspect_or_compare"
    if tool == "run_parameter_sweep":
        return "sweep_or_broaden"
    return "other"


def _outcome_class(record: dict[str, Any]) -> str:
    if _is_infra_failure(record):
        return "infra_fail"
    if _is_mechanical_failure(record):
        return "mechanical_fail"
    if _current_step_score(record) is not None:
        return "productive_scored"
    if _current_attempt_id(record):
        return "productive_unscored"
    if _has_trace_marker(record, "step_guard_triggered") or _repeat_tool_streak(record):
        return "stale_loop"
    bucket = _decision_bucket(record)
    if bucket == "reseed_scaffold" and _recent_scored_attempt_count(record) > 0:
        return "strategically_weak_but_valid"
    return "ambiguous"


def _interesting_tags(record: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    bucket = _decision_bucket(record)
    step = int(record.get("step") or 0)
    errors = " ".join(_result_errors(record)).lower()
    if bucket == "reseed_scaffold" and _recent_scored_attempt_count(record) > 0:
        tags.append("reseed_after_scored_attempt")
    if bucket == "reseed_scaffold" and step >= 15:
        tags.append("late_reseed")
    if bucket == "mutate_or_simplify" and "meta object" in errors:
        tags.append("indicator_meta_patch_fail")
    if "unknown instrument" in errors:
        tags.append("unknown_instrument")
    if "requires candidate_name or profile_ref" in errors:
        tags.append("missing_handle")
    if _is_infra_failure(record):
        tags.append("service_unavailable")
    if _has_trace_marker(record, "step_guard_triggered"):
        tags.append("blocked_after_fail")
    if _repeat_tool_streak(record):
        tags.append("repeat_tool_streak")
    if _current_step_score(record) is not None:
        tags.append("scored_attempt")
    elif _current_attempt_id(record):
        tags.append("attempt_logged")
    deduped: list[str] = []
    for item in tags:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _interest_score(record: dict[str, Any]) -> int:
    outcome = _outcome_class(record)
    bucket = _decision_bucket(record)
    score = {
        "productive_scored": 100,
        "strategically_weak_but_valid": 90,
        "stale_loop": 80,
        "productive_unscored": 70,
        "mechanical_fail": 60,
        "ambiguous": 40,
        "infra_fail": 10,
    }.get(outcome, 20)
    if bucket in {"mutate_or_simplify", "reseed_scaffold", "sweep_or_broaden"}:
        score += 8
    if _recent_scored_attempt_count(record) > 0:
        score += 6
    if int(record.get("step") or 0) >= 15:
        score += 4
    score += len(_interesting_tags(record))
    return score


def _state_summary(record: dict[str, Any]) -> str:
    phase = str(record.get("phase") or "unknown")
    best_score = _recent_best_score(record)
    scored_count = _recent_scored_attempt_count(record)
    controller = (
        _prompt_state(record).get("controller")
        if isinstance(_prompt_state(record).get("controller"), dict)
        else {}
    )
    score_target = str(controller.get("score_target") or "").strip()
    score_bits = []
    if best_score is not None:
        score_bits.append(f"best={best_score:.3f}")
    if scored_count:
        score_bits.append(f"scored={scored_count}")
    if score_target:
        score_bits.append(score_target)
    joined = " | ".join(score_bits) if score_bits else "no prior scored attempts"
    return f"{phase} | {joined}"


def _action_summary(record: dict[str, Any]) -> str:
    signatures = record.get("action_signatures_normalized")
    if isinstance(signatures, list) and signatures:
        first = signatures[0]
        if isinstance(first, dict):
            return json.dumps(
                {key: value for key, value in first.items() if key != "signature"},
                ensure_ascii=True,
                sort_keys=True,
            )
    first = _first_action(record)
    if isinstance(first, dict):
        return json.dumps(first, ensure_ascii=True, sort_keys=True)
    return ""


def _result_summary_text(record: dict[str, Any]) -> str:
    items = _tool_results_summary(record)
    if not items:
        return ""
    first = items[0]
    if not isinstance(first, dict):
        return str(first)
    if first.get("error"):
        return str(first.get("error"))
    errors = first.get("errors")
    if isinstance(errors, list) and errors:
        return str(errors[0])
    if first.get("score") is not None:
        return f"score={first.get('score')}"
    if first.get("attempt_id"):
        return f"attempt={first.get('attempt_id')}"
    return json.dumps(first, ensure_ascii=True, sort_keys=True)


def _review_sheet_line(row: dict[str, Any]) -> str:
    return (
        f"- `{row['review_id']}` | step `{row['step']}` | "
        f"`{row['decision_bucket']}` / `{row['outcome_class']}` | "
        f"{row['review_state_summary']} | "
        f"`{row['review_action_summary']}` | "
        f"{row['review_result_summary'] or 'no result summary'}"
    )


def _candidate_row(normalized: dict[str, Any]) -> dict[str, Any] | None:
    tool = _first_tool(normalized)
    if not tool:
        return None
    bucket = _decision_bucket(normalized)
    if bucket == "other":
        return None
    run_id = str(normalized.get("run_id") or "")
    step = int(normalized.get("step") or 0)
    review_id = _stable_review_id(run_id, step, bucket)
    prompt_state = _prompt_state(normalized)
    run_metadata = (
        prompt_state.get("run_metadata")
        if isinstance(prompt_state.get("run_metadata"), dict)
        else {}
    )
    row = dict(normalized)
    row["review_id"] = review_id
    row["decision_bucket"] = bucket
    row["outcome_class"] = _outcome_class(normalized)
    row["interesting_tags"] = _interesting_tags(normalized)
    row["interest_score"] = _interest_score(normalized)
    row["review_state_summary"] = _state_summary(normalized)
    row["review_action_summary"] = _action_summary(normalized)
    row["review_result_summary"] = _result_summary_text(normalized)
    row["explorer_profile"] = str(run_metadata.get("explorer_profile") or "")
    row["review_sheet_line"] = _review_sheet_line(row)
    return row


def _unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def _select_rows(
    rows: list[dict[str, Any]],
    *,
    quotas: dict[str, int],
    max_per_run: int,
    include_example_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get("decision_bucket") or "other")].append(row)

    selected: list[dict[str, Any]] = []
    per_run_counts = Counter()
    include_example_ids = _unique_preserve_order(include_example_ids or [])
    selected_example_ids: set[str] = set()
    if include_example_ids:
        by_example_id = {
            str(row.get("example_id") or "").strip(): row
            for row in rows
            if str(row.get("example_id") or "").strip()
        }
        for example_id in include_example_ids:
            row = by_example_id.get(example_id)
            if row is None:
                continue
            selected.append(row)
            selected_example_ids.add(example_id)
            run_id = str(row.get("run_id") or "")
            per_run_counts[run_id] += 1
    for bucket, limit in quotas.items():
        candidates = buckets.get(bucket, [])
        candidates.sort(
            key=lambda row: (
                -int(row.get("interest_score") or 0),
                -int(row.get("step") or 0),
                str(row.get("run_id") or ""),
                str(row.get("review_id") or ""),
            )
        )
        preferred_candidates = [
            row
            for row in candidates
            if str(row.get("outcome_class") or "") not in {"infra_fail", "mechanical_fail"}
        ]
        fallback_candidates = [
            row
            for row in candidates
            if str(row.get("outcome_class") or "") in {"infra_fail", "mechanical_fail"}
        ]
        bucket_selected = 0
        for pool in (preferred_candidates, fallback_candidates):
            for row in pool:
                example_id = str(row.get("example_id") or "").strip()
                if example_id in selected_example_ids:
                    continue
                run_id = str(row.get("run_id") or "")
                if per_run_counts[run_id] >= max_per_run:
                    continue
                selected.append(row)
                per_run_counts[run_id] += 1
                bucket_selected += 1
                if bucket_selected >= limit:
                    break
            if bucket_selected >= limit:
                break
    selected.sort(key=_sort_key)
    return selected


def _render_sheet(path: Path, rows: list[dict[str, Any]], manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_bucket[str(row.get("decision_bucket") or "other")].append(row)
    sections = [
        "# Exploration Judgment Review Set v1",
        "",
        f"- Generated: `{manifest['generated_at']}`",
        f"- Candidate rows: `{manifest['rows_emitted']}`",
        f"- Source runs: `{manifest['selected_run_count']}`",
        "",
        "Use the `review_id` values as the stable keys for manual labels.",
        "",
    ]
    for bucket in quotas_order(by_bucket.keys()):
        sections.append(f"## {bucket}")
        sections.append("")
        for row in by_bucket[bucket]:
            sections.append(_review_sheet_line(row))
        sections.append("")
    path.write_text("\n".join(sections).rstrip() + "\n", encoding="utf-8")


def quotas_order(keys: Any) -> list[str]:
    preferred = list(DEFAULT_BUCKET_QUOTAS)
    seen = set(preferred)
    ordered = [item for item in preferred if item in set(keys)]
    ordered.extend(sorted(item for item in keys if item not in seen))
    return ordered


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as sink:
        for row in rows:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")


def _write_labels_template(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as sink:
        for row in rows:
            template = {
                "review_id": row.get("review_id"),
                "decision": "",
                "notes": "",
                "corrected_reasoning": "",
                "corrected_action": None,
            }
            sink.write(json.dumps(template, ensure_ascii=True) + "\n")


def build_exploration_review_set(
    *,
    roots: list[Path],
    output_path: Path,
    sheet_path: Path,
    manifest_path: Path,
    labels_template_path: Path | None = None,
    quotas: dict[str, int] | None = None,
    max_per_run: int = 6,
    include_run_ids: set[str] | None = None,
    exclude_run_ids: set[str] | None = None,
    include_example_ids: list[str] | None = None,
) -> dict[str, Any]:
    quotas = quotas or dict(DEFAULT_BUCKET_QUOTAS)
    include_run_ids = include_run_ids or set()
    exclude_run_ids = exclude_run_ids or set()
    include_example_ids = _unique_preserve_order(include_example_ids or [])
    discovered = discover_runs(roots)
    candidate_rows: list[dict[str, Any]] = []
    skipped = Counter()
    for run in discovered:
        if include_run_ids and run.run_id not in include_run_ids:
            skipped["not_included_run"] += 1
            continue
        if run.run_id in exclude_run_ids:
            skipped["excluded_run"] += 1
            continue
        for record in replay_run_steps(run):
            normalized = normalize_record(record)
            candidate = _candidate_row(normalized)
            if candidate is None:
                skipped["not_candidate"] += 1
                continue
            candidate_rows.append(candidate)

    selected = _select_rows(
        candidate_rows,
        quotas=quotas,
        max_per_run=max_per_run,
        include_example_ids=include_example_ids,
    )
    selection_counts = Counter(str(row.get("decision_bucket") or "other") for row in selected)
    outcome_counts = Counter(str(row.get("outcome_class") or "ambiguous") for row in selected)
    selected_runs = {str(row.get("run_id") or "") for row in selected}
    selected_example_ids = {
        str(row.get("example_id") or "").strip()
        for row in selected
        if str(row.get("example_id") or "").strip()
    }
    included_found = [example_id for example_id in include_example_ids if example_id in selected_example_ids]
    included_missing = [example_id for example_id in include_example_ids if example_id not in selected_example_ids]

    _write_jsonl(output_path, selected)
    if labels_template_path is not None:
        _write_labels_template(labels_template_path, selected)

    manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "roots": [str(path.resolve()) for path in roots],
        "output_path": str(output_path.resolve()),
        "sheet_path": str(sheet_path.resolve()),
        "labels_template_path": str(labels_template_path.resolve()) if labels_template_path else None,
        "quotas": quotas,
        "max_per_run": max_per_run,
        "include_example_ids_requested": include_example_ids,
        "include_example_ids_found": included_found,
        "include_example_ids_missing": included_missing,
        "source_run_count": len(discovered),
        "candidate_pool_size": len(candidate_rows),
        "rows_emitted": len(selected),
        "selected_run_count": len(selected_runs),
        "selection_counts": dict(selection_counts),
        "outcome_counts": dict(outcome_counts),
        "skipped_counts": dict(skipped),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _render_sheet(sheet_path, selected, manifest)
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an exploration-judgment review set.")
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        help="Run corpus root or specific run directory. Repeatable.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output review-set JSONL.")
    parser.add_argument("--sheet-out", type=Path, required=True, help="Output markdown review sheet.")
    parser.add_argument("--manifest-out", type=Path, required=True, help="Output manifest JSON.")
    parser.add_argument(
        "--labels-template-out",
        type=Path,
        help="Optional blank manual-label template JSONL.",
    )
    parser.add_argument(
        "--quota",
        action="append",
        help="Per-bucket quota in BUCKET:COUNT form. Repeatable.",
    )
    parser.add_argument("--max-per-run", type=int, default=6, help="Max selected rows per run.")
    parser.add_argument(
        "--include-run-id",
        action="append",
        default=[],
        help="Only include these run ids. Repeatable.",
    )
    parser.add_argument(
        "--exclude-run-id",
        action="append",
        default=[],
        help="Exclude these run ids. Repeatable.",
    )
    parser.add_argument(
        "--include-example-id",
        action="append",
        default=[],
        help="Guarantee these example ids are emitted if discovered. Repeatable.",
    )
    parser.add_argument(
        "--include-example-id-file",
        type=Path,
        help="Optional newline-delimited file of example ids to guarantee in the output.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    roots = [Path(item) for item in args.roots] if args.roots else [_default_runs_root()]
    include_example_ids = list(args.include_example_id)
    if args.include_example_id_file is not None:
        include_example_ids.extend(
            line.strip()
            for line in args.include_example_id_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    build_exploration_review_set(
        roots=roots,
        output_path=args.out,
        sheet_path=args.sheet_out,
        manifest_path=args.manifest_out,
        labels_template_path=args.labels_template_out,
        quotas=_parse_quota(args.quota),
        max_per_run=max(1, int(args.max_per_run)),
        include_run_ids=set(args.include_run_id),
        exclude_run_ids=set(args.exclude_run_id),
        include_example_ids=include_example_ids,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
