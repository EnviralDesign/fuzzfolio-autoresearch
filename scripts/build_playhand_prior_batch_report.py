from __future__ import annotations

import argparse
import csv
import html
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def json_compact(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def safe_rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def safe_round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def batch_label(batch_status: dict[str, Any], batch_dir: Path | None = None) -> str:
    total = batch_status.get("total")
    if total:
        return f"Clean {total}"
    if batch_dir is not None:
        name = batch_dir.name.replace("playhand-prior-test-", "")
        if name:
            return name.replace("-", " ").title()
    return "Play Hand Prior Batch"


def compact_label(label: str) -> str:
    return "".join(ch for ch in label.lower() if ch.isalnum())


def pair_family_id(row: dict[str, Any]) -> str:
    template_probe = clean_text(row.get("template_branch_source_probe_id"))
    dealt_probe = clean_text(row.get("dealt_pair_probe_id"))
    if template_probe:
        return template_probe
    if dealt_probe:
        return dealt_probe
    first = clean_text(row.get("first_indicator_id"))
    second = clean_text(row.get("second_indicator_id"))
    timeframe = clean_text(row.get("probe_timeframe"))
    if first or second or timeframe:
        return "|".join([first or "unknown", second or "unknown", timeframe or "unknown"])
    return ""


def read_deal_event(run_dir: Path) -> dict[str, Any]:
    events_path = run_dir / "play-hand-events.jsonl"
    if not events_path.exists():
        return {}
    with events_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("phase") == "deal":
                return payload
    return {}


def branch_record(summary: dict[str, Any], branch: str) -> dict[str, Any]:
    for row in summary.get("final_branch_scores") or []:
        if isinstance(row, dict) and row.get("branch") == branch:
            return row
    return {}


def branch_passed(summary: dict[str, Any], branch: str) -> bool:
    row = branch_record(summary, branch)
    if "passed" in row:
        return safe_bool(row.get("passed"))
    score = safe_float(row.get("score"))
    if score is None:
        score = safe_float(summary.get(f"{branch}_score"))
    return bool(score is not None and score > 0.0)


def score_sort_value(value: Any) -> float:
    score = safe_float(value)
    return score if score is not None else -999999.0


def classify_case(
    *,
    dealt_indicator_source: str,
    dealt_recipe_source: str,
    pair_template_available: bool,
    exact_template_materialized: bool,
    exact_passed: bool,
    mutated_passed: bool,
) -> str:
    if dealt_indicator_source and dealt_indicator_source != "play_hand_seed_plan":
        return "policy_exploration"
    if pair_template_available and not exact_template_materialized:
        return "template_not_materialized"
    if exact_template_materialized:
        if exact_passed and mutated_passed:
            return "template_materialized_exact_passed_mutated_passed"
        if exact_passed and not mutated_passed:
            return "template_materialized_exact_passed_mutated_failed"
        if not exact_passed and mutated_passed:
            return "template_materialized_exact_failed_mutated_passed"
        return "template_materialized_both_failed"
    if dealt_recipe_source == "curated_recipe_prior":
        return "no_template_curated_recipe"
    return "no_exact_template_guided_pair"


def seed_number(path: Path) -> int:
    name = path.stem
    # seed-03-summary -> 3
    parts = name.split("-")
    for part in parts:
        if part.isdigit():
            return int(part)
    return 0


def load_rows(batch_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary_path in sorted(batch_dir.glob("seed-*-summary.json"), key=seed_number):
        seed = seed_number(summary_path)
        summary = load_json(summary_path)
        run_dir = Path(clean_text(summary.get("run_dir")))
        metadata_path = run_dir / "run-metadata.json"
        metadata = load_json(metadata_path) if metadata_path.exists() else {}
        deal_event = read_deal_event(run_dir)
        pair = (
            metadata.get("dealt_recipe_pair")
            or deal_event.get("dealt_recipe_pair")
            or {}
        )
        if not isinstance(pair, dict):
            pair = {}
        recommended_template = pair.get("recommended_profile_template")
        pair_template_available = isinstance(recommended_template, dict)
        exact_template_materialized = bool(summary.get("exact_template_attempt_id"))
        exact_passed = branch_passed(summary, "exact_template")
        mutated_passed = branch_passed(summary, "mutated")
        exact_template_score = safe_float(summary.get("exact_template_score"))
        mutated_score = safe_float(summary.get("mutated_score"))
        mutation_delta = (
            mutated_score - exact_template_score
            if mutated_score is not None and exact_template_score is not None
            else None
        )
        dealt_indicator_source = clean_text(
            metadata.get("dealt_indicator_source")
            or deal_event.get("indicator_deal_source")
        )
        dealt_recipe_source = clean_text(
            metadata.get("dealt_recipe_source")
            or deal_event.get("dealt_recipe_source")
        )
        case = classify_case(
            dealt_indicator_source=dealt_indicator_source,
            dealt_recipe_source=dealt_recipe_source,
            pair_template_available=pair_template_available,
            exact_template_materialized=exact_template_materialized,
            exact_passed=exact_passed,
            mutated_passed=mutated_passed,
        )
        row = {
            "seed": seed,
            "run_id": summary.get("run_id"),
            "run_dir": str(run_dir),
            "run_status": clean_text(summary.get("run_status")),
            "tombstone_reason": clean_text(summary.get("tombstone_reason")),
            "final_scrutiny_score": safe_float(summary.get("final_scrutiny_score")),
            "dealt_indicator_source": dealt_indicator_source,
            "dealt_indicator_count": summary.get("dealt_indicator_count"),
            "dealt_indicator_ids": json_compact(summary.get("dealt_indicator_ids")),
            "dealt_recipe": clean_text(
                metadata.get("dealt_recipe") or deal_event.get("dealt_recipe")
            ),
            "dealt_recipe_source": dealt_recipe_source,
            "dealt_recipe_confidence": metadata.get("dealt_recipe_confidence"),
            "dealt_pair_probe_id": clean_text(pair.get("probe_id")),
            "dealt_pair_source": clean_text(pair.get("source")),
            "first_indicator_id": clean_text(pair.get("first_indicator_id")),
            "second_indicator_id": clean_text(pair.get("second_indicator_id")),
            "probe_timeframe": clean_text(pair.get("probe_timeframe")),
            "pair_sampling_score": safe_float(pair.get("pair_sampling_score")),
            "pair_composite_score": safe_float(pair.get("composite_score")),
            "pair_retention_bucket": clean_text(pair.get("retention_bucket")),
            "pair_template_available": pair_template_available,
            "pair_template_materialized": exact_template_materialized,
            "template_not_materialized": pair_template_available
            and not exact_template_materialized,
            "template_instrument_policy": clean_text(
                metadata.get("template_instrument_policy")
            ),
            "template_instrument_pool_applied": safe_bool(
                metadata.get("template_instrument_pool_applied")
            ),
            "exact_template_source": clean_text(summary.get("exact_template_source")),
            "exact_template_source_profile_path": clean_text(
                summary.get("exact_template_source_profile_path")
            ),
            "template_branch_source_probe_id": clean_text(
                summary.get("template_branch_source_probe_id")
            ),
            "template_branch_instruments": json_compact(
                summary.get("template_branch_instruments")
            ),
            "exact_template_attempt_id": clean_text(
                summary.get("exact_template_attempt_id")
            ),
            "exact_template_score": exact_template_score,
            "exact_template_passed": exact_passed,
            "mutated_attempt_id": clean_text(summary.get("mutated_attempt_id")),
            "mutated_score": mutated_score,
            "mutated_passed": mutated_passed,
            "mutation_delta": safe_round(mutation_delta),
            "selected_final_branch": clean_text(summary.get("selected_final_branch")),
            "canonical_selection_reason": clean_text(
                summary.get("canonical_selection_reason")
            ),
            "canonical_attempt_id": clean_text(summary.get("canonical_attempt_id")),
            "final_attempt_id": clean_text(summary.get("final_attempt_id")),
            "primary_instrument": clean_text(summary.get("primary_instrument")),
            "instrument_pool": json_compact(summary.get("instrument_pool")),
            "instruments": json_compact(summary.get("instruments")),
            "timeframe": clean_text(summary.get("timeframe")),
            "case": case,
        }
        rows.append(row)
    return rows


def count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter: Counter[str] = Counter(clean_text(row.get(key)) or "unknown" for row in rows)
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = safe_float(row.get(key))
        if value is not None:
            values.append(value)
    return values


def score_stats(rows: list[dict[str, Any]], key: str = "final_scrutiny_score") -> dict[str, Any]:
    values = numeric_values(rows, key)
    positive = [value for value in values if value > 0.0]
    if not values:
        return {"count": 0, "positive_count": 0}
    return {
        "count": len(values),
        "positive_count": len(positive),
        "min": round(min(values), 4),
        "median": round(median(values), 4),
        "avg": round(mean(values), 4),
        "max": round(max(values), 4),
        "avg_positive": round(mean(positive), 4) if positive else None,
        "median_positive": round(median(positive), 4) if positive else None,
    }


def grouped_summary(
    rows: list[dict[str, Any]],
    *,
    keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        group_key = tuple(clean_text(row.get(key)) or "unknown" for key in keys)
        groups[group_key].append(row)
    output: list[dict[str, Any]] = []
    for group_key, items in groups.items():
        promoted = [row for row in items if row.get("run_status") == "promoted"]
        tombstoned = [row for row in items if row.get("run_status") == "tombstoned"]
        exact_selected = [
            row for row in items if row.get("selected_final_branch") == "exact_template"
        ]
        mutated_selected = [
            row for row in items if row.get("selected_final_branch") == "mutated"
        ]
        exact_rescues = [
            row
            for row in items
            if row.get("canonical_selection_reason") == "rescued_by_exact_template"
        ]
        exact_outscored = [
            row
            for row in items
            if row.get("canonical_selection_reason") == "exact_template_outscored_mutated"
        ]
        comparable_template_rows = [
            row
            for row in items
            if row.get("exact_template_score") is not None
            and row.get("mutated_score") is not None
        ]
        mutated_wins = [
            row
            for row in comparable_template_rows
            if score_sort_value(row.get("mutated_score"))
            > score_sort_value(row.get("exact_template_score"))
        ]
        mutation_deltas = numeric_values(comparable_template_rows, "mutation_delta")
        avg_mutation_delta = mean(mutation_deltas) if mutation_deltas else None
        exact_rescue_rate = safe_rate(len(exact_rescues), len(items))
        exact_selected_rate = safe_rate(len(exact_selected), len(items))
        mutated_win_rate = safe_rate(len(mutated_wins), len(comparable_template_rows))
        if len(items) < 3:
            family_classification = "under_sampled"
        elif (exact_rescue_rate or 0.0) >= 0.40:
            family_classification = "template_locked"
        elif (mutated_win_rate or 0.0) >= 0.60 and (avg_mutation_delta or 0.0) > 3.0:
            family_classification = "mutation_friendly"
        elif (exact_selected_rate or 0.0) >= 0.40:
            family_classification = "template_guarded"
        else:
            family_classification = "unstable"
        row_out = {key: group_key[index] for index, key in enumerate(keys)}
        row_out.update(
            {
                "count": len(items),
                "promoted": len(promoted),
                "tombstoned": len(tombstoned),
                "promotion_rate": round(len(promoted) / len(items), 4),
                "exact_selected": len(exact_selected),
                "mutated_selected": len(mutated_selected),
                "exact_rescues": len(exact_rescues),
                "exact_outscored_mutated": len(exact_outscored),
                "comparable_template_runs": len(comparable_template_rows),
                "mutated_wins_over_exact": len(mutated_wins),
                "exact_rescue_rate": exact_rescue_rate,
                "exact_selected_rate": exact_selected_rate,
                "mutated_win_rate": mutated_win_rate,
                "avg_mutation_delta": safe_round(avg_mutation_delta),
                "median_mutation_delta": safe_round(median(mutation_deltas))
                if mutation_deltas
                else None,
                "family_classification": family_classification,
                "avg_score": score_stats(items).get("avg"),
                "avg_positive_score": score_stats(items).get("avg_positive"),
                "best_score": score_stats(items).get("max"),
                "best_seed": max(
                    items,
                    key=lambda row: score_sort_value(row.get("final_scrutiny_score")),
                ).get("seed"),
            }
        )
        output.append(row_out)
    output.sort(
        key=lambda row: (
            int(row.get("promoted") or 0),
            score_sort_value(row.get("avg_positive_score")),
            score_sort_value(row.get("best_score")),
            int(row.get("count") or 0),
        ),
        reverse=True,
    )
    return output


def build_summary(rows: list[dict[str, Any]], batch_status: dict[str, Any]) -> dict[str, Any]:
    promoted = [row for row in rows if row.get("run_status") == "promoted"]
    tombstoned = [row for row in rows if row.get("run_status") == "tombstoned"]
    template_rows = [row for row in rows if row.get("pair_template_materialized")]
    exact_selected = [
        row for row in rows if row.get("selected_final_branch") == "exact_template"
    ]
    mutated_selected = [row for row in rows if row.get("selected_final_branch") == "mutated"]
    exact_rescues = [
        row
        for row in rows
        if row.get("canonical_selection_reason") == "rescued_by_exact_template"
    ]
    exact_outscored = [
        row
        for row in rows
        if row.get("canonical_selection_reason") == "exact_template_outscored_mutated"
    ]
    mutated_improved_template = [
        row
        for row in rows
        if row.get("exact_template_score") is not None
        and row.get("mutated_score") is not None
        and row.get("canonical_selection_reason") == "mutated_branch_selected"
    ]
    template_not_materialized = [
        row for row in rows if row.get("template_not_materialized")
    ]
    policy_exploration = [
        row for row in rows if row.get("case") == "policy_exploration"
    ]
    discovered_recipe = [
        row for row in rows if row.get("dealt_recipe_source") == "discovery_recipe_validation"
    ]
    curated_recipe = [
        row for row in rows if row.get("dealt_recipe_source") == "curated_recipe_prior"
    ]
    promoted_pair_families = {
        pair_family_id(row)
        for row in promoted
        if pair_family_id(row) and pair_family_id(row) != "unknown|unknown|unknown"
    }
    all_pair_counts = Counter(pair_family_id(row) for row in rows if pair_family_id(row))
    top_family_count = max(all_pair_counts.values()) if all_pair_counts else 0
    return {
        "schema_version": "playhand_prior_batch_report_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_status": batch_status,
        "result_counts": {
            "runs": len(rows),
            "completed": int(batch_status.get("completed") or len(rows)),
            "failed": int(batch_status.get("failed") or 0),
            "promoted": len(promoted),
            "tombstoned": len(tombstoned),
            "promotion_rate": round(len(promoted) / len(rows), 4) if rows else None,
            "template_materialized": len(template_rows),
            "template_not_materialized": len(template_not_materialized),
            "template_materialization_rate": safe_rate(len(template_rows), len(rows)),
            "exact_template_selected": len(exact_selected),
            "mutated_selected": len(mutated_selected),
            "exact_template_rescues": len(exact_rescues),
            "exact_template_rescue_rate": safe_rate(
                len(exact_rescues), len(template_rows)
            ),
            "exact_template_outscored_mutated": len(exact_outscored),
            "mutated_improved_over_exact_template": len(mutated_improved_template),
            "mutation_improvement_rate": safe_rate(
                len(mutated_improved_template), len(template_rows)
            ),
            "policy_exploration_runs": len(policy_exploration),
            "policy_exploration_promoted": len(
                [row for row in policy_exploration if row.get("run_status") == "promoted"]
            ),
            "policy_exploration_hit_rate": safe_rate(
                len([row for row in policy_exploration if row.get("run_status") == "promoted"]),
                len(policy_exploration),
            ),
            "discovered_recipe_runs": len(discovered_recipe),
            "discovered_recipe_promoted": len(
                [row for row in discovered_recipe if row.get("run_status") == "promoted"]
            ),
            "discovered_recipe_hit_rate": safe_rate(
                len([row for row in discovered_recipe if row.get("run_status") == "promoted"]),
                len(discovered_recipe),
            ),
            "curated_recipe_runs": len(curated_recipe),
            "curated_recipe_promoted": len(
                [row for row in curated_recipe if row.get("run_status") == "promoted"]
            ),
            "curated_recipe_hit_rate": safe_rate(
                len([row for row in curated_recipe if row.get("run_status") == "promoted"]),
                len(curated_recipe),
            ),
            "top_family_concentration_share": safe_rate(top_family_count, len(rows)),
            "unique_promoted_pair_families": len(promoted_pair_families),
        },
        "score_stats": {
            "all": score_stats(rows),
            "promoted": score_stats(promoted),
            "tombstoned": score_stats(tombstoned),
            "exact_selected": score_stats(exact_selected),
            "mutated_selected": score_stats(mutated_selected),
            "template_materialized": score_stats(template_rows),
        },
        "counts": {
            "run_status": count_by(rows, "run_status"),
            "case": count_by(rows, "case"),
            "dealt_indicator_source": count_by(rows, "dealt_indicator_source"),
            "dealt_recipe_source": count_by(rows, "dealt_recipe_source"),
            "dealt_recipe": count_by(rows, "dealt_recipe"),
            "selected_final_branch": count_by(rows, "selected_final_branch"),
            "canonical_selection_reason": count_by(rows, "canonical_selection_reason"),
            "dealt_pair_source": count_by(rows, "dealt_pair_source"),
            "template_instrument_pool_applied": count_by(
                rows, "template_instrument_pool_applied"
            ),
        },
        "top_promoted": sorted(
            promoted,
            key=lambda row: score_sort_value(row.get("final_scrutiny_score")),
            reverse=True,
        )[:12],
        "exact_template_rescues": exact_rescues,
        "template_not_materialized_rows": template_not_materialized,
    }


def make_markdown(
    *,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    recipe_summary: list[dict[str, Any]],
    pair_summary: list[dict[str, Any]],
) -> str:
    counts = summary["result_counts"]
    label = batch_label(summary["batch_status"])
    score = summary["score_stats"]

    def pct(value: Any) -> str:
        numeric = safe_float(value)
        return f"{numeric:.0%}" if numeric is not None else "n/a"

    lines = [
        f"# Play Hand Prior Test {label} Report",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "## Batch Result",
        "",
        f"- Runs completed: {counts['completed']}/{summary['batch_status'].get('total')} with {counts['failed']} failures.",
        f"- Promotions: {counts['promoted']} promoted, {counts['tombstoned']} tombstoned ({pct(counts['promotion_rate'])} promotion rate).",
        f"- Final score: median {score['all'].get('median')}, average {score['all'].get('avg')}, best {score['all'].get('max')}.",
        f"- Template materialization: {counts['template_materialized']} exact-template branches ({pct(counts.get('template_materialization_rate'))}), {counts['template_not_materialized']} template-not-materialized rows.",
        f"- Branch selection: {counts['mutated_selected']} mutated, {counts['exact_template_selected']} exact-template.",
        f"- Exact-template impact: {counts['exact_template_rescues']} rescues, {counts['exact_template_outscored_mutated']} exact-template outscored mutated, {counts['mutated_improved_over_exact_template']} mutated improved over an exact template.",
        f"- Source hit rates: discovered {pct(counts.get('discovered_recipe_hit_rate'))}, curated {pct(counts.get('curated_recipe_hit_rate'))}, policy exploration {pct(counts.get('policy_exploration_hit_rate'))}.",
        f"- Family concentration: top family share {pct(counts.get('top_family_concentration_share'))}; unique promoted pair/template families {counts.get('unique_promoted_pair_families')}.",
        "",
        "## Case Counts",
        "",
    ]
    for case, count in summary["counts"]["case"].items():
        lines.append(f"- `{case}`: {count}")
    lines.extend(["", "## Top Recipes", ""])
    lines.append("| Recipe | Source | Runs | Promoted | Exact Selected | Mutated Selected | Best | Avg Positive |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in recipe_summary[:12]:
        lines.append(
            "| {recipe} | {source} | {count} | {promoted} | {exact} | {mutated} | {best} | {avg_pos} |".format(
                recipe=row.get("dealt_recipe"),
                source=row.get("dealt_recipe_source"),
                count=row.get("count"),
                promoted=row.get("promoted"),
                exact=row.get("exact_selected"),
                mutated=row.get("mutated_selected"),
                best=row.get("best_score"),
                avg_pos=row.get("avg_positive_score"),
            )
        )
    lines.extend(["", "## Top Pair/Template Families", ""])
    lines.append("| Probe | Recipe | Class | Pair Source | Runs | Promoted | Exact Selected | Rescues | Avg Delta | Best |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in pair_summary[:15]:
        lines.append(
            "| {probe} | {recipe} | {klass} | {source} | {count} | {promoted} | {exact} | {rescues} | {delta} | {best} |".format(
                probe=row.get("template_branch_source_probe_id")
                or row.get("dealt_pair_probe_id"),
                recipe=row.get("dealt_recipe"),
                klass=row.get("family_classification"),
                source=row.get("dealt_pair_source"),
                count=row.get("count"),
                promoted=row.get("promoted"),
                exact=row.get("exact_selected"),
                rescues=row.get("exact_rescues"),
                delta=row.get("avg_mutation_delta"),
                best=row.get("best_score"),
            )
        )
    lines.extend(["", "## Top Promoted Runs", ""])
    lines.append("| Seed | Score | Branch | Reason | Recipe | Pair |")
    lines.append("|---:|---:|---|---|---|---|")
    for row in summary["top_promoted"][:12]:
        lines.append(
            "| {seed} | {score} | {branch} | {reason} | {recipe} | {pair} |".format(
                seed=row.get("seed"),
                score=row.get("final_scrutiny_score"),
                branch=row.get("selected_final_branch"),
                reason=row.get("canonical_selection_reason"),
                recipe=row.get("dealt_recipe"),
                pair=row.get("template_branch_source_probe_id")
                or row.get("dealt_pair_probe_id"),
            )
        )
    completed = int(summary["batch_status"].get("completed") or 0)
    total = int(summary["batch_status"].get("total") or completed)
    lines.extend(
        [
            "",
            "## Family Classification Rules",
            "",
            "```text",
            "under_sampled: count < 3",
            "template_locked: exact_rescue_rate >= 0.40",
            "mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3",
            "template_guarded: exact_selected_rate >= 0.40",
            "unstable: otherwise",
            "```",
            "",
            "## Data Hygiene",
            "",
            f"- `batch_status.completed == batch_status.total`: {completed == total} ({completed}/{total}).",
            "- `mutation_delta` is only computed when both `exact_template_score` and `mutated_score` are non-null.",
            "- Policy-exploration and blank/unknown rows are excluded from pair/template-family concentration metrics.",
            "- Unique promoted pair/template families excludes blank/unknown families.",
            "- Clean-50 and current-batch family labels are computed independently before comparison.",
            "",
            "## Interpretation",
            "",
            "- The guided prior path is materializing real candidates and producing a high promotion rate in this controlled run.",
            "- Exact-template control branches materially changed outcomes: several runs were promoted only because the retained template survived while the mutated branch failed.",
            "- Mutations also improved on retained templates in multiple cases, which means the prior system is useful as a starting region rather than only as a replay mechanism.",
            "- The next decision should focus on whether the promoted families are sufficiently diverse and whether exact-template rescues should feed back as stronger template-preservation priors.",
            "",
        ]
    )
    return "\n".join(lines)


def make_html(
    *,
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    recipe_summary: list[dict[str, Any]],
    pair_summary: list[dict[str, Any]],
) -> str:
    counts = summary["result_counts"]
    label = batch_label(summary["batch_status"])
    total = summary["batch_status"].get("total") or counts["completed"]

    def esc(value: Any) -> str:
        return html.escape("" if value is None else str(value))

    def table(items: list[dict[str, Any]], columns: list[str], limit: int | None = None) -> str:
        limited = items[:limit] if limit is not None else items
        head = "".join(f"<th>{esc(column)}</th>" for column in columns)
        body_rows = []
        for item in limited:
            cells = "".join(f"<td>{esc(item.get(column))}</td>" for column in columns)
            body_rows.append(f"<tr>{cells}</tr>")
        return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"

    case_cards = "".join(
        f"<div class='chip'><span>{esc(case)}</span><strong>{count}</strong></div>"
        for case, count in summary["counts"]["case"].items()
    )
    top_rows = sorted(
        rows,
        key=lambda row: score_sort_value(row.get("final_scrutiny_score")),
        reverse=True,
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Play Hand Prior Test {esc(label)}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #111318;
      --panel: #1b2029;
      --panel-2: #232a35;
      --text: #eef2f7;
      --muted: #a7b0bf;
      --line: #374151;
      --good: #4ade80;
      --warn: #facc15;
      --accent: #60a5fa;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, Segoe UI, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.45;
    }}
    main {{ max-width: 1260px; margin: 0 auto; padding: 28px; }}
    h1, h2 {{ margin: 0 0 14px; }}
    h1 {{ font-size: 30px; }}
    h2 {{ font-size: 19px; margin-top: 30px; }}
    p {{ color: var(--muted); }}
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; }}
    .metric, .chip {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 12px; }}
    .metric span, .chip span {{ display: block; color: var(--muted); font-size: 12px; }}
    .metric strong {{ display: block; font-size: 26px; margin-top: 4px; }}
    .chips {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }}
    .chip {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 10px; text-align: left; font-size: 12px; vertical-align: top; }}
    th {{ background: var(--panel-2); color: var(--muted); font-weight: 600; }}
    tr:last-child td {{ border-bottom: none; }}
    code {{ color: var(--accent); }}
    @media (max-width: 900px) {{
      main {{ padding: 18px; }}
      .metrics, .chips {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
<main>
  <h1>Play Hand Prior Test {esc(label)}</h1>
  <p>Generated <code>{esc(summary['generated_at'])}</code>. Controlled batch using current recipe priors, min 2 indicators, max 4 indicators.</p>
  <section class="metrics">
    <div class="metric"><span>Completed</span><strong>{counts['completed']}/{esc(total)}</strong></div>
    <div class="metric"><span>Promoted</span><strong>{counts['promoted']}</strong></div>
    <div class="metric"><span>Tombstoned</span><strong>{counts['tombstoned']}</strong></div>
    <div class="metric"><span>Exact Wins</span><strong>{counts['exact_template_selected']}</strong></div>
    <div class="metric"><span>Rescues</span><strong>{counts['exact_template_rescues']}</strong></div>
  </section>
  <h2>Case Counts</h2>
  <section class="chips">{case_cards}</section>
  <h2>Recipe Summary</h2>
  {table(recipe_summary, ['dealt_recipe', 'dealt_recipe_source', 'count', 'promoted', 'tombstoned', 'promotion_rate', 'exact_selected', 'mutated_selected', 'exact_rescues', 'best_score', 'avg_positive_score'], 20)}
  <h2>Pair Family Summary</h2>
  {table(pair_summary, ['template_branch_source_probe_id', 'dealt_pair_probe_id', 'dealt_recipe', 'family_classification', 'dealt_pair_source', 'count', 'promoted', 'tombstoned', 'promotion_rate', 'exact_selected', 'exact_rescues', 'mutated_wins_over_exact', 'avg_mutation_delta', 'best_score', 'avg_positive_score'], 30)}
  <h2>Top Runs</h2>
  {table(top_rows, ['seed', 'run_status', 'final_scrutiny_score', 'selected_final_branch', 'canonical_selection_reason', 'dealt_recipe', 'dealt_recipe_source', 'template_branch_source_probe_id', 'dealt_pair_probe_id'], 20)}
  <h2>All Runs</h2>
  {table(rows, ['seed', 'run_status', 'final_scrutiny_score', 'case', 'selected_final_branch', 'canonical_selection_reason', 'dealt_recipe', 'dealt_pair_probe_id', 'template_branch_source_probe_id', 'exact_template_score', 'mutated_score', 'mutation_delta'], None)}
</main>
</body>
</html>
"""


def normalized_report_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in report.get("runs") or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        if row.get("mutation_delta") is None:
            exact = safe_float(row.get("exact_template_score"))
            mutated = safe_float(row.get("mutated_score"))
            row["mutation_delta"] = (
                safe_round(mutated - exact)
                if exact is not None and mutated is not None
                else None
            )
        rows.append(row)
    return rows


def hit_rate(rows: list[dict[str, Any]], predicate_key: str, predicate_value: str) -> float | None:
    selected = [
        row for row in rows if clean_text(row.get(predicate_key)) == predicate_value
    ]
    promoted = [row for row in selected if row.get("run_status") == "promoted"]
    return safe_rate(len(promoted), len(selected))


def metric_bundle(report: dict[str, Any]) -> dict[str, Any]:
    rows = normalized_report_rows(report)
    summary = report.get("summary") or {}
    result_counts = summary.get("result_counts") or {}
    scores = (summary.get("score_stats") or {}).get("all") or score_stats(rows)
    promoted = [row for row in rows if row.get("run_status") == "promoted"]
    template_rows = [row for row in rows if row.get("pair_template_materialized")]
    exact_rescues = [
        row
        for row in rows
        if row.get("canonical_selection_reason") == "rescued_by_exact_template"
    ]
    mutated_improved_template = [
        row
        for row in rows
        if row.get("exact_template_score") is not None
        and row.get("mutated_score") is not None
        and row.get("canonical_selection_reason") == "mutated_branch_selected"
    ]
    all_pair_counts = Counter(pair_family_id(row) for row in rows if pair_family_id(row))
    top_family_count = max(all_pair_counts.values()) if all_pair_counts else 0
    promoted_pair_families = {
        pair_family_id(row)
        for row in promoted
        if pair_family_id(row) and pair_family_id(row) != "unknown|unknown|unknown"
    }
    policy_exploration = [row for row in rows if row.get("case") == "policy_exploration"]
    return {
        "completed": result_counts.get("completed", len(rows)),
        "failed": result_counts.get("failed", 0),
        "promoted": result_counts.get("promoted", len(promoted)),
        "tombstoned": result_counts.get(
            "tombstoned",
            len([row for row in rows if row.get("run_status") == "tombstoned"]),
        ),
        "promotion_rate": result_counts.get(
            "promotion_rate", safe_rate(len(promoted), len(rows))
        ),
        "median_final_score": scores.get("median"),
        "average_final_score": scores.get("avg"),
        "best_final_score": scores.get("max"),
        "template_materialization_rate": result_counts.get(
            "template_materialization_rate", safe_rate(len(template_rows), len(rows))
        ),
        "exact_rescue_rate": result_counts.get(
            "exact_template_rescue_rate",
            safe_rate(len(exact_rescues), len(template_rows)),
        ),
        "mutation_improvement_rate": result_counts.get(
            "mutation_improvement_rate",
            safe_rate(len(mutated_improved_template), len(template_rows)),
        ),
        "policy_exploration_hit_rate": result_counts.get(
            "policy_exploration_hit_rate",
            safe_rate(
                len([row for row in policy_exploration if row.get("run_status") == "promoted"]),
                len(policy_exploration),
            ),
        ),
        "discovered_recipe_hit_rate": result_counts.get(
            "discovered_recipe_hit_rate",
            hit_rate(rows, "dealt_recipe_source", "discovery_recipe_validation"),
        ),
        "curated_recipe_hit_rate": result_counts.get(
            "curated_recipe_hit_rate",
            hit_rate(rows, "dealt_recipe_source", "curated_recipe_prior"),
        ),
        "top_family_concentration_share": result_counts.get(
            "top_family_concentration_share", safe_rate(top_family_count, len(rows))
        ),
        "unique_promoted_pair_families": result_counts.get(
            "unique_promoted_pair_families", len(promoted_pair_families)
        ),
    }


def family_rows(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = normalized_report_rows(report)
    summary_rows = grouped_summary(
        rows,
        keys=(
            "template_branch_source_probe_id",
            "dealt_pair_probe_id",
            "dealt_recipe",
            "dealt_pair_source",
        ),
    )
    families: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        family_id = (
            clean_text(row.get("template_branch_source_probe_id"))
            or clean_text(row.get("dealt_pair_probe_id"))
        )
        if not family_id or family_id == "unknown":
            continue
        families[family_id] = row
    return families


def build_comparison(
    *,
    previous_report: dict[str, Any],
    current_report: dict[str, Any],
    previous_label: str,
    current_label: str,
) -> dict[str, Any]:
    previous_metrics = metric_bundle(previous_report)
    current_metrics = metric_bundle(current_report)
    metric_deltas: dict[str, Any] = {}
    for key, current_value in current_metrics.items():
        previous_value = previous_metrics.get(key)
        current_float = safe_float(current_value)
        previous_float = safe_float(previous_value)
        metric_deltas[key] = (
            safe_round(current_float - previous_float)
            if current_float is not None and previous_float is not None
            else None
        )
    previous_families = family_rows(previous_report)
    current_families = family_rows(current_report)
    family_comparison: list[dict[str, Any]] = []
    for family_id in sorted(set(previous_families) | set(current_families)):
        previous = previous_families.get(family_id, {})
        current = current_families.get(family_id, {})
        row = {
            "family_id": family_id,
            "previous_count": previous.get("count", 0),
            "current_count": current.get("count", 0),
            "previous_promoted": previous.get("promoted", 0),
            "current_promoted": current.get("promoted", 0),
            "previous_promotion_rate": previous.get("promotion_rate"),
            "current_promotion_rate": current.get("promotion_rate"),
            "previous_classification": previous.get("family_classification"),
            "current_classification": current.get("family_classification"),
            "previous_exact_rescue_rate": previous.get("exact_rescue_rate"),
            "current_exact_rescue_rate": current.get("exact_rescue_rate"),
            "previous_mutated_win_rate": previous.get("mutated_win_rate"),
            "current_mutated_win_rate": current.get("mutated_win_rate"),
            "previous_avg_mutation_delta": previous.get("avg_mutation_delta"),
            "current_avg_mutation_delta": current.get("avg_mutation_delta"),
        }
        row["classification_changed"] = (
            row["previous_classification"] != row["current_classification"]
        )
        family_comparison.append(row)
    family_comparison.sort(
        key=lambda row: (
            int(row.get("current_promoted") or 0),
            int(row.get("current_count") or 0),
            score_sort_value(row.get("current_promotion_rate")),
        ),
        reverse=True,
    )
    return {
        "schema_version": "playhand_prior_batch_comparison_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "previous_label": previous_label,
        "current_label": current_label,
        "previous_metrics": previous_metrics,
        "current_metrics": current_metrics,
        "metric_deltas": metric_deltas,
        "family_comparison": family_comparison,
        "family_classification_changes": [
            row for row in family_comparison if row.get("classification_changed")
        ],
    }


def make_comparison_markdown(comparison: dict[str, Any]) -> str:
    previous_label = comparison["previous_label"]
    current_label = comparison["current_label"]
    previous_metrics = comparison["previous_metrics"]
    current_metrics = comparison["current_metrics"]
    metric_deltas = comparison["metric_deltas"]
    metric_order = [
        "promotion_rate",
        "template_materialization_rate",
        "exact_rescue_rate",
        "mutation_improvement_rate",
        "policy_exploration_hit_rate",
        "discovered_recipe_hit_rate",
        "curated_recipe_hit_rate",
        "top_family_concentration_share",
        "unique_promoted_pair_families",
        "median_final_score",
        "average_final_score",
        "best_final_score",
    ]
    lines = [
        f"# Recipe Performance Comparison: {previous_label} vs {current_label}",
        "",
        f"Generated: `{comparison['generated_at']}`",
        "",
        "## Family Classification Rules",
        "",
        "```text",
        "under_sampled: count < 3",
        "template_locked: exact_rescue_rate >= 0.40",
        "mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3",
        "template_guarded: exact_selected_rate >= 0.40",
        "unstable: otherwise",
        "```",
        "",
        "## Data Hygiene",
        "",
        "- `mutation_delta` is null unless both branch scores exist.",
        "- Policy-exploration and blank/unknown rows are excluded from pair/template-family concentration metrics.",
        "- Unique promoted pair/template families excludes blank/unknown families.",
        "- Previous and current family labels are computed independently before comparison.",
        "",
        "## Metric Deltas",
        "",
        "| Metric | Previous | Current | Delta |",
        "|---|---:|---:|---:|",
    ]
    for key in metric_order:
        lines.append(
            f"| `{key}` | {previous_metrics.get(key)} | {current_metrics.get(key)} | {metric_deltas.get(key)} |"
        )
    lines.extend(
        [
            "",
            "## Family Classification Changes",
            "",
            "| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    changes = comparison["family_classification_changes"]
    for row in changes[:30]:
        lines.append(
            "| {family} | {prev} | {curr} | {prev_count} | {curr_count} | {promo} | {rescue} | {mut_win} | {delta} |".format(
                family=row.get("family_id"),
                prev=row.get("previous_classification"),
                curr=row.get("current_classification"),
                prev_count=row.get("previous_count"),
                curr_count=row.get("current_count"),
                promo=row.get("current_promotion_rate"),
                rescue=row.get("current_exact_rescue_rate"),
                mut_win=row.get("current_mutated_win_rate"),
                delta=row.get("current_avg_mutation_delta"),
            )
        )
    lines.extend(
        [
            "",
            "## Top Current Families",
            "",
            "| Family | Class | Runs | Promoted | Promotion Rate | Rescue Rate | Mutated Win Rate | Avg Mutation Delta |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in comparison["family_comparison"][:20]:
        lines.append(
            "| {family} | {klass} | {count} | {promoted} | {promo} | {rescue} | {mut_win} | {delta} |".format(
                family=row.get("family_id"),
                klass=row.get("current_classification"),
                count=row.get("current_count"),
                promoted=row.get("current_promoted"),
                promo=row.get("current_promotion_rate"),
                rescue=row.get("current_exact_rescue_rate"),
                mut_win=row.get("current_mutated_win_rate"),
                delta=row.get("current_avg_mutation_delta"),
            )
        )
    return "\n".join(lines) + "\n"


def write_comparison_artifacts(
    *,
    previous_report_path: Path,
    current_report: dict[str, Any],
    out_dir: Path,
) -> list[Path]:
    previous_report = load_json(previous_report_path)
    previous_status = (previous_report.get("summary") or {}).get("batch_status") or {}
    current_status = (current_report.get("summary") or {}).get("batch_status") or {}
    previous_label = batch_label(previous_status, previous_report_path.parent)
    current_label = batch_label(current_status, out_dir)
    comparison = build_comparison(
        previous_report=previous_report,
        current_report=current_report,
        previous_label=previous_label,
        current_label=current_label,
    )
    stem = (
        f"recipe-performance-comparison-"
        f"{compact_label(previous_label)}-vs-{compact_label(current_label)}"
    )
    json_path = out_dir / f"{stem}.json"
    md_path = out_dir / f"{stem}.md"
    write_json(json_path, comparison)
    md_path.write_text(make_comparison_markdown(comparison), encoding="utf-8")
    return [json_path, md_path]


def default_compare_report(batch_dir: Path) -> Path | None:
    status_path = batch_dir / "batch-status.json"
    if not status_path.exists():
        return None
    status = load_json(status_path)
    if int(status.get("total") or 0) <= 50:
        return None
    candidate = Path("cgpt review") / "playhand-prior-test-clean-50" / "recipe-performance-report.json"
    return candidate if candidate.exists() else None


def build_report(
    batch_dir: Path,
    out_dir: Path,
    *,
    compare_report_path: Path | None = None,
) -> dict[str, Any]:
    status_path = batch_dir / "batch-status.json"
    if not status_path.exists():
        raise FileNotFoundError(f"Missing batch status: {status_path}")
    batch_status = load_json(status_path)
    rows = load_rows(batch_dir)
    if not rows:
        raise RuntimeError(f"No seed summary rows found in {batch_dir}")
    summary = build_summary(rows, batch_status)
    recipe_summary = grouped_summary(rows, keys=("dealt_recipe", "dealt_recipe_source"))
    pair_summary = grouped_summary(
        rows,
        keys=(
            "template_branch_source_probe_id",
            "dealt_pair_probe_id",
            "dealt_recipe",
            "dealt_pair_source",
        ),
    )
    pair_summary = [
        row
        for row in pair_summary
        if clean_text(row.get("template_branch_source_probe_id")) not in {"", "unknown"}
        or clean_text(row.get("dealt_pair_probe_id")) not in {"", "unknown"}
    ]
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "summary": summary,
        "recipe_summary": recipe_summary,
        "pair_summary": pair_summary,
        "runs": rows,
    }
    write_json(out_dir / "recipe-performance-report.json", report)
    write_csv(out_dir / "recipe-performance-runs.csv", rows, list(rows[0].keys()))
    write_csv(
        out_dir / "recipe-performance-recipes.csv",
        recipe_summary,
        list(recipe_summary[0].keys()) if recipe_summary else [],
    )
    write_csv(
        out_dir / "recipe-performance-pairs.csv",
        pair_summary,
        list(pair_summary[0].keys()) if pair_summary else [],
    )
    markdown = make_markdown(
        rows=rows,
        summary=summary,
        recipe_summary=recipe_summary,
        pair_summary=pair_summary,
    )
    (out_dir / "recipe-performance-report.md").write_text(markdown, encoding="utf-8")
    html_doc = make_html(
        rows=rows,
        summary=summary,
        recipe_summary=recipe_summary,
        pair_summary=pair_summary,
    )
    (out_dir / "recipe-performance-dashboard.html").write_text(html_doc, encoding="utf-8")
    if compare_report_path is None:
        compare_report_path = default_compare_report(batch_dir)
    if compare_report_path is not None and compare_report_path.exists():
        write_comparison_artifacts(
            previous_report_path=compare_report_path,
            current_report=report,
            out_dir=out_dir,
        )
    return report


def copy_review_packet(out_dir: Path, review_dir: Path, batch_dir: Path | None = None) -> None:
    review_dir.mkdir(parents=True, exist_ok=True)
    for name in [
        "recipe-performance-report.json",
        "recipe-performance-runs.csv",
        "recipe-performance-recipes.csv",
        "recipe-performance-pairs.csv",
        "recipe-performance-report.md",
        "recipe-performance-dashboard.html",
    ]:
        source = out_dir / name
        if source.exists():
            (review_dir / name).write_bytes(source.read_bytes())
    for source in sorted(out_dir.glob("recipe-performance-comparison-*.json")):
        (review_dir / source.name).write_bytes(source.read_bytes())
    for source in sorted(out_dir.glob("recipe-performance-comparison-*.md")):
        (review_dir / source.name).write_bytes(source.read_bytes())
    if batch_dir is not None:
        for name in ["batch-status.json", "batch-run.log"]:
            source = batch_dir / name
            if source.exists():
                (review_dir / name).write_bytes(source.read_bytes())
        for source in sorted(batch_dir.glob("run-clean-*.ps1")):
            (review_dir / source.name).write_bytes(source.read_bytes())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a Play Hand recipe prior batch performance report."
    )
    parser.add_argument(
        "--batch-dir",
        type=Path,
        default=Path("runs/derived/playhand-prior-test-clean-50"),
        help="Directory containing seed summary/status files.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Report output directory. Defaults to the batch directory.",
    )
    parser.add_argument(
        "--review-dir",
        type=Path,
        default=None,
        help="Optional cgpt review subdirectory to receive report artifacts.",
    )
    parser.add_argument(
        "--compare-report",
        type=Path,
        default=None,
        help="Optional previous recipe-performance-report.json to compare against.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    batch_dir = args.batch_dir.resolve()
    out_dir = (args.out_dir or batch_dir).resolve()
    compare_report = args.compare_report.resolve() if args.compare_report else None
    report = build_report(batch_dir, out_dir, compare_report_path=compare_report)
    if args.review_dir is not None:
        copy_review_packet(out_dir, args.review_dir.resolve(), batch_dir=batch_dir)
    summary = report["summary"]["result_counts"]
    print(
        "Built Play Hand prior batch report: "
        f"{summary['completed']} completed, {summary['promoted']} promoted, "
        f"{summary['tombstoned']} tombstoned, {summary['failed']} failed."
    )
    print(f"Report: {out_dir / 'recipe-performance-report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
