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
            "exact_template_score": safe_float(summary.get("exact_template_score")),
            "exact_template_passed": exact_passed,
            "mutated_attempt_id": clean_text(summary.get("mutated_attempt_id")),
            "mutated_score": safe_float(summary.get("mutated_score")),
            "mutated_passed": mutated_passed,
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
            "exact_template_selected": len(exact_selected),
            "mutated_selected": len(mutated_selected),
            "exact_template_rescues": len(exact_rescues),
            "exact_template_outscored_mutated": len(exact_outscored),
            "mutated_improved_over_exact_template": len(mutated_improved_template),
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
    score = summary["score_stats"]
    lines = [
        "# Play Hand Prior Test Clean 50 Report",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "## Batch Result",
        "",
        f"- Runs completed: {counts['completed']}/{summary['batch_status'].get('total')} with {counts['failed']} failures.",
        f"- Promotions: {counts['promoted']} promoted, {counts['tombstoned']} tombstoned ({counts['promotion_rate']:.0%} promotion rate).",
        f"- Final score: median {score['all'].get('median')}, average {score['all'].get('avg')}, best {score['all'].get('max')}.",
        f"- Template materialization: {counts['template_materialized']} exact-template branches, {counts['template_not_materialized']} template-not-materialized rows.",
        f"- Branch selection: {counts['mutated_selected']} mutated, {counts['exact_template_selected']} exact-template.",
        f"- Exact-template impact: {counts['exact_template_rescues']} rescues, {counts['exact_template_outscored_mutated']} exact-template outscored mutated, {counts['mutated_improved_over_exact_template']} mutated improved over an exact template.",
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
    lines.append("| Probe | Recipe | Pair Source | Runs | Promoted | Exact Selected | Rescues | Best |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for row in pair_summary[:15]:
        lines.append(
            "| {probe} | {recipe} | {source} | {count} | {promoted} | {exact} | {rescues} | {best} |".format(
                probe=row.get("template_branch_source_probe_id")
                or row.get("dealt_pair_probe_id"),
                recipe=row.get("dealt_recipe"),
                source=row.get("dealt_pair_source"),
                count=row.get("count"),
                promoted=row.get("promoted"),
                exact=row.get("exact_selected"),
                rescues=row.get("exact_rescues"),
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
    lines.extend(
        [
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
  <title>Play Hand Prior Test Clean 50</title>
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
  <h1>Play Hand Prior Test Clean 50</h1>
  <p>Generated <code>{esc(summary['generated_at'])}</code>. Controlled batch using current recipe priors, min 2 indicators, max 4 indicators.</p>
  <section class="metrics">
    <div class="metric"><span>Completed</span><strong>{counts['completed']}/50</strong></div>
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
  {table(pair_summary, ['template_branch_source_probe_id', 'dealt_pair_probe_id', 'dealt_recipe', 'dealt_pair_source', 'count', 'promoted', 'tombstoned', 'promotion_rate', 'exact_selected', 'exact_rescues', 'best_score', 'avg_positive_score'], 30)}
  <h2>Top Runs</h2>
  {table(top_rows, ['seed', 'run_status', 'final_scrutiny_score', 'selected_final_branch', 'canonical_selection_reason', 'dealt_recipe', 'dealt_recipe_source', 'template_branch_source_probe_id', 'dealt_pair_probe_id'], 20)}
  <h2>All Runs</h2>
  {table(rows, ['seed', 'run_status', 'final_scrutiny_score', 'case', 'selected_final_branch', 'canonical_selection_reason', 'dealt_recipe', 'dealt_pair_probe_id', 'template_branch_source_probe_id', 'exact_template_score', 'mutated_score'], None)}
</main>
</body>
</html>
"""


def build_report(batch_dir: Path, out_dir: Path) -> dict[str, Any]:
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
    return report


def copy_review_packet(out_dir: Path, review_dir: Path) -> None:
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    batch_dir = args.batch_dir.resolve()
    out_dir = (args.out_dir or batch_dir).resolve()
    report = build_report(batch_dir, out_dir)
    if args.review_dir is not None:
        copy_review_packet(out_dir, args.review_dir.resolve())
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
