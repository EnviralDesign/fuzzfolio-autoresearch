"""Bounded older-history holdout for matched Phase 3 survivors.

This is deliberately a script, not a new autoresearch runtime surface.  It
freezes a plan from the durable campaign evidence, then uses the established
full-backtest gateway seam for fixed-cell V2 outer tests.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any, Iterable

from autoresearch.config import load_config
from autoresearch.corpus_lab_backtests import (
    build_full_backtest_lab_task,
    resolve_lab_backtest_config,
    run_lab_full_backtests,
)
from autoresearch.corpus_tools import load_profile_snapshot
from autoresearch.evidence_plan import (
    build_execution_cell_sha256,
    build_replay_evidence_plan,
    canonical_sha256,
    normalize_evidence_profile_snapshot,
)
from autoresearch.lake_window import resolve_replay_lake_window_request
from autoresearch.lake_window_client import resolve_lake_window_binding
from autoresearch.nested_evidence import FrozenExecutionCellReceipt

DEFAULT_CAMPAIGN = "runs/derived/play-hand-lab-campaigns/phase3-darwin-rich-ab-v3/play-hand-lab-state.json"
DEFAULT_AUTHORITY = "runs/derived/phase3-authorities/phase3-darwin-rich-ab-v3/phase3-playhand-authority.json"
DEFAULT_OUTPUT = "runs/derived/phase3-survivor-holdout"
RESERVED_TAIL = ("2026-01-14T00:00:00Z", "2026-07-14T00:00:00Z")
DATA_CUTOFF = "2026-01-14T00:00:00Z"
WINDOWS = (
    ("h1", "2021-07-14T00:00:00Z", "2022-01-14T00:00:00Z", 6),
    ("h2", "2022-01-14T00:00:00Z", "2022-07-14T00:00:00Z", 6),
    ("h3", "2022-07-14T00:00:00Z", "2023-01-14T00:00:00Z", 6),
    ("aggregate", "2021-07-14T00:00:00Z", "2023-01-14T00:00:00Z", 18),
)
SCHEMA = "phase3-survivor-holdout-plan-v1"


class HoldoutError(RuntimeError):
    pass


class UnresolvedSource(HoldoutError):
    """A genuinely absent historical chain; safe to exclude but never hide."""

    pass


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HoldoutError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise HoldoutError(f"JSON object required: {path}")
    return value


def _sha(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        raise HoldoutError(f"regular source file required: {path}")
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")), encoding="utf-8")


def _require_disjoint_output(experiment_root: Path, protected_paths: Iterable[Path]) -> None:
    root = experiment_root.resolve()
    for raw_path in protected_paths:
        protected = raw_path.resolve()
        overlaps = root == protected or root in protected.parents
        if protected.is_dir():
            overlaps = overlaps or protected in root.parents
        if overlaps:
            raise HoldoutError(
                f"experiment output overlaps protected source evidence: {raw_path}"
            )


def _attr_value(value: Any) -> str:
    if isinstance(value, dict) and value.get("kind") == "value":
        return str(value.get("value") or "").strip().upper()
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _phase_score(lane: dict[str, Any], name: str) -> float | None:
    raw = (lane.get("phase_scores") or {}).get(name)
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _valid_window(start: str, end: str) -> None:
    if not start < end:
        raise HoldoutError("holdout window must be non-empty")
    tail_start, tail_end = RESERVED_TAIL
    if start < tail_end and end > tail_start:
        raise HoldoutError("reserved tail is prohibited from this experiment")
    if end > DATA_CUTOFF:
        raise HoldoutError("holdout window extends beyond data-availability cutoff")


def _validate_receipt(receipt: dict[str, Any], artifact_dir: Path, attempt_id: str) -> dict[str, Any]:
    unsigned = dict(receipt)
    supplied = str(unsigned.pop("receipt_sha256", ""))
    if supplied != canonical_sha256(unsigned):
        raise HoldoutError(f"task receipt hash drift: {artifact_dir}")
    recorded = receipt.get("recorded_result")
    if not isinstance(recorded, dict) or str(recorded.get("attempt_id") or "") != attempt_id:
        raise HoldoutError(f"task receipt attempt drift: {artifact_dir}")
    if str(recorded.get("phase") or "") != "final_36mo":
        raise HoldoutError(f"task receipt is not final_36mo: {artifact_dir}")
    if Path(str(recorded.get("artifact_dir") or "")).resolve() != artifact_dir.resolve():
        raise HoldoutError(f"task receipt artifact directory drift: {artifact_dir}")
    artifact = receipt.get("artifact_receipt")
    if not isinstance(artifact, dict) or Path(str(artifact.get("root") or "")).resolve() != artifact_dir.resolve():
        raise HoldoutError(f"task artifact receipt root drift: {artifact_dir}")
    files = artifact.get("files")
    if not isinstance(files, dict):
        raise HoldoutError(f"task artifact receipt files missing: {artifact_dir}")
    for name, expected_raw in files.items():
        relative = Path(str(name))
        candidate = (artifact_dir / relative).resolve()
        if relative.is_absolute() or artifact_dir.resolve() not in candidate.parents:
            raise HoldoutError(f"unsafe task artifact receipt path: {artifact_dir}")
        expected = str(expected_raw or "")
        observed = _sha(candidate)
        if expected != observed:
            raise HoldoutError(f"task artifact receipt hash drift for {name}: {artifact_dir}")
    for name in ("deep-replay-job.json", "sensitivity-response.json"):
        if name not in files:
            raise HoldoutError(f"task artifact receipt omits {name}: {artifact_dir}")
    return recorded


def _resolve_lane(lane: dict[str, Any], cohort: str) -> dict[str, Any]:
    attempt_id = str(lane.get("final_attempt_id") or "").strip()
    run_dir = Path(str(lane.get("run_dir") or "")).resolve()
    if not attempt_id or not run_dir.is_dir() or run_dir.is_symlink():
        raise UnresolvedSource("missing_run_directory_or_final_attempt")
    progress = run_dir / "progress-index.csv"
    row: dict[str, Any] = {}
    artifact_dir: Path | None = None
    if progress.is_file():
        rows = list(csv.DictReader(progress.read_text(encoding="utf-8").splitlines()))
        row = next((item for item in rows if str(item.get("attempt_id") or "") == attempt_id), {})
        if row:
            artifact_dir = Path(str(row.get("artifact_dir") or "")).resolve()
    if artifact_dir is None:
        candidates: list[Path] = []
        evals = run_dir / "evals"
        for candidate in sorted(evals.glob("eval_lab_final_36mo_*")) if evals.is_dir() else []:
            if not candidate.is_dir() or candidate.is_symlink() or not (candidate / "task-result-receipt.json").is_file():
                continue
            try:
                recorded = _json(candidate / "task-result-receipt.json").get("recorded_result")
            except HoldoutError:
                raise
            if isinstance(recorded, dict) and str(recorded.get("attempt_id") or "") == attempt_id and str(recorded.get("phase") or "") == "final_36mo":
                candidates.append(candidate.resolve())
        if not candidates:
            raise UnresolvedSource("missing_progress_final_row_and_final_artifact")
        if len(candidates) != 1:
            raise HoldoutError(f"ambiguous final artifact candidates: {attempt_id}")
        artifact_dir = candidates[0]
    if not artifact_dir.is_dir() or artifact_dir.is_symlink():
        raise UnresolvedSource("missing_final_artifact_directory")
    for name in ("task-result-receipt.json", "deep-replay-job.json", "sensitivity-response.json"):
        if not (artifact_dir / name).is_file():
            raise UnresolvedSource(f"missing_final_artifact:{name}")
    receipt_path = artifact_dir / "task-result-receipt.json"
    receipt = _json(receipt_path)
    recorded = _validate_receipt(receipt, artifact_dir, attempt_id)
    profile_path = Path(str(recorded.get("profile_path") or "")).resolve()
    profile = load_profile_snapshot(profile_path)
    if not isinstance(profile, dict):
        raise HoldoutError(f"worker-ready profile unavailable: {attempt_id}")
    profile = normalize_evidence_profile_snapshot(profile)
    job = _json(artifact_dir / "deep-replay-job.json")
    request = job.get("request") if isinstance(job.get("request"), dict) else {}
    timeframe = str(request.get("timeframe") or row.get("effective_timeframe") or lane.get("timeframe") or "").strip().upper()
    instruments = [str(value).strip().upper() for value in profile.get("instruments") or request.get("instruments") or [] if str(value).strip()]
    if not timeframe or not instruments:
        raise HoldoutError(f"source request lacks timeframe/instruments: {attempt_id}")
    sensitivity_path = artifact_dir / "sensitivity-response.json"
    sensitivity = _json(sensitivity_path)
    aggregate = ((sensitivity.get("data") or {}).get("aggregate") or {}) if isinstance(sensitivity.get("data"), dict) else {}
    matrix = aggregate.get("matrix_summary") if isinstance(aggregate, dict) else None
    matrix = matrix if isinstance(matrix, dict) else {}
    source_name = "robust_cell" if isinstance(matrix.get("robust_cell"), dict) else "recommended_cell"
    source_cell = matrix.get(source_name) if source_name == "robust_cell" else aggregate.get("recommended_cell")
    if not isinstance(source_cell, dict):
        raise HoldoutError(f"no robust/recommended cell: {attempt_id}")
    try:
        cell = {"reward_multiple": float(source_cell["reward_multiple"]), "stop_loss_percent": float(source_cell["stop_loss_percent"])}
        trades = int(source_cell["resolved_trades"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HoldoutError(f"invalid frozen cell: {attempt_id}") from exc
    attrs = ((lane.get("policy_assignment") or {}).get("candidate_attributes") or {})
    return {
        "cohort": cohort,
        "lane_index": int(lane.get("lane_index")), "lane_id": str(lane.get("lane_id") or ""),
        "attempt_id": attempt_id, "run_dir": str(run_dir), "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path), "profile_snapshot": profile,
        "profile_sha256": canonical_sha256(profile), "profile_file_sha256": _sha(profile_path),
        "receipt_sha256": _sha(receipt_path), "job_sha256": _sha(artifact_dir / "deep-replay-job.json"),
        "sensitivity_sha256": _sha(sensitivity_path), "cell": cell, "cell_sha256": build_execution_cell_sha256(cell),
        "cell_source": source_name, "resolved_trades": trades, "timeframe": timeframe, "instruments": sorted(set(instruments)),
        "canonical_pair_family": _attr_value(attrs.get("canonical_pair_family_id")),
        "original_instrument": _attr_value(attrs.get("instrument")),
        "final_score": _phase_score(lane, "final_36mo"), "validation_12mo_score": _phase_score(lane, "validation_12mo"),
        "policy_family": _attr_value(attrs.get("recipe_id")),
        "recipe_id": _attr_value(attrs.get("recipe_id")),
        "indicator_ids": sorted(_attr_value(value) for value in ((attrs.get("indicator_ids") or {}).get("values") or []) if isinstance(value, dict)),
    }


def _dedupe(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record["profile_sha256"]
        if key not in selected or (record["lane_index"], record["profile_sha256"]) < (selected[key]["lane_index"], key):
            selected[key] = record
    return sorted(selected.values(), key=lambda item: (item["lane_index"], item["profile_sha256"]))


def match_pairs(promoted: list[dict[str, Any]], controls: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    available = list(controls)
    pairs: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for survivor in sorted(promoted, key=lambda item: item["profile_sha256"]):
        exact = [control for control in available if control["recipe_id"] == survivor["recipe_id"] and control["indicator_ids"] == survivor["indicator_ids"]]
        same_recipe = [control for control in available if control["recipe_id"] == survivor["recipe_id"]]
        pool = exact or same_recipe or available
        if not pool:
            unmatched.append({"profile_sha256": survivor["profile_sha256"], "lane_index": survivor["lane_index"], "reason": "no_unused_control"})
            continue
        tier = "exact_recipe_and_indicators" if exact else ("same_recipe_indicator_overlap" if same_recipe else "global_indicator_overlap")
        def key(control: dict[str, Any]) -> tuple[int, int, float, float, int, str]:
            left, right = survivor.get("validation_12mo_score"), control.get("validation_12mo_score")
            delta = abs(left - right) if left is not None and right is not None else float("inf")
            overlap = len(set(survivor["indicator_ids"]) & set(control["indicator_ids"]))
            relative_trade = abs(control["resolved_trades"] - survivor["resolved_trades"]) / max(1, survivor["resolved_trades"])
            return (-overlap if not exact else 0, 0 if control["original_instrument"] == survivor["original_instrument"] else 1, delta, relative_trade, control["lane_index"], control["profile_sha256"])
        control = min(pool, key=key)
        available.remove(control)
        pairs.append({"pair_id": canonical_sha256({"promoted": survivor["profile_sha256"], "control": control["profile_sha256"]}), "match_tier": tier, "same_original_instrument": control["original_instrument"] == survivor["original_instrument"], "indicator_overlap": len(set(survivor["indicator_ids"]) & set(control["indicator_ids"])), "promoted": survivor, "control": control})
    return pairs, unmatched


def _authority_fields(authority: dict[str, Any]) -> dict[str, str]:
    contract = authority.get("bound_contract") if isinstance(authority.get("bound_contract"), dict) else {}
    lake = (authority.get("source_identities") or {}).get("lake_manifest_sha256")
    worker = contract.get("worker_contract_sha256")
    image = contract.get("operator_launch_worker_image")
    authority_id = authority.get("authority_id")
    if not all(isinstance(x, str) and x for x in (lake, worker, image, authority_id)):
        raise HoldoutError("authority lacks lake/worker/image identity")
    return {"authority_id": authority_id, "lake_manifest_sha256": lake, "worker_contract_sha256": worker, "worker_contract_schema": str(contract.get("worker_contract_id") or "replay-worker-contract-v1"), "worker_image": image}


def build_plan(*, campaign_path: Path, authority_path: Path, experiment_root: Path) -> dict[str, Any]:
    authority = _json(authority_path); fields = _authority_fields(authority)
    state = _json(campaign_path); lanes = state.get("lanes")
    if not isinstance(lanes, list):
        raise HoldoutError("campaign state lanes must be an array")
    promoted_lanes = [lane for lane in lanes if isinstance(lane, dict) and (str(lane.get("current_phase")) == "promoted" or str(lane.get("terminal_outcome_category")) == "promoted" or lane.get("run_promoted") is True)]
    control_lanes = [lane for lane in lanes if isinstance(lane, dict) and str((lane.get("phase_completed_at") or {}).get("final_36mo") or "") and "final_36mo_scrutiny_failed" in (lane.get("tombstone_reasons") or [])]
    unresolved: list[dict[str, Any]] = []
    def resolve_all(source_lanes: list[dict[str, Any]], cohort: str) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        for lane in source_lanes:
            try:
                resolved.append(_resolve_lane(lane, cohort))
            except UnresolvedSource as exc:
                unresolved.append({"cohort": cohort, "lane_index": lane.get("lane_index"), "lane_id": lane.get("lane_id"), "attempt_id": lane.get("final_attempt_id"), "terminal": "unresolved_source", "reason": str(exc)})
        return resolved
    promoted = _dedupe(resolve_all(promoted_lanes, "promoted"))
    controls = _dedupe(resolve_all(control_lanes, "control"))
    pairs, unmatched = match_pairs(promoted, controls)
    _require_disjoint_output(
        experiment_root,
        [
            campaign_path,
            authority_path,
            *(
                Path(record[key])
                for record in [*promoted, *controls]
                for key in ("run_dir", "artifact_dir", "profile_path")
            ),
        ],
    )
    requests = []
    for record in [*promoted, *controls]:
        for name, start, end, months in WINDOWS:
            _valid_window(start, end)
            request = resolve_replay_lake_window_request(pairs=record["instruments"], base_timeframe=record["timeframe"], profile_snapshot=record["profile_snapshot"], analysis_window_start=start, analysis_window_end=end)
            requests.append({"cohort": record["cohort"], "profile_sha256": record["profile_sha256"], "window": name, "request": request.canonical_payload(), "requested_horizon_months": months})
    semantic = {"schema_version": SCHEMA, "harness_source_sha256": _sha(Path(__file__).resolve()), "authority": fields, "source_files": {"campaign": {"path": str(campaign_path.resolve()), "sha256": _sha(campaign_path)}, "authority": {"path": str(authority_path.resolve()), "sha256": _sha(authority_path)}}, "reserved_tail": {"start": RESERVED_TAIL[0], "end": RESERVED_TAIL[1], "semantics": "[start,end)", "prohibited": True}, "data_availability_cutoff": DATA_CUTOFF, "windows": [{"name": n, "start": s, "end": e, "requested_horizon_months": m} for n,s,e,m in WINDOWS], "cohort_counts": {"promoted_lanes": len(promoted_lanes), "control_lanes": len(control_lanes), "promoted_unresolved": sum(x["cohort"] == "promoted" for x in unresolved), "controls_unresolved": sum(x["cohort"] == "control" for x in unresolved), "promoted_deduped": len(promoted), "controls_deduped": len(controls), "matched_pairs": len(pairs), "unmatched_promoted": len(unmatched), "primary_task_count": (len(promoted) + len(controls)) * len(WINDOWS)}, "promoted": promoted, "controls": controls, "pairs": pairs, "unmatched_promoted": unmatched, "unresolved_sources": unresolved, "lake_window_requests": sorted(requests, key=lambda item: (item["window"], item["profile_sha256"], item["cohort"]))}
    plan = {"plan_id": canonical_sha256(semantic), **semantic}
    target = experiment_root / "plan.json"
    if target.exists():
        expected_bytes = json.dumps(plan, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        existing = _json(target)
        if existing != plan or target.read_text(encoding="utf-8") != expected_bytes:
            raise HoldoutError("existing plan differs; refusing plan drift")
    else:
        _write_json(target, plan)
    return plan


def _revalidate_plan(plan: dict[str, Any], campaign_path: Path, authority_path: Path, experiment_root: Path) -> None:
    if str(plan.get("plan_id") or "") != canonical_sha256({key: value for key, value in plan.items() if key != "plan_id"}):
        raise HoldoutError("plan identity drift")
    fresh = build_plan(campaign_path=campaign_path, authority_path=authority_path, experiment_root=experiment_root)
    if fresh != plan:
        raise HoldoutError("plan/source revalidation drift")


def deterministic_task_id(plan_id: str, pair_id: str, cohort: str, window: str, *, canary_pairs: int | None = None) -> str:
    payload = {"plan_id": plan_id, "pair_id": pair_id, "cohort": cohort, "window": window, "canary_pairs": canary_pairs}
    return "phase3-survivor-holdout-" + canonical_sha256(payload).split(":", 1)[1][:32]


def _tracked_result(
    result_path: Path,
    curve_path: Path,
    expected_cell: dict[str, float],
) -> dict[str, Any]:
    payload = _json(result_path)
    if payload.get("outcome") == "no_valid_cell" or payload.get("status") == "nonviable":
        return {"status": "no_signal", "no_signal": True, "net_r": None, "resolved_trades": 0}
    tracked = (((payload.get("data") or {}).get("aggregate") or {}).get("tracked_cell_result")) if isinstance(payload.get("data"), dict) else None
    if not isinstance(tracked, dict):
        raise HoldoutError(f"redacted outer result lacks tracked_cell_result: {result_path}")
    detail = _json(curve_path)
    detail_cell = detail.get("cell")
    if not isinstance(detail_cell, dict):
        raise HoldoutError(f"tracked outer detail lacks its execution cell: {curve_path}")
    for key in ("stop_loss_percent", "reward_multiple"):
        if (
            tracked.get(key) is None
            or detail_cell.get(key) is None
            or abs(float(tracked[key]) - float(expected_cell[key])) > 1e-12
            or abs(float(detail_cell[key]) - float(expected_cell[key])) > 1e-12
        ):
            raise HoldoutError(f"tracked outer result differs from frozen cell: {result_path}")
    trades = next((tracked.get(k) for k in ("resolved_trades", "trade_count", "closed_trade_count") if tracked.get(k) is not None), None)
    resolved_trades = int(trades) if trades is not None else None
    path_metrics = detail.get("path_metrics")
    if resolved_trades == 0:
        net = 0.0
    elif not isinstance(path_metrics, dict) or path_metrics.get("final_equity_r") is None:
        raise HoldoutError(f"tracked outer detail lacks final_equity_r: {curve_path}")
    else:
        net = float(path_metrics["final_equity_r"])
    result = {
        "status": "calculated",
        "no_signal": resolved_trades == 0,
        "net_r": float(net),
        "resolved_trades": resolved_trades,
        "path_metrics": path_metrics,
    }
    return result


def calculate_verdict(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_profile: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for item in records: by_profile.setdefault((item["cohort"], item["profile_sha256"]), {})[item["window"]] = item["result"]
    outcomes = []
    for (cohort, profile), windows in sorted(by_profile.items()):
        aggregate = windows.get("aggregate", {}).get("net_r")
        six = [windows.get(name, {}).get("net_r") for name in ("h1", "h2", "h3")]
        passed = aggregate is not None and aggregate > 0 and sum(value is not None and value > 0 for value in six) >= 2
        outcomes.append({"cohort": cohort, "profile_sha256": profile, "aggregate_net_r": aggregate, "individual_pass": passed})
    promoted = [row for row in outcomes if row["cohort"] == "promoted"]; controls = [row for row in outcomes if row["cohort"] == "control"]
    def rate(rows: list[dict[str, Any]]) -> float: return sum(bool(row["individual_pass"]) for row in rows) / len(rows) if rows else 0.0
    p_agg = [float(row["aggregate_net_r"]) for row in promoted if row["aggregate_net_r"] is not None]; c_agg = [float(row["aggregate_net_r"]) for row in controls if row["aggregate_net_r"] is not None]
    paired = []
    for item in records:
        if item["window"] == "aggregate": paired.append(item)
    grouped: dict[str, dict[str, float | None]] = {}
    for item in paired:
        if item.get("pair_id"):
            grouped.setdefault(item["pair_id"], {})[item["cohort"]] = item["result"].get("net_r")
    evaluable = [row for row in grouped.values() if row.get("promoted") is not None and row.get("control") is not None]
    pair_rate = sum(row["promoted"] > row["control"] for row in evaluable) / len(evaluable) if evaluable else 0.0
    components = {"promoted_pass_count": sum(row["individual_pass"] for row in promoted), "promoted_pass_rate": rate(promoted), "control_pass_rate": rate(controls), "pass_rate_margin": rate(promoted)-rate(controls), "median_promoted_aggregate_net_r": statistics.median(p_agg) if p_agg else None, "median_control_aggregate_net_r": statistics.median(c_agg) if c_agg else None, "matched_pair_count": len(grouped), "evaluable_pair_count": len(evaluable), "promoted_pair_win_rate": pair_rate}
    supports = components["promoted_pass_count"] >= 5 and components["pass_rate_margin"] >= .10 and components["median_promoted_aggregate_net_r"] is not None and components["median_control_aggregate_net_r"] is not None and components["median_promoted_aggregate_net_r"] > components["median_control_aggregate_net_r"] and components["matched_pair_count"] >= 5 and components["evaluable_pair_count"] == components["matched_pair_count"] and pair_rate >= .55
    return {"verdict": "supports_later_tail_test" if supports else "no_support_for_tail", "components": components, "individual_outcomes": outcomes}


def run_plan(*, plan: dict[str, Any], campaign_path: Path, authority_path: Path, experiment_root: Path, gateway_url: str | None, gateway_token: str | None, max_workers: int, canary_pairs: int | None) -> dict[str, Any]:
    _revalidate_plan(plan, campaign_path, authority_path, experiment_root)
    authority = _authority_fields(_json(authority_path))
    config = load_config(repo_root=Path.cwd())
    lab = resolve_lab_backtest_config(gateway_url=gateway_url, gateway_token=gateway_token, trading_dashboard_root=Path("C:/repos/Trading-Dashboard"))
    if lab.worker_contract_hash != authority["worker_contract_sha256"] or lab.worker_contract_schema != authority["worker_contract_schema"]:
        raise HoldoutError("current lab worker contract differs from authority")
    pairs = list(plan["pairs"])
    if canary_pairs is not None:
        if canary_pairs < 1: raise HoldoutError("--canary-pairs must be positive")
        pairs = pairs[:canary_pairs]
    root = experiment_root / (f"canary-{canary_pairs}" if canary_pairs is not None else "execution")
    pair_by_profile = {
        (role, pair[role]["profile_sha256"]): pair["pair_id"]
        for pair in plan["pairs"]
        for role in ("promoted", "control")
    }
    if canary_pairs is None:
        profiles = [*plan["promoted"], *plan["controls"]]
    else:
        selected = {pair[role]["profile_sha256"] for pair in pairs for role in ("promoted", "control")}
        profiles = [record for record in [*plan["promoted"], *plan["controls"]] if record["profile_sha256"] in selected]
    request_index = {(row["profile_sha256"], row["cohort"], row["window"]): row["request"] for row in plan["lake_window_requests"]}
    collected: list[dict[str, Any]] = []
    for window, start, end, months in WINDOWS:
        _valid_window(start, end)
        items = []; receipts = {}; evidence = {}; task_ids = {}; prebuilt = {}; context: dict[str, tuple[str, str]] = {}
        for source in profiles:
                cohort = source["cohort"]; request = resolve_replay_lake_window_request(pairs=source["instruments"], base_timeframe=source["timeframe"], profile_snapshot=source["profile_snapshot"], analysis_window_start=start, analysis_window_end=end)
                if request.canonical_payload() != request_index[(source["profile_sha256"], cohort, window)]: raise HoldoutError("frozen LakeWindowRequest drift")
                binding = resolve_lake_window_binding(request, legacy_selection_manifest_sha256=authority["lake_manifest_sha256"])
                task_id = deterministic_task_id(plan["plan_id"], source["profile_sha256"], cohort, window, canary_pairs=canary_pairs)
                attempt_id = task_id
                artifact_dir = root / "attempts" / window / task_id
                artifact_dir.mkdir(parents=True, exist_ok=True)
                clone = {"attempt_id": attempt_id, "artifact_dir": str(artifact_dir), "profile_path": source["profile_path"], "profile_ref": f"holdout:{source['profile_sha256']}", "requested_timeframe": source["timeframe"], "_nested_source_artifact_dir": source["artifact_dir"]}
                source_receipt = {
                    "profile_sha256": source["profile_sha256"],
                    "profile_file_sha256": source["profile_file_sha256"],
                    "receipt_sha256": source["receipt_sha256"],
                    "job_sha256": source["job_sha256"],
                    "sensitivity_sha256": source["sensitivity_sha256"],
                    "execution_cell_sha256": source["cell_sha256"],
                }
                receipt = FrozenExecutionCellReceipt(campaign_plan_id=plan["plan_id"], fold_id=f"{source['profile_sha256']}:{cohort}:{window}", profile_snapshot_sha256=source["profile_sha256"], train_evidence_plan_id=canonical_sha256(source_receipt), selection_basis=source["cell_source"], execution_cell=source["cell"], execution_cell_sha256=source["cell_sha256"], source=source_receipt, lake_manifest_sha256=authority["lake_manifest_sha256"])
                outer = build_replay_evidence_plan(campaign_plan_id=plan["plan_id"], evidence_role="outer_test", selection_data_end=DATA_CUTOFF, analysis_window_start=start, analysis_window_end=end, requested_horizon_months=months, profile_snapshot=source["profile_snapshot"], execution_cell_sha256=source["cell_sha256"], lake_window_binding=binding, data_availability_cutoff=DATA_CUTOFF)
                task = build_full_backtest_lab_task(config=config, run_dir=root / "run", attempt=clone, run_metadata={}, lab_config=lab, evidence_plan=outer, profile_snapshot_override=source["profile_snapshot"], tracked_cell=source["cell"], task_id=task_id)
                items.append((root / "run", clone, dict(clone), {})); receipts[attempt_id] = receipt; evidence[attempt_id] = outer.model_dump(mode="json"); task_ids[attempt_id] = task_id; prebuilt[attempt_id] = task; context[attempt_id] = (source["profile_sha256"], cohort)
        results, _, failed = run_lab_full_backtests(config=config, items=items, lab_config=lab, max_workers=max_workers, requested_horizon_months=months, evidence_window_start=start, evidence_window_end=end, evidence_role="outer_test", selection_data_end=DATA_CUTOFF, campaign_plan_id=plan["plan_id"], cell_receipts_by_attempt_id=receipts, evidence_plans_by_attempt_id=evidence, task_ids_by_attempt_id=task_ids, prebuilt_tasks_by_attempt_id=prebuilt)
        if failed or len(results) != len(items): raise HoldoutError(f"infrastructure/materialization failure in {window}")
        for result in results:
            attempt_id = str(result.get("attempt_id") or ""); profile_sha256, cohort = context[attempt_id]
            if str(result.get("status")) == "failed": raise HoldoutError(f"gateway failure in {window}: {result.get('error')}")
            if str(result.get("status")) == "nonviable": observed = {"status": "no_signal", "no_signal": True, "net_r": None, "resolved_trades": 0}
            else:
                observed = _tracked_result(
                    Path(str(result.get("result_path") or "")),
                    Path(str(result.get("curve_path") or "")),
                    next(
                        source["cell"]
                        for source in profiles
                        if source["profile_sha256"] == profile_sha256
                        and source["cohort"] == cohort
                    ),
                )
            collected.append({"pair_id": pair_by_profile.get((cohort, profile_sha256)), "cohort": cohort, "profile_sha256": profile_sha256, "window": window, "plan_id": evidence[attempt_id]["plan_id"], "lake_window_binding": evidence[attempt_id]["lake_window_binding"], "task_id": task_ids[attempt_id], "result": observed})
    window_order = {name: index for index, (name, *_rest) in enumerate(WINDOWS)}
    collected.sort(key=lambda item: (window_order[item["window"]], item["cohort"], item["profile_sha256"]))
    report = {"schema_version": "phase3-survivor-holdout-execution-report-v1", "plan_id": plan["plan_id"], "execution_root": str(root), "canary_pairs": canary_pairs, "reserved_tail_used": False, "unresolved_sources": plan["unresolved_sources"], "pairing": plan["pairs"], "results": collected, **calculate_verdict(collected)}
    _write_json(root / "execution-report.json", report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run"):
        item = sub.add_parser(name); item.add_argument("--campaign", type=Path, default=Path(DEFAULT_CAMPAIGN)); item.add_argument("--authority", type=Path, default=Path(DEFAULT_AUTHORITY)); item.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT)); item.add_argument("--experiment-id", default="phase3-darwin-rich-ab-v3")
        if name == "run":
            item.add_argument("--gateway-url"); item.add_argument("--gateway-token"); item.add_argument("--max-workers", type=int, default=4); item.add_argument("--canary-pairs", type=int)
    args = parser.parse_args(argv); root = args.output / args.experiment_id
    try:
        plan = build_plan(campaign_path=args.campaign, authority_path=args.authority, experiment_root=root)
        if args.command == "plan": print(json.dumps({"plan_id": plan["plan_id"], "path": str(root / "plan.json")}, sort_keys=True)); return 0
        report = run_plan(plan=plan, campaign_path=args.campaign, authority_path=args.authority, experiment_root=root, gateway_url=args.gateway_url, gateway_token=args.gateway_token, max_workers=args.max_workers, canary_pairs=args.canary_pairs)
        print(json.dumps({"verdict": report["verdict"], "report": str(Path(report["execution_root"]) / "execution-report.json")}, sort_keys=True)); return 0
    except HoldoutError as exc:
        parser.error(str(exc)); return 2


if __name__ == "__main__":
    raise SystemExit(main())
