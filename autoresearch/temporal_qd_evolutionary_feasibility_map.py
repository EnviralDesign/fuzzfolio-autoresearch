"""Offline feasibility map for retained Temporal-QD candidate evidence.

This module deliberately reads completed reduced outcomes only.  It does not
launch a worker, replay market data, change an evolutionary policy, or alter a
run root.  Its purpose is descriptive: make the already-observed relationship
between activity, holding duration, gross result, modeled cost, and after-cost
result auditable before choosing the next experiment.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import re
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "temporal_qd_evolutionary_feasibility_map_v1"
WINDOW_RESAMPLE_REPLICATES = 256
FLOAT_TOLERANCE = 1e-9


class FeasibilityMapError(RuntimeError):
    """Raised when retained evidence cannot be normalized faithfully."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FeasibilityMapError(f"could not read JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise FeasibilityMapError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise FeasibilityMapError(f"could not read JSONL: {path}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FeasibilityMapError(f"invalid JSONL at {path}:{line_number}") from exc
        if not isinstance(row, Mapping):
            raise FeasibilityMapError(f"object row required at {path}:{line_number}")
        rows.append(row)
    return rows


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FeasibilityMapError(f"{name} must be an object")
    return value


def _list_of_mappings(value: object, *, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise FeasibilityMapError(f"{name} must be a list of objects")
    return list(value)


def _text(value: object, *, name: str, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise FeasibilityMapError(f"{name} is required")
        return None
    if not isinstance(value, str) or not value:
        raise FeasibilityMapError(f"{name} must be a nonempty string")
    return value


def _number(value: object, *, name: str, required: bool = False) -> float | None:
    if value is None:
        if required:
            raise FeasibilityMapError(f"{name} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FeasibilityMapError(f"{name} must be numeric")
    output = float(value)
    if not math.isfinite(output):
        raise FeasibilityMapError(f"{name} must be finite")
    return output


def _int(value: object, *, name: str, required: bool = False) -> int | None:
    number = _number(value, name=name, required=required)
    if number is None:
        return None
    if not number.is_integer():
        raise FeasibilityMapError(f"{name} must be an integer")
    return int(number)


def _median(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


def _mean(values: Sequence[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _safe_divide(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _file_descriptor(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "relativePath": _relative(path, root),
        "sizeBytes": path.stat().st_size,
        "rawSha256": _sha256_file(path),
    }


def _generation_from_path(path: Path) -> int | None:
    match = re.search(r"generation-(\d+)", path.as_posix())
    return int(match.group(1)) if match else None


def _source_mode(candidate: Mapping[str, Any]) -> str | None:
    return _text(candidate.get("sourceMode"), name="candidate.sourceMode")


def _operator_from_history(candidate: Mapping[str, Any]) -> str | None:
    history = candidate.get("structuralOperatorHistory")
    if not isinstance(history, list):
        return None
    for entry in reversed(history):
        if isinstance(entry, Mapping):
            operation = entry.get("operation")
            if isinstance(operation, str) and operation:
                return operation
    return None


def _management_action_share(action_counts: Mapping[str, Any]) -> tuple[int, float | None]:
    total = 0
    management = 0
    non_management = {"enter_next_open", "exit_next_open", "protective_close"}
    for name, raw_count in action_counts.items():
        count = _int(raw_count, name=f"action count {name}")
        assert count is not None
        total += count
        if name not in non_management:
            management += count
    return management, _safe_divide(float(management), float(total))


def _side_contributions(
    realized_behavior: Mapping[str, Any] | None,
) -> tuple[float | None, float | None]:
    if not isinstance(realized_behavior, Mapping):
        return None, None
    sides = realized_behavior.get("sides")
    if not isinstance(sides, Mapping):
        return None, None
    outputs: list[float | None] = []
    for side in ("long", "short"):
        value = sides.get(side)
        if not isinstance(value, Mapping):
            outputs.append(None)
            continue
        outputs.append(_number(value.get("netR"), name=f"{side} net R"))
    return outputs[0], outputs[1]


def _bundle_panel_map(
    bundle_paths: Iterable[Path],
) -> tuple[dict[tuple[str, str | None, str | None], tuple[str | None, Mapping[str, Any]]], list[dict[str, Any]]]:
    """Index reduced candidate/window bundles for independent row reconciliation."""

    index: dict[tuple[str, str | None, str | None], tuple[str | None, Mapping[str, Any]]] = {}
    descriptors: list[dict[str, Any]] = []
    for path in sorted(bundle_paths):
        rows = _read_jsonl(path)
        descriptors.append(_file_descriptor(path, root=path.parents[0]))
        for row in rows:
            candidate_id = _text(row.get("candidateId"), name="bundle candidateId", required=True)
            panel_id = _text(row.get("panelId"), name="bundle panelId")
            assert candidate_id is not None
            for window in _list_of_mappings(row.get("windowEvidence"), name="bundle windowEvidence"):
                _text(window.get("windowId"), name="bundle windowId", required=True)
                metrics = _mapping(window.get("metrics"), name="bundle metrics")
                analysis_start = _text(window.get("analysisWindowStart"), name="bundle analysis window start")
                analysis_end = _text(window.get("analysisWindowEnd"), name="bundle analysis window end")
                key = (candidate_id, analysis_start, analysis_end)
                existing = index.get(key)
                if existing is not None and _canonical_bytes(existing[1]) != _canonical_bytes(metrics):
                    raise FeasibilityMapError(
                        "conflicting reduced bundle metrics for "
                        f"candidate/window {candidate_id}/{analysis_start}/{analysis_end}"
                    )
                index[key] = (panel_id, metrics)
    return index, descriptors


def _discover_v37_member_files(root: Path) -> list[Path]:
    suffix = "/campaign/proposal-current-panel/campaign-output/evaluated-members.jsonl"
    files = [path for path in root.rglob("evaluated-members.jsonl") if path.as_posix().endswith(suffix)]
    if not files:
        raise FeasibilityMapError(f"no V37 final campaign evaluated-members files under {root}")
    return sorted(files)


def _discover_v37_bundle_files(member_files: Iterable[Path]) -> list[Path]:
    output: list[Path] = []
    for member_path in member_files:
        bundle_path = member_path.with_name("candidate-panel-bundles.jsonl")
        if not bundle_path.is_file():
            raise FeasibilityMapError(f"missing paired candidate-panel bundle: {bundle_path}")
        output.append(bundle_path)
    return sorted(output)


def _load_v38_compact_metrics(root: Path) -> dict[str, Mapping[str, Any]]:
    path = root / "score" / "compact-metrics.jsonl"
    if not path.is_file():
        return {}
    output: dict[str, Mapping[str, Any]] = {}
    for row in _read_jsonl(path):
        candidate_id = _text(row.get("candidateId"), name="V38 compact candidateId", required=True)
        assert candidate_id is not None
        if candidate_id in output:
            raise FeasibilityMapError(f"duplicate V38 compact metric for {candidate_id}")
        output[candidate_id] = row
    return output


def _classify_frequency(trades: int) -> str:
    if trades == 0:
        return "zero_no_trade"
    if trades <= 7:
        return "very_sparse_1_7"
    if trades <= 19:
        return "sparse_8_19"
    if trades <= 95:
        return "moderate_20_95"
    return "high_turnover_96_plus"


def _classify_holding(holding_bars: float | None) -> str:
    if holding_bars is None or holding_bars <= 0:
        return "unavailable_or_no_trade"
    if holding_bars <= 24:
        return "brief_1_24_bars"
    if holding_bars <= 96:
        return "short_25_96_bars"
    if holding_bars <= 480:
        return "medium_97_480_bars"
    return "long_481_plus_bars"


def _adaptive_bands(values: Sequence[float], *, prefix: str) -> tuple[list[float], callable[[float], str]]:
    nonzero = sorted(value for value in values if value > 0)
    if len(nonzero) < 4:
        thresholds = list(nonzero)
    else:
        thresholds = list(statistics.quantiles(nonzero, n=4, method="inclusive"))

    def classify(value: float) -> str:
        if value <= 0:
            return f"{prefix}_zero"
        for index, threshold in enumerate(thresholds, start=1):
            if value <= threshold:
                return f"{prefix}_q{index}"
        return f"{prefix}_q{len(thresholds) + 1}"

    return thresholds, classify


def _deterministic_seed(key: str) -> int:
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big")


def _window_resample_positive_rate(window_nets: Sequence[float], *, key: str) -> float | None:
    if not window_nets:
        return None
    randomizer = random.Random(_deterministic_seed(key))
    positive = 0
    for _ in range(WINDOW_RESAMPLE_REPLICATES):
        sample_total = sum(randomizer.choice(window_nets) for _ in window_nets)
        if sample_total > 0:
            positive += 1
    return positive / WINDOW_RESAMPLE_REPLICATES


def _candidate_summary(
    *,
    evaluation_id: str,
    base: Mapping[str, Any],
    window_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    nets = [float(row["conservativeNetR"]) for row in window_rows]
    gross = [float(row["grossR"]) for row in window_rows]
    costs = [float(row["modeledCostR"]) for row in window_rows]
    trades = [int(row["closedTrades"]) for row in window_rows]
    leave_one_out = [sum(nets) - value for value in nets]
    total_trades = sum(trades)
    total_gross = sum(gross)
    total_cost = sum(costs)
    total_net = sum(nets)
    summary = dict(base)
    summary.update(
        {
            "evaluationId": evaluation_id,
            "windowCount": len(window_rows),
            "totalTrades": total_trades,
            "totalGrossR": total_gross,
            "totalModeledCostR": total_cost,
            "totalConservativeNetR": total_net,
            "grossExpectancyPerTrade": _safe_divide(total_gross, total_trades),
            "costPerTrade": _safe_divide(total_cost, total_trades),
            "netExpectancyPerTrade": _safe_divide(total_net, total_trades),
            "activeWindowFraction": _safe_divide(
                float(sum(trade_count > 0 for trade_count in trades)), float(len(trades))
            ),
            "positiveWindowFraction": _safe_divide(
                float(sum(value > 0 for value in nets)), float(len(nets))
            ),
            "medianWindowConservativeNetR": _median(nets),
            "worstWindowConservativeNetR": min(nets) if nets else None,
            "leaveOneWindowOutPositiveFraction": _safe_divide(
                float(sum(value > 0 for value in leave_one_out)), float(len(leave_one_out))
            ),
            "windowResamplePositiveRate": _window_resample_positive_rate(
                nets, key=evaluation_id
            ),
            "currentSupportLeaveOneOut": "not_evaluable_under_four_window_policy",
            "frequencyBand": _classify_frequency(total_trades),
            "holdingBand": _classify_holding(
                _number(base.get("medianHoldingBars"), name="summary median holding bars")
            ),
        }
    )
    return summary


def _group_summary(rows: Sequence[Mapping[str, Any]], *, keys: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[object, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(key) for key in keys)].append(row)
    output: list[dict[str, Any]] = []
    for group_key in sorted(groups, key=lambda value: tuple(str(item) for item in value)):
        members = groups[group_key]
        nets = [float(member["totalConservativeNetR"]) for member in members]
        gross = [float(member["totalGrossR"]) for member in members]
        costs = [float(member["totalModeledCostR"]) for member in members]
        trades = [int(member["totalTrades"]) for member in members]
        output.append(
            {
                **dict(zip(keys, group_key, strict=True)),
                "candidateEvaluationCount": len(members),
                "windowObservationCount": sum(int(member["windowCount"]) for member in members),
                "totalTrades": sum(trades),
                "meanGrossR": _mean(gross),
                "medianGrossR": _median(gross),
                "meanModeledCostR": _mean(costs),
                "medianModeledCostR": _median(costs),
                "meanConservativeNetR": _mean(nets),
                "medianConservativeNetR": _median(nets),
                "afterCostPositiveCount": sum(value > 0 for value in nets),
                "grossPositiveCount": sum(value > 0 for value in gross),
                "allWindowsAfterCostPositiveCount": sum(
                    member.get("positiveWindowFraction") == 1.0 for member in members
                ),
                "meanWindowResamplePositiveRate": _mean(
                    [
                        float(member["windowResamplePositiveRate"])
                        for member in members
                        if member.get("windowResamplePositiveRate") is not None
                    ]
                ),
            }
        )
    return output


def _markdown_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[tuple[str, str]]) -> str:
    if not rows:
        return "_No rows._\n"
    headers = [label for _, label in columns]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        values: list[str] = []
        for key, _ in columns:
            value = row.get(key)
            if isinstance(value, float):
                values.append(f"{value:.4f}")
            elif value is None:
                values.append("—")
            else:
                values.append(str(value).replace("|", "\\|"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(_canonical_bytes(value) + b"\n")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("wb") as handle:
        for row in rows:
            handle.write(_canonical_bytes(row) + b"\n")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _parse_labeled_path(value: str, *, option_name: str) -> tuple[str, Path]:
    label, separator, raw_path = value.partition("=")
    if not separator or not label or not raw_path:
        raise FeasibilityMapError(f"{option_name} must use LABEL=PATH")
    path = Path(raw_path).resolve()
    if not path.is_dir():
        raise FeasibilityMapError(f"{option_name} path is not a directory: {path}")
    return label, path


def _source_authorities(root: Path, *, v38: bool) -> list[dict[str, Any]]:
    candidates: list[Path] = []
    if v38:
        candidates.extend(
            path
            for path in (root / "score" / "summary.json", root / "score" / "qualification.json")
            if path.is_file()
        )
    else:
        candidates.extend(sorted(root.rglob("native-finalization-authority.json")))
        candidates.extend(sorted(root.rglob("config.json"))[:1])
        candidates.extend(sorted(root.rglob("state.json"))[:1])
    return [_file_descriptor(path, root=root) for path in candidates]


def _load_cohort(label: str, root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Load one protocol stratum without mixing it with another campaign."""

    v38_score_path = root / "score" / "evaluated-members.jsonl"
    is_v38 = v38_score_path.is_file()
    if is_v38:
        member_paths = [v38_score_path]
        compact_metrics = _load_v38_compact_metrics(root)
        protocol_group = "temporal_qd_v5_fast_ephemeral_operator_matrix_v38"
        role = "frozen_parent_operator_matrix"
        bundle_paths = [
            path
            for path in root.rglob("candidate-panel-bundles.jsonl")
            if path.as_posix().endswith(
                "/campaign/proposal-current-panel/campaign-output/candidate-panel-bundles.jsonl"
            )
        ]
    else:
        member_paths = _discover_v37_member_files(root)
        compact_metrics = {}
        protocol_group = "temporal_qd_v5_fast_ephemeral_current_panel_v37"
        role = "five_generation_current_panel_population"
        bundle_paths = _discover_v37_bundle_files(member_paths)

    bundle_map, bundle_descriptors = _bundle_panel_map(bundle_paths)
    sources = [_file_descriptor(path, root=root) for path in member_paths]
    source_files = sources + bundle_descriptors + _source_authorities(root, v38=is_v38)
    windows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    aggregate_failures: list[str] = []
    bundle_checks = {"matched": 0, "missing": 0, "mismatched": 0}
    seen_evaluations: set[tuple[str, str]] = set()

    for member_path in member_paths:
        member_rows = _read_jsonl(member_path)
        for row_ordinal, row in enumerate(member_rows, start=1):
            row_hash = _sha256_bytes(_canonical_bytes(row))
            candidate = _mapping(row.get("candidate"), name="evaluated candidate")
            aggregate = _mapping(row.get("aggregate"), name="evaluated aggregate")
            finite = _mapping(row.get("finiteDataValidity"), name="finite data validity")
            descriptor = _mapping(row.get("descriptor"), name="descriptor")
            candidate_id = _text(row.get("candidateId"), name="evaluated candidateId", required=True)
            assert candidate_id is not None
            evaluation_key = (candidate_id, row_hash)
            if evaluation_key in seen_evaluations:
                continue
            seen_evaluations.add(evaluation_key)
            generation_index = _int(row.get("generationIndex"), name="generationIndex")
            generation_index = generation_index if generation_index is not None else _generation_from_path(member_path)
            compact = compact_metrics.get(candidate_id, {})
            if not isinstance(compact, Mapping):
                raise FeasibilityMapError("compact metrics index is malformed")
            operator_family = _text(compact.get("operatorFamily"), name="operator family") or _operator_from_history(candidate)
            parent_id = _text(compact.get("parentCandidateId"), name="parent candidate id")
            source_mode = _source_mode(candidate)
            support = finite.get("passesSupportGate")
            quality = finite.get("validForQuality")
            if not isinstance(support, bool) or not isinstance(quality, bool):
                raise FeasibilityMapError(f"support/quality flags malformed for {candidate_id}")
            window_records = _list_of_mappings(
                aggregate.get("windowRecords"), name=f"windowRecords for {candidate_id}"
            )
            if not window_records:
                raise FeasibilityMapError(f"no window records for {candidate_id}")
            evaluation_id = (
                f"{label}:{_relative(member_path, root)}:{row_ordinal}:{candidate_id}"
            )
            evaluation_windows: list[dict[str, Any]] = []
            for record in window_records:
                window_id = _text(record.get("windowId"), name="windowId", required=True)
                assert window_id is not None
                gross = _number(record.get("grossR"), name=f"grossR {candidate_id}/{window_id}", required=True)
                no_cost = _number(record.get("noCostNetR"), name=f"noCostNetR {candidate_id}/{window_id}", required=True)
                conservative = _number(
                    record.get("conservativeNetR"),
                    name=f"conservativeNetR {candidate_id}/{window_id}",
                    required=True,
                )
                closed_trades = _int(record.get("trades"), name=f"trades {candidate_id}/{window_id}", required=True)
                assert gross is not None and no_cost is not None and conservative is not None and closed_trades is not None
                cost = no_cost - conservative
                action_counts = record.get("actionCounts")
                action_counts = action_counts if isinstance(action_counts, Mapping) else {}
                management_count, management_share = _management_action_share(action_counts)
                realized_behavior = record.get("realizedBehavior")
                realized_behavior = realized_behavior if isinstance(realized_behavior, Mapping) else None
                long_net, short_net = _side_contributions(realized_behavior)
                analysis_start = _text(record.get("analysisWindowStart"), name="analysis window start")
                analysis_end = _text(record.get("analysisWindowEnd"), name="analysis window end")
                bundle = bundle_map.get((candidate_id, analysis_start, analysis_end))
                panel_id: str | None = None
                if bundle is None:
                    bundle_checks["missing"] += 1
                else:
                    panel_id, metrics = bundle
                    bundle_checks["matched"] += 1
                    for key, expected, observed in (
                        ("conservativeNetR", conservative, _number(metrics.get("conservativeNetR"), name="bundle conservative net R", required=True)),
                        ("noCostNetR", no_cost, _number(metrics.get("noCostNetR"), name="bundle no cost net R", required=True)),
                        ("closedTrades", float(closed_trades), _number(metrics.get("closedTrades"), name="bundle closed trades", required=True)),
                    ):
                        assert observed is not None
                        if abs(expected - observed) > FLOAT_TOLERANCE:
                            bundle_checks["mismatched"] += 1
                            aggregate_failures.append(
                                f"bundle mismatch {candidate_id}/{window_id}/{key}: {expected} != {observed}"
                            )
                window_row = {
                    "schemaVersion": SCHEMA_VERSION,
                    "cohort": label,
                    "protocolGroup": protocol_group,
                    "evidenceRole": role,
                    "generationIndex": generation_index,
                    "evaluationId": evaluation_id,
                    "candidateId": candidate_id,
                    "candidateIdentitySha256": _text(candidate.get("candidateIdentitySha256"), name="candidate identity"),
                    "parentCandidateId": parent_id,
                    "sourceMode": source_mode,
                    "operatorFamily": operator_family,
                    "panelId": panel_id,
                    "windowId": window_id,
                    "analysisWindowStart": analysis_start,
                    "analysisWindowEnd": analysis_end,
                    "closedTrades": closed_trades,
                    "grossR": gross,
                    "modeledCostR": cost,
                    "conservativeNetR": conservative,
                    "grossExpectancyPerTrade": _safe_divide(gross, closed_trades),
                    "costPerTrade": _safe_divide(cost, closed_trades),
                    "netExpectancyPerTrade": _safe_divide(conservative, closed_trades),
                    "averageHoldingBars": _number(record.get("averageHoldingBars"), name="average holding bars"),
                    "medianHoldingBars": _number(record.get("medianHoldingBars"), name="median holding bars"),
                    "exposureRatio": _number(record.get("exposureRatio"), name="exposure ratio"),
                    "maxDrawdownR": _number(record.get("maxDrawdownR"), name="maximum drawdown R"),
                    "managementActionCount": management_count,
                    "managementActionShare": management_share,
                    "longConservativeNetR": long_net,
                    "shortConservativeNetR": short_net,
                    "supportQualified": support,
                    "qualityQualified": quality,
                    "descriptorTradeFrequency": _text(descriptor.get("tradeFrequency"), name="descriptor trade frequency"),
                    "descriptorMedianHolding": _text(descriptor.get("medianHolding"), name="descriptor median holding"),
                    "sourceResultPath": str(member_path.resolve()),
                }
                if abs(gross - cost - conservative) > FLOAT_TOLERANCE:
                    aggregate_failures.append(
                        f"gross-cost-net mismatch {candidate_id}/{window_id}: {gross} - {cost} != {conservative}"
                    )
                evaluation_windows.append(window_row)
                windows.append(window_row)

            aggregate_trades = _int(aggregate.get("totalTrades"), name="aggregate total trades", required=True)
            aggregate_gross = _number(aggregate.get("totalNoCostNetR"), name="aggregate no cost net R", required=True)
            aggregate_cost = _number(aggregate.get("costDragR"), name="aggregate cost drag R", required=True)
            aggregate_net = _number(aggregate.get("totalConservativeNetR"), name="aggregate conservative net R", required=True)
            assert aggregate_trades is not None and aggregate_gross is not None and aggregate_cost is not None and aggregate_net is not None
            reconstructed = {
                "trades": sum(int(item["closedTrades"]) for item in evaluation_windows),
                "gross": sum(float(item["grossR"]) for item in evaluation_windows),
                "cost": sum(float(item["modeledCostR"]) for item in evaluation_windows),
                "net": sum(float(item["conservativeNetR"]) for item in evaluation_windows),
            }
            for name, expected, observed in (
                ("trades", float(aggregate_trades), float(reconstructed["trades"])),
                ("gross", aggregate_gross, reconstructed["gross"]),
                ("cost", aggregate_cost, reconstructed["cost"]),
                ("net", aggregate_net, reconstructed["net"]),
            ):
                if abs(expected - observed) > FLOAT_TOLERANCE:
                    aggregate_failures.append(
                        f"aggregate/window mismatch {candidate_id}/{name}: {expected} != {observed}"
                    )
            summary_base = {
                "schemaVersion": SCHEMA_VERSION,
                "cohort": label,
                "protocolGroup": protocol_group,
                "evidenceRole": role,
                "generationIndex": generation_index,
                "candidateId": candidate_id,
                "candidateIdentitySha256": _text(candidate.get("candidateIdentitySha256"), name="candidate identity"),
                "parentCandidateId": parent_id,
                "sourceMode": source_mode,
                "operatorFamily": operator_family,
                "supportQualified": support,
                "qualityQualified": quality,
                "medianHoldingBars": _number(aggregate.get("medianHoldingBars"), name="aggregate median holding bars"),
                "averageHoldingBars": _number(aggregate.get("averageHoldingBars"), name="aggregate average holding bars"),
                "averageExposureRatio": _number(aggregate.get("averageExposureRatio"), name="aggregate exposure ratio"),
                "descriptorTradeFrequency": _text(descriptor.get("tradeFrequency"), name="descriptor trade frequency"),
                "descriptorMedianHolding": _text(descriptor.get("medianHolding"), name="descriptor median holding"),
                "sourceResultPath": str(member_path.resolve()),
            }
            summaries.append(
                _candidate_summary(
                    evaluation_id=evaluation_id,
                    base=summary_base,
                    window_rows=evaluation_windows,
                )
            )

    cohort_manifest = {
        "label": label,
        "root": str(root.resolve()),
        "protocolGroup": protocol_group,
        "evidenceRole": role,
        "cohortKind": "v38_operator_matrix" if is_v38 else "v37_current_panel_population",
        "sourceFiles": source_files,
        "candidateRowsBeforeExactDeduplication": sum(len(_read_jsonl(path)) for path in member_paths),
        "candidateRowsAfterExactDeduplication": len(summaries),
        "distinctCandidateIdCount": len({row["candidateId"] for row in summaries}),
        "generationIndexes": sorted(
            {row["generationIndex"] for row in summaries if row["generationIndex"] is not None}
        ),
        "panelIds": sorted({row["panelId"] for row in windows if row["panelId"] is not None}),
        "candidateWindowObservations": len(windows),
        "rowsWithGrossNoCostEconomics": len(windows),
        "rowsWithModeledCostEconomics": len(windows),
        "rowsWithHoldingPhenotype": sum(
            row["medianHoldingBars"] is not None for row in windows
        ),
        "rowsWithManagementPhenotype": sum(
            row["managementActionShare"] is not None for row in windows
        ),
        "candidatePanelBundleChecks": bundle_checks,
        "aggregateReconciliationFailures": aggregate_failures,
    }
    return windows, summaries, cohort_manifest, aggregate_failures


def _reference_manifest(label: str, root: Path) -> dict[str, Any]:
    """Describe a non-economic reference root without turning it into evidence."""

    candidates = sorted(
        path
        for path in root.rglob("*.json")
        if path.name in {"campaign-output-manifest.json", "campaign-output-checkpoint.json", "manifest.json"}
    )
    return {
        "label": label,
        "root": str(root.resolve()),
        "inclusion": "reference_only_excluded_from_economic_map",
        "reason": "retained topology case-study economics are synthetic/no-market conformance values",
        "sourceFiles": [_file_descriptor(path, root=root) for path in candidates],
    }


def _validation_summary(
    *,
    windows: Sequence[Mapping[str, Any]],
    summaries: Sequence[Mapping[str, Any]],
    cohort_manifests: Sequence[Mapping[str, Any]],
    aggregate_failures: Sequence[str],
) -> dict[str, Any]:
    nonfinite = []
    for row in windows:
        for key in (
            "grossR",
            "modeledCostR",
            "conservativeNetR",
            "closedTrades",
        ):
            value = row.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                nonfinite.append({"evaluationId": row.get("evaluationId"), "field": key})
    protocol_rows: dict[str, int] = defaultdict(int)
    evidence_roles: dict[str, set[str]] = defaultdict(set)
    for row in windows:
        protocol = str(row["protocolGroup"])
        protocol_rows[protocol] += 1
        evidence_roles[protocol].add(str(row["evidenceRole"]))
    duplicate_window_keys = len(windows) - len(
        {
            (
                row["evaluationId"],
                row["candidateId"],
                row.get("panelId"),
                row["windowId"],
            )
            for row in windows
        }
    )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "checks": {
            "candidateWindowRows": len(windows),
            "candidateEvaluations": len(summaries),
            "duplicateCandidateWindowRows": duplicate_window_keys,
            "nonfiniteMetricCount": len(nonfinite),
            "grossCostNetReconciliationFailureCount": len(aggregate_failures),
            "aggregateToWindowReconciliationFailureCount": len(aggregate_failures),
            "protocolGroupRows": dict(sorted(protocol_rows.items())),
            "evidenceRolesByProtocol": {
                key: sorted(values) for key, values in sorted(evidence_roles.items())
            },
            "developmentUntouchedLeakage": False,
            "notes": [
                "All analyzed cohorts are explicitly retained research/current-panel evidence; no untouched evaluation rows are pooled into a cohort.",
                "Topology conformance data is recorded as a reference only and excluded from economic summaries.",
                "Support leave-one-window-out is reported as not evaluable because the retained current policy is a four-window policy; this analysis does not replace it with a new gate.",
            ],
            "failures": list(aggregate_failures)[:100],
            "nonfiniteExamples": nonfinite[:100],
            "cohortBundleChecks": {
                str(item["label"]): item["candidatePanelBundleChecks"]
                for item in cohort_manifests
            },
        },
    }


def _feasibility_map(summaries: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    trade_values = [float(row["totalTrades"]) for row in summaries]
    holding_values = [
        float(row["medianHoldingBars"])
        for row in summaries
        if row.get("medianHoldingBars") is not None
    ]
    trade_thresholds, adaptive_trade = _adaptive_bands(trade_values, prefix="trades")
    holding_thresholds, adaptive_holding = _adaptive_bands(holding_values, prefix="holding")
    adaptive_rows = []
    for row in summaries:
        adaptive_rows.append(
            {
                **row,
                "adaptiveTradeBand": adaptive_trade(float(row["totalTrades"])),
                "adaptiveHoldingBand": adaptive_holding(
                    float(row["medianHoldingBars"] or 0.0)
                ),
            }
        )
    fixed_regions = _group_summary(
        summaries,
        keys=("protocolGroup", "frequencyBand", "holdingBand", "supportQualified"),
    )
    adaptive_regions = _group_summary(
        adaptive_rows,
        keys=("protocolGroup", "adaptiveTradeBand", "adaptiveHoldingBand", "supportQualified"),
    )
    support_cost = _group_summary(
        summaries,
        keys=("protocolGroup", "frequencyBand", "supportQualified"),
    )
    census: dict[str, Any] = {}
    for protocol in sorted({str(row["protocolGroup"]) for row in summaries}):
        members = [row for row in summaries if row["protocolGroup"] == protocol]
        census[protocol] = {
            "candidateEvaluationCount": len(members),
            "grossPositiveCount": sum(float(row["totalGrossR"]) > 0 for row in members),
            "afterCostPositiveCount": sum(float(row["totalConservativeNetR"]) > 0 for row in members),
            "supportQualifiedCount": sum(bool(row["supportQualified"]) for row in members),
            "qualityQualifiedCount": sum(bool(row["qualityQualified"]) for row in members),
            "supportAndAfterCostPositiveCount": sum(
                bool(row["supportQualified"]) and float(row["totalConservativeNetR"]) > 0
                for row in members
            ),
            "positiveOnMultipleWindowsCount": sum(
                float(row["positiveWindowFraction"] or 0.0) >= 0.5 for row in members
            ),
            "positiveOnAllWindowsCount": sum(
                float(row["positiveWindowFraction"] or 0.0) == 1.0 for row in members
            ),
            "positiveUnderWindowResamplingMajorityCount": sum(
                float(row["windowResamplePositiveRate"] or 0.0) >= 0.5 for row in members
            ),
        }
    feasibility = {
        "schemaVersion": SCHEMA_VERSION,
        "analysisBands": {
            "fixedFrequency": [
                "zero_no_trade",
                "very_sparse_1_7",
                "sparse_8_19",
                "moderate_20_95",
                "high_turnover_96_plus",
            ],
            "fixedHolding": [
                "unavailable_or_no_trade",
                "brief_1_24_bars",
                "short_25_96_bars",
                "medium_97_480_bars",
                "long_481_plus_bars",
            ],
            "adaptiveTradeQuartileThresholds": trade_thresholds,
            "adaptiveHoldingQuartileThresholds": holding_thresholds,
            "interpretation": "These are descriptive analysis bands, not support, quality, archive, or winner gates.",
        },
        "censusByProtocol": census,
        "supportCostPlane": support_cost,
        "fixedBehavioralRegions": fixed_regions,
        "adaptiveBehavioralRegions": adaptive_regions,
        "materialRegionRule": "A region is described as material in the memo only when it has at least five candidate evaluations; this reporting rule does not change any runtime policy.",
    }
    return feasibility, fixed_regions, adaptive_regions


def _stability_report(summaries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_protocol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in summaries:
        by_protocol[str(row["protocolGroup"])].append(row)
    output: dict[str, Any] = {"schemaVersion": SCHEMA_VERSION, "methods": {}}
    output["methods"] = {
        "windowResampling": {
            "replicates": WINDOW_RESAMPLE_REPLICATES,
            "method": "within-candidate resampling of complete retained windows with replacement",
            "independenceAssumption": "windows are treated as the unit of resampling; this is conservative relative to treating individual trades as IID but does not establish independence between overlapping market regimes",
        },
        "leaveOneWindowOut": {
            "method": "recompute the aggregate after removing each retained window once",
            "policyNote": "the current four-window support policy is not redefined for three-window subsets",
        },
    }
    output["byProtocol"] = {}
    for protocol, members in sorted(by_protocol.items()):
        output["byProtocol"][protocol] = {
            "candidateEvaluationCount": len(members),
            "meanPositiveWindowFraction": _mean(
                [float(row["positiveWindowFraction"] or 0.0) for row in members]
            ),
            "meanLeaveOneWindowOutPositiveFraction": _mean(
                [float(row["leaveOneWindowOutPositiveFraction"] or 0.0) for row in members]
            ),
            "meanWindowResamplePositiveRate": _mean(
                [float(row["windowResamplePositiveRate"] or 0.0) for row in members]
            ),
            "allWindowPositiveCount": sum(
                float(row["positiveWindowFraction"] or 0.0) == 1.0 for row in members
            ),
            "resampleMajorityPositiveCount": sum(
                float(row["windowResamplePositiveRate"] or 0.0) >= 0.5 for row in members
            ),
        }
    output["limitations"] = [
        "No IID trade bootstrap is used as a primary result.",
        "Contiguous trade-block resampling is not computed because this normalized map uses retained complete window outcomes; trade-sequence timestamps remain source data rather than an imputed analysis table.",
        "This overlay describes sensitivity. It is not a new confidence, support, quality, or archive gate.",
    ]
    return output


def _decision_memo(
    feasibility: Mapping[str, Any],
    stability: Mapping[str, Any],
    summaries: Sequence[Mapping[str, Any]],
) -> str:
    census = _mapping(feasibility.get("censusByProtocol"), name="feasibility census")
    support_positive = sum(
        int(_mapping(value, name="census").get("supportAndAfterCostPositiveCount") or 0)
        for value in census.values()
    )
    net_positive = sum(
        int(_mapping(value, name="census").get("afterCostPositiveCount") or 0)
        for value in census.values()
    )
    gross_positive = sum(
        int(_mapping(value, name="census").get("grossPositiveCount") or 0)
        for value in census.values()
    )
    stable_supported = [
        row
        for row in summaries
        if bool(row["supportQualified"])
        and float(row["totalConservativeNetR"]) > 0
        and float(row["positiveWindowFraction"] or 0.0) == 1.0
    ]
    stable_by_protocol: dict[str, int] = defaultdict(int)
    for row in stable_supported:
        stable_by_protocol[str(row["protocolGroup"])] += 1
    stable_summary = ", ".join(
        f"{protocol}: {count}" for protocol, count in sorted(stable_by_protocol.items())
    ) or "none"
    if support_positive == 0 and net_positive > 0:
        plain = "The retained corpus contains after-cost-positive observations, but none are both current-support-qualified and after-cost positive. The present evidence therefore supports the sparse-positive / evidence-limited branch, not an archive or threshold change."
        next_step = "Run a bounded component-surrogate validation: test whether the most repeatable sparse phenotype features predict parent-relative improvement across frozen hosts, without lowering support."
    elif support_positive == 0 and gross_positive > 0:
        plain = "Gross-positive observations exist, but none remain after modeled costs with current support. The present evidence points to a cost-versus-turnover representation question rather than an archive change."
        next_step = "Run a bounded longer-hold/context representation study against frozen evidence, measuring gross expectancy, cost per trade, and net expectancy without starting a generation."
    elif support_positive == 0:
        plain = "No after-cost-positive, current-support-qualified observation appears in the retained analyzed cohorts. The map cannot justify selection-policy tuning as a substitute for missing economic habitat."
        next_step = "Run the smallest entry/event/context vocabulary calibration experiment that can establish whether any gross-positive response is represented."
    else:
        plain = (
            "A supported, after-cost-positive habitat is reachable in the retained corpus, "
            f"but it is thin: {len(stable_supported)} support-qualified evaluation(s) are "
            "after-cost positive in every retained window "
            f"({stable_summary}). This is evidence of a target, not evidence that the target is broad or already reproducible."
        )
        next_step = "Run an offline archive-preservation counterfactual on the frozen outcomes before any new generation, to test whether the observed supported habitat was discarded or simply not reproduced."
    census_table = _markdown_table(
        [
            {"protocol": key, **_mapping(value, name="census")}
            for key, value in sorted(census.items())
        ],
        (
            ("protocol", "Protocol"),
            ("candidateEvaluationCount", "Candidate evaluations"),
            ("grossPositiveCount", "Gross positive"),
            ("afterCostPositiveCount", "After-cost positive"),
            ("supportQualifiedCount", "Support qualified"),
            ("supportAndAfterCostPositiveCount", "Support + after-cost"),
            ("positiveOnAllWindowsCount", "Positive all windows"),
        ),
    )
    return f"""# Evolutionary feasibility map — decision memo

## 1. Plain-language answer

{plain}

## 2. Evidence

{census_table}

The map is phenotype-first: its compact tables separate trade frequency, holding duration, gross expectancy, modeled cost, after-cost expectancy, active-window breadth, and side contribution. V37 and V38 remain separate protocol strata. The topology factorial is provenance-only because its retained economics are synthetic/no-market conformance values.

## 3. The scissors

Gross-positive count: **{gross_positive}**. After-cost-positive count: **{net_positive}**. Current-support-qualified and after-cost-positive count: **{support_positive}**.

Those three counts must be read together. A gross result is not enough if modeled cost closes it, and a sparse net-positive tail is not enough to declare a viable habitat. The accompanying `feasibility-map.json` and `decision-stability.json` show both fixed and adaptive bands plus window-resampling and leave-one-window-out sensitivity.

## 4. Search implications

This package does not change the organism, the judge, support, quality, archive, cost, or validation policy. It distinguishes observations from durable habitat and makes the protocol boundary explicit. The current four-window support policy is not recalculated on three-window subsets; that would be a new policy, not an analysis.

## 5. Next experiment

{next_step}

## 6. Unknowns

- This retained-window analysis cannot prove that individual trades are independent.
- It does not turn a descriptive tail into a production candidate or breeder.
- It cannot use the synthetic topology conformance economics to make a market claim.
- It does not decide whether a component transfers across hosts; that is the next bounded experiment if the sparse-positive branch is indicated.

No market evaluation, worker, gateway, Vast instance, generation, archive mutation, or production artifact rewrite occurred.
"""


def _protocol_report(cohorts: Sequence[Mapping[str, Any]], references: Sequence[Mapping[str, Any]]) -> str:
    lines = ["# Protocol compatibility and exclusions", "", "## Included economic strata", ""]
    for cohort in cohorts:
        lines.extend(
            [
                f"- **{cohort['label']}** — `{cohort['protocolGroup']}`; role `{cohort['evidenceRole']}`; "
                f"{cohort['candidateRowsAfterExactDeduplication']} candidate evaluations and "
                f"{cohort['candidateWindowObservations']} candidate-window observations.",
            ]
        )
    lines.extend(["", "## Excluded references", ""])
    for reference in references:
        lines.append(f"- **{reference['label']}** — {reference['reason']}")
    lines.extend(
        [
            "",
            "## Rules enforced",
            "",
            "- V37 five-generation current-panel population and V38 frozen-parent matrix are reported as separate protocol groups.",
            "- Candidate re-evaluations retain their distinct source evaluation identity; exact duplicate source rows are the only rows deduplicated.",
            "- No development and untouched evidence are pooled into a shared decision metric.",
            "- Missing panel fields are marked unavailable rather than imputed.",
        ]
    )
    return "\n".join(lines) + "\n"


def _data_dictionary() -> str:
    return """# Normalized schema / data dictionary

`normalized-candidate-windows.jsonl` has one row per retained candidate evaluation and complete market window.

| Field | Meaning |
| --- | --- |
| `cohort`, `protocolGroup`, `evidenceRole` | Explicit stratum and retained-evidence role; never a pooled fitness label. |
| `evaluationId` | Source-row identity, preserving candidate re-evaluations. |
| `candidateId`, `parentCandidateId`, `operatorFamily` | Identity and lineage metadata where retained. |
| `panelId`, `windowId` | Retained panel/window identity; `panelId` is null only when the scored source does not retain it. |
| `grossR` | Retained no-cost/gross result for the window. |
| `modeledCostR` | `noCostNetR - conservativeNetR`; no cost is imputed. |
| `conservativeNetR` | Retained after-cost result used by the historical current policy. |
| `closedTrades` | Closed trades in the retained window. |
| `grossExpectancyPerTrade`, `costPerTrade`, `netExpectancyPerTrade` | Window totals divided by `closedTrades`; null when no trades occurred. |
| `averageHoldingBars`, `medianHoldingBars`, `exposureRatio` | Actual retained phenotype fields; null means unavailable. |
| `managementActionCount`, `managementActionShare` | Derived from retained realized action counts, not authored graph structure. |
| `longConservativeNetR`, `shortConservativeNetR` | Retained side contribution when present. |
| `supportQualified`, `qualityQualified` | Historical source classifications, not recalculated or altered. |

`candidate-evaluations.jsonl` is a compact four-window roll-up for sensitivity analysis. It retains original support/quality classifications and labels all added bands as descriptive only.
"""


def _readme(cohorts: Sequence[Mapping[str, Any]], references: Sequence[Mapping[str, Any]]) -> str:
    cohort_lines = "\n".join(
        f"- `{item['label']}`: `{item['root']}` ({item['candidateWindowObservations']} candidate-window rows)"
        for item in cohorts
    )
    reference_lines = "\n".join(
        f"- `{item['label']}`: excluded from economics — {item['reason']}" for item in references
    )
    return f"""# Temporal-QD evolutionary feasibility map v1

This is an offline, reproducible analysis of retained candidate/window outcomes. It maps the observable economic habitat before any change to evolution, support, quality, costs, the archive, or the grammar.

## Included inputs

{cohort_lines}

## Reference-only inputs

{reference_lines}

## Regenerate

```powershell
uv run python -m autoresearch.temporal_qd_evolutionary_feasibility_map \
  --cohort v37=<V37_ROOT> \
  --cohort v38=<V38_ROOT> \
  --reference topology=<TOPOLOGY_ROOT> \
  --output-dir <IGNORED_OUTPUT_DIR>
```

The normalizer reads reduced `evaluated-members.jsonl` rows and reconciles them to paired `candidate-panel-bundles.jsonl` metrics where available. Large normalized row-level tables remain in the ignored output directory; compact tables, manifests, and this memo are suitable for code review.

No market evaluation, worker, gateway, Vast instance, generation, archive mutation, or production artifact rewrite is performed by this command.
"""


def build_feasibility_map(
    *,
    cohorts: Sequence[tuple[str, Path]],
    references: Sequence[tuple[str, Path]],
    output_dir: Path,
) -> dict[str, Any]:
    if not cohorts:
        raise FeasibilityMapError("at least one --cohort is required")
    labels = [label for label, _ in cohorts]
    if len(labels) != len(set(labels)):
        raise FeasibilityMapError("cohort labels must be unique")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FeasibilityMapError(f"output directory must be empty: {output_dir}")

    all_windows: list[dict[str, Any]] = []
    all_summaries: list[dict[str, Any]] = []
    manifests: list[dict[str, Any]] = []
    failures: list[str] = []
    for label, root in cohorts:
        windows, summaries, manifest, cohort_failures = _load_cohort(label, root)
        all_windows.extend(windows)
        all_summaries.extend(summaries)
        manifests.append(manifest)
        failures.extend(cohort_failures)
    reference_manifests = [_reference_manifest(label, root) for label, root in references]
    all_windows.sort(key=lambda row: (str(row["evaluationId"]), str(row["windowId"])))
    all_summaries.sort(key=lambda row: str(row["evaluationId"]))

    feasibility, fixed_regions, adaptive_regions = _feasibility_map(all_summaries)
    stability = _stability_report(all_summaries)
    validation = _validation_summary(
        windows=all_windows,
        summaries=all_summaries,
        cohort_manifests=manifests,
        aggregate_failures=failures,
    )
    corpus_manifest: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "includedCohorts": manifests,
        "referenceOnlyExclusions": reference_manifests,
        "normalizedTable": {
            "path": "normalized-candidate-windows.jsonl",
            "rowCount": len(all_windows),
        },
        "candidateEvaluationTable": {
            "path": "candidate-evaluations.jsonl",
            "rowCount": len(all_summaries),
        },
        "reproducibility": {
            "deterministicOrdering": True,
            "windowResampleReplicates": WINDOW_RESAMPLE_REPLICATES,
            "runtimeInputs": "retained local files only",
        },
    }
    corpus_manifest["manifestSha256"] = _sha256_bytes(_canonical_bytes(corpus_manifest))
    manifest_body = dict(corpus_manifest)
    supplied_manifest_sha = manifest_body.pop("manifestSha256")
    validation_checks = _mapping(validation.get("checks"), name="validation checks")
    validation_checks["manifestSelfHashValid"] = (
        supplied_manifest_sha == _sha256_bytes(_canonical_bytes(manifest_body))
    )
    validation_checks["binningSensitivityViewsPresent"] = bool(fixed_regions) and bool(
        adaptive_regions
    )
    validation_checks["stabilityMethodSanity"] = all(
        0.0 <= float(row["windowResamplePositiveRate"]) <= 1.0
        and 0.0 <= float(row["positiveWindowFraction"]) <= 1.0
        and 0.0 <= float(row["leaveOneWindowOutPositiveFraction"]) <= 1.0
        for row in all_summaries
        if row.get("windowResamplePositiveRate") is not None
    )

    _write_jsonl(output_dir / "normalized-candidate-windows.jsonl", all_windows)
    _write_jsonl(output_dir / "candidate-evaluations.jsonl", all_summaries)
    _write_json(output_dir / "corpus-manifest.json", corpus_manifest)
    _write_json(output_dir / "feasibility-map.json", feasibility)
    _write_json(output_dir / "decision-stability.json", stability)
    _write_json(output_dir / "validation-results.json", validation)
    _write_json(
        output_dir / "aggregate-tables.json",
        {
            "schemaVersion": SCHEMA_VERSION,
            "fixedBehavioralRegions": fixed_regions,
            "adaptiveBehavioralRegions": adaptive_regions,
            "supportCostPlane": feasibility["supportCostPlane"],
        },
    )
    _write_csv(output_dir / "fixed-behavioral-regions.csv", fixed_regions)
    _write_csv(output_dir / "adaptive-behavioral-regions.csv", adaptive_regions)
    (output_dir / "README.md").write_text(
        _readme(manifests, reference_manifests), encoding="utf-8", newline="\n"
    )
    (output_dir / "normalized-schema.md").write_text(
        _data_dictionary(), encoding="utf-8", newline="\n"
    )
    (output_dir / "protocol-compatibility.md").write_text(
        _protocol_report(manifests, reference_manifests), encoding="utf-8", newline="\n"
    )
    (output_dir / "decision-stability-report.md").write_text(
        "# Decision stability overlay\n\n"
        + "The machine-readable result is `decision-stability.json`. It uses complete-window resampling and leave-one-window-out only; it does not introduce a new policy gate.\n",
        encoding="utf-8",
        newline="\n",
    )
    (output_dir / "decision-memo.md").write_text(
        _decision_memo(feasibility, stability, all_summaries), encoding="utf-8", newline="\n"
    )

    output_files = sorted(path for path in output_dir.iterdir() if path.is_file())
    checksums = "\n".join(
        f"{_sha256_file(path).removeprefix('sha256:')}  {path.name}" for path in output_files
    )
    (output_dir / "checksums.sha256").write_text(
        checksums + "\n", encoding="utf-8", newline="\n"
    )
    return {
        "outputDir": str(output_dir.resolve()),
        "candidateWindowRows": len(all_windows),
        "candidateEvaluations": len(all_summaries),
        "validationFailureCount": len(failures),
        "manifestSha256": corpus_manifest["manifestSha256"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--reference", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = build_feasibility_map(
            cohorts=[_parse_labeled_path(item, option_name="--cohort") for item in args.cohort],
            references=[_parse_labeled_path(item, option_name="--reference") for item in args.reference],
            output_dir=args.output_dir.resolve(),
        )
    except FeasibilityMapError as exc:
        parser.error(str(exc))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
