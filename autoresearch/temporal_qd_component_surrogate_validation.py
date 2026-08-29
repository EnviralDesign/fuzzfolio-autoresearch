"""Offline, provenance-first V38 component-surrogate validation.

This module deliberately does not replay a profile, open a worker, or infer an
event stream from mutable current defaults.  It turns the retained V38 forensic
records into a reproducible *retrospective availability audit*: the outcome
table is useful descriptive evidence, while the absence of frozen event-level
features prevents it from becoming a component-selection gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from .evidence_plan import canonical_json, canonical_sha256


SCHEMA_VERSION = "temporal_qd_component_surrogate_validation_v1"
DEVELOPMENT_PANEL_FALLBACK = "panel-3"
EXPECTED_TOPOLOGY_RESULT_SHA256 = (
    "sha256:f5c49eae57aace0c254b43f3c22e479aa02f807de7db3b283d096f9f30fa60d0"
)
EPSILON = 1e-12


class ValidationError(ValueError):
    """Raised when a retained input cannot be bound exactly."""


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValidationError(f"expected JSON object: {path}")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _validate_report_hash(payload: Mapping[str, Any], source_name: str) -> str:
    reported = payload.get("reportSha256")
    if not isinstance(reported, str):
        raise ValidationError(f"{source_name} has no reportSha256")
    material = {key: value for key, value in payload.items() if key != "reportSha256"}
    actual = canonical_sha256(material)
    if actual != reported:
        raise ValidationError(
            f"{source_name} report hash mismatch: expected {reported}, calculated {actual}"
        )
    return actual


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(canonical_json(row))
            handle.write("\n")


def _read_jsonl_by_id(
    path: Path, wanted_ids: set[str], *, require_all: bool = True
) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            candidate_id = value.get("candidateId")
            if candidate_id in wanted_ids:
                if candidate_id in found:
                    raise ValidationError(
                        f"duplicate candidateId {candidate_id!r} at {path}:{line_number}"
                    )
                found[candidate_id] = value
    missing = sorted(wanted_ids.difference(found))
    if missing and require_all:
        raise ValidationError(
            f"evaluated-member source lacks {len(missing)} requested candidates: {missing[:3]}"
        )
    return found


def _suffix_after(value: str, marker: str) -> str:
    if marker not in value:
        raise ValidationError(f"expected {marker!r} in retained identity {value!r}")
    suffix = value.split(marker, 1)[1]
    if not suffix:
        raise ValidationError(f"empty identity suffix in {value!r}")
    return suffix


def _find_binding(
    *, source_profile: Mapping[str, Any], forensic_case: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = source_profile.get("graph")
    indicators = source_profile.get("indicators")
    if not isinstance(graph, Mapping) or not isinstance(indicators, list):
        raise ValidationError("candidate source profile lacks graph or indicators")
    event_suffix = _suffix_after(str(forensic_case["eventId"]), "evt_")
    instance_suffix = _suffix_after(str(forensic_case["indicatorInstanceId"]), "evtind_")
    bindings = graph.get("eventBindings")
    if not isinstance(bindings, list):
        raise ValidationError("candidate source profile lacks eventBindings")
    matching_bindings = [
        value
        for value in bindings
        if isinstance(value, dict) and str(value.get("id", "")).endswith(event_suffix)
    ]
    if len(matching_bindings) != 1:
        raise ValidationError(
            f"expected one frozen binding for {forensic_case['candidateId']}, found {len(matching_bindings)}"
        )
    matching_indicators = [
        value
        for value in indicators
        if isinstance(value, dict)
        and isinstance(value.get("meta"), dict)
        and str(value["meta"].get("instanceId", "")).endswith(instance_suffix)
    ]
    if len(matching_indicators) != 1:
        raise ValidationError(
            f"expected one frozen indicator for {forensic_case['candidateId']}, found {len(matching_indicators)}"
        )
    binding = matching_bindings[0]
    indicator = matching_indicators[0]
    if binding.get("indicatorInstanceId") != indicator["meta"].get("instanceId"):
        raise ValidationError(f"event binding / indicator mismatch for {forensic_case['candidateId']}")
    if indicator["meta"].get("id") != forensic_case.get("indicatorId"):
        raise ValidationError(f"indicator identity mismatch for {forensic_case['candidateId']}")
    return binding, indicator


def _string_reference_paths(value: Any, target: str, path: str = "graph") -> list[str]:
    """Return frozen graph paths that name a binding; no semantic inference."""

    if isinstance(value, str):
        return [path] if value == target else []
    if isinstance(value, list):
        return [
            item
            for index, child in enumerate(value)
            for item in _string_reference_paths(child, target, f"{path}[{index}]")
        ]
    if isinstance(value, dict):
        return [
            item
            for key, child in value.items()
            for item in _string_reference_paths(child, target, f"{path}.{key}")
        ]
    return []


def _terminal_history(record: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = record.get("candidate")
    if not isinstance(candidate, Mapping):
        raise ValidationError("evaluated member lacks candidate")
    history = candidate.get("structuralOperatorHistory")
    if not isinstance(history, list) or not history or not isinstance(history[-1], Mapping):
        raise ValidationError(f"candidate {record.get('candidateId')} lacks terminal operator history")
    return history[-1]


def _component_identity(indicator: Mapping[str, Any], binding: Mapping[str, Any]) -> str:
    meta = indicator["meta"]
    payload = {
        "schemaVersion": "temporal_qd_component_identity_v1",
        "indicatorId": meta.get("id"),
        "baseIndicatorId": meta.get("baseIndicatorId"),
        "indicatorConfig": indicator.get("config"),
        "signalPersistence": meta.get("signalPersistence"),
        "signalRole": meta.get("signalRole"),
        "timeframe": indicator.get("config", {}).get("timeframe"),
        "lookbackBars": indicator.get("config", {}).get("lookbackBars"),
        "useFormingBar": indicator.get("config", {}).get("useFormingBar"),
        "eventOutputs": {
            "longOutput": binding.get("longOutput"),
            "shortOutput": binding.get("shortOutput"),
        },
    }
    return canonical_sha256(payload)


def _classify_mechanism(case: Mapping[str, Any]) -> str | None:
    if not case["relative"].get("comparable"):
        return None
    deltas = case["deltas"]
    metrics = case["metrics"]
    gross = float(deltas["grossNoCostNetR"])
    cost = float(deltas["costDragR"])
    net = float(deltas["cumulativeConservativeNetR"])
    trades = float(metrics["tradeCount"])
    if trades == 0 and float(deltas["tradeCount"]) < 0:
        return "zero_trade_suppression"
    if net < -EPSILON:
        return "destructive"
    if abs(net) <= EPSILON:
        return "economically_inert"
    if net > EPSILON and cost < -EPSILON and gross <= EPSILON:
        return "cost_suppression_only"
    if net > EPSILON and gross > EPSILON and cost < -EPSILON:
        return "gross_and_cost_improvement"
    if net > EPSILON and gross > EPSILON:
        return "gross_selection_improvement"
    return "destructive"


def _quantiles(values: Sequence[float]) -> dict[str, float | None]:
    ordered = sorted(values)
    if not ordered:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}

    def at(fraction: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        position = (len(ordered) - 1) * fraction
        low = math.floor(position)
        high = math.ceil(position)
        if low == high:
            return ordered[low]
        return ordered[low] + (ordered[high] - ordered[low]) * (position - low)

    return {"min": ordered[0], "p25": at(0.25), "median": at(0.5), "p75": at(0.75), "max": ordered[-1]}


def _average_ranks(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(indexed):
        end = start + 1
        while end < len(indexed) and indexed[end][1] == indexed[start][1]:
            end += 1
        rank = (start + 1 + end) / 2.0
        for original_index, _ in indexed[start:end]:
            ranks[original_index] = rank
        start = end
    return ranks


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    rank_left = _average_ranks(left)
    rank_right = _average_ranks(right)
    left_mean = sum(rank_left) / len(rank_left)
    right_mean = sum(rank_right) / len(rank_right)
    numerator = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(rank_left, rank_right)
    )
    left_denominator = math.sqrt(sum((a - left_mean) ** 2 for a in rank_left))
    right_denominator = math.sqrt(sum((b - right_mean) ** 2 for b in rank_right))
    if left_denominator == 0 or right_denominator == 0:
        return None
    return numerator / (left_denominator * right_denominator)


def _phenotype_groups(cases: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in cases:
        groups[str(case["phenotypeIdentitySha256"])].append(case)
    return dict(groups)


def _source_entry(path: Path, *, role: str, report_sha256: str | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "path": str(path),
        "sha256": _file_sha256(path),
        "role": role,
    }
    if report_sha256 is not None:
        entry["selfReportedSha256"] = report_sha256
        entry["selfReportHashValidated"] = True
    return entry


def _metric_deltas(case: Mapping[str, Any]) -> dict[str, Any]:
    deltas = case["deltas"]
    gross = float(deltas["grossNoCostNetR"])
    cost = float(deltas["costDragR"])
    net = float(deltas["cumulativeConservativeNetR"])
    residual = net - (gross - cost)
    if abs(residual) > EPSILON:
        raise ValidationError(
            f"net/cost identity mismatch for {case['candidateId']}: residual {residual}"
        )
    return {
        "deltaGrossR": gross,
        "deltaModeledCostR": cost,
        "deltaNetR": net,
        "deltaTradeCount": float(deltas["tradeCount"]),
        "deltaWorstWindowR": float(deltas["worstWindowConservativeNetR"]),
        "deltaMedianWindowR": float(
            case["metrics"]["medianWindowConservativeNetR"]
            - case["parentMetrics"]["medianWindowConservativeNetR"]
        ),
        "identity": "deltaNetR = deltaGrossR - deltaModeledCostR",
    }


def _topology_reference(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {
            "status": "excluded_missing_expected_authenticated_result",
            "expectedAnalysisSha256": EXPECTED_TOPOLOGY_RESULT_SHA256,
            "reason": "No supplied topology result has the prompt-required authenticated hash; no fixture or alternate result was substituted.",
            "comparison": "not_performed",
        }
    payload = _read_json(path)
    actual = payload.get("analysisSha256") or payload.get("reportSha256")
    if actual != EXPECTED_TOPOLOGY_RESULT_SHA256:
        raise ValidationError(
            "topology source does not carry the prompt-required authenticated hash: "
            f"{actual!r} != {EXPECTED_TOPOLOGY_RESULT_SHA256}"
        )
    blocks = payload.get("blocks")
    if not isinstance(blocks, Mapping) or len(blocks) != 3:
        raise ValidationError("topology source must have exactly three retained blocks")
    return {
        "status": "available_mechanism_reference_only",
        "expectedAnalysisSha256": EXPECTED_TOPOLOGY_RESULT_SHA256,
        "source": _source_entry(path, role="factorial mechanism/reference only"),
        "blockCount": len(blocks),
        "panelCount": 3,
        "comparison": "P/E reference retained; no component profitability or selected-panel confirmation claim is made.",
    }


def _markdown_report(summary: Mapping[str, Any]) -> str:
    decision = summary["decision"]
    source = summary["sources"]
    outcome = summary["samePanelDescriptive"]
    return "\n".join(
        [
            "# V38 component-surrogate validation v1",
            "",
            "## Decision",
            "",
            f"**{decision['taxonomy']}** — no frozen event-start, forward-return, MFE/MAE, or parent-opportunity series exists in the retained inputs, and the authorized boundary forbids rebuilding one by replay.  S0–S3 therefore produced no score and no component-selection rule.",
            "",
            "## What was bound",
            "",
            f"- {summary['corpus']['acceptedDirectionalEventInsertChildren']} accepted directional-event-insert children; {summary['corpus']['samePanelComparableChildren']} exact development-panel parent comparisons.",
            f"- {summary['corpus']['multipanelChildren']} multipanel child records are retained only as a selected, retrospective cross-panel check.",
            f"- The V38 forensic report self-hash was validated: `{source['eventForensic']['selfReportedSha256']}`.",
            "",
            "## Descriptive outcome evidence (not a surrogate validation)",
            "",
            f"- Exact comparable cases: {outcome['count']}; parent beats: {outcome['parentBeats']}; losses: {outcome['parentLosses']}; full economic phenotype ties: {outcome['fullEconomicPhenotypeTies']}.",
            f"- Δnet R median: {outcome['deltaNetR']['median']}; Δgross R median: {outcome['deltaGrossR']['median']}; Δmodeled cost R median: {outcome['deltaModeledCostR']['median']}.",
            "",
            "The outcome-only table can describe the retained V38 cohort, but it cannot validate whether a component score predicts an insertion result: no score was available before outcomes.  Do not use it to rank events or change archive, quality, risk, cost, direction, or operator policy.",
            "",
            "## Preserved conclusion",
            "",
            "V37 archive terminalization remains unchanged: the archive phase is complete, with no request here to revive members, backfill capacity, or modify any production/archive policy.",
            "",
        ]
    )


def _write_checksums(output_dir: Path) -> dict[str, str]:
    files = sorted(
        path
        for path in output_dir.iterdir()
        if path.is_file() and path.name != "CHECKSUMS.sha256"
    )
    checksums = {path.name: _file_sha256(path) for path in files}
    (output_dir / "CHECKSUMS.sha256").write_text(
        "".join(f"{value[7:]}  {name}\n" for name, value in checksums.items()),
        encoding="utf-8",
    )
    return checksums


def run_validation(
    *,
    event_forensic_path: Path,
    multipanel_path: Path,
    evaluated_members_path: Path,
    output_dir: Path,
    topology_analysis_path: Path | None = None,
) -> dict[str, Any]:
    """Materialize the offline report and return its stable summary."""

    forensic = _read_json(event_forensic_path)
    multipanel = _read_json(multipanel_path)
    forensic_hash = _validate_report_hash(forensic, "event forensic")
    multipanel_hash = _validate_report_hash(multipanel, "multipanel")
    cases = forensic.get("cases")
    multi_children = multipanel.get("children")
    if not isinstance(cases, list) or not isinstance(multi_children, list):
        raise ValidationError("retained V38 reports lack cases/children arrays")
    if len(cases) != int(forensic.get("acceptedChildren", -1)):
        raise ValidationError("event forensic acceptedChildren count does not match cases")
    if len(cases) != len(multi_children):
        raise ValidationError("event forensic and multipanel child counts differ")
    child_ids = {str(case["candidateId"]) for case in cases}
    parent_ids = {str(case["parentCandidateId"]) for case in cases}
    records = _read_jsonl_by_id(evaluated_members_path, child_ids)
    parent_records = _read_jsonl_by_id(
        evaluated_members_path, parent_ids, require_all=False
    )
    multi_by_child = {str(child["candidateId"]): child for child in multi_children}
    if set(multi_by_child) != child_ids:
        raise ValidationError("multipanel child identities do not equal forensic child identities")

    output_dir = output_dir.resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    development_panel = str(multipanel.get("developmentPanelId") or DEVELOPMENT_PANEL_FALLBACK)

    accepted_rows: list[dict[str, Any]] = []
    contexts: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    score_rows: list[dict[str, Any]] = []
    component_contexts: dict[str, list[str]] = defaultdict(list)
    comparable_cases: list[Mapping[str, Any]] = []
    mechanism_counts: Counter[str] = Counter()

    for case in sorted(cases, key=lambda value: str(value["candidateId"])):
        child_id = str(case["candidateId"])
        child = records[child_id]
        parent = parent_records.get(str(case["parentCandidateId"]))
        candidate = child.get("candidate")
        if not isinstance(candidate, Mapping):
            raise ValidationError(f"candidate metadata unavailable for {child_id}")
        if candidate.get("programSha256") != case.get("authoredProgramSha256"):
            raise ValidationError(f"authored program binding mismatch for {child_id}")
        source_profile = candidate.get("sourceProfile")
        if not isinstance(source_profile, Mapping):
            raise ValidationError(f"source profile unavailable for {child_id}")
        binding, indicator = _find_binding(source_profile=source_profile, forensic_case=case)
        meta = indicator["meta"]
        component_id = _component_identity(indicator, binding)
        terminal_history = _terminal_history(child)
        parent_behavior = (
            parent.get("aggregate", {}).get("behaviorIdentitySha256")
            if isinstance(parent, Mapping)
            else None
        )
        route_references = _string_reference_paths(
            source_profile.get("graph", {}), str(binding["id"])
        )
        context_payload = {
            "schemaVersion": "temporal_qd_component_context_identity_v1",
            "componentIdentity": component_id,
            "parentCandidateId": case["parentCandidateId"],
            "parentPhenotypeIdentitySha256": parent_behavior,
            "side": case["side"],
            "bindingId": binding["id"],
            "routeReferencePaths": route_references,
            "routeAvailability": "exact_graph_references" if route_references else "not_explicit_in_retained_snapshot",
        }
        context_id = canonical_sha256(context_payload)
        component_contexts[component_id].append(context_id)
        accepted_rows.append(
            {
                "candidateId": child_id,
                "parentCandidateId": case["parentCandidateId"],
                "side": case["side"],
                "forensicCase": case,
                "evaluatedMemberBindings": {
                    "candidateIdentitySha256": candidate.get("candidateIdentitySha256"),
                    "proposalEntrySha256": candidate.get("proposalEntrySha256"),
                    "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
                    "sourceProfileSha256": candidate.get("sourceProfileSha256"),
                    "terminalOperatorApplicationSha256": terminal_history.get(
                        "terminalOperatorApplicationSha256"
                    ),
                    "terminalOperatorPlanSha256": terminal_history.get(
                        "terminalOperatorPlanSha256"
                    ),
                },
            }
        )
        contexts.append(
            {
                "componentIdentity": component_id,
                "componentContextIdentity": context_id,
                "candidateId": child_id,
                "parentCandidateId": case["parentCandidateId"],
                "parentPhenotypeIdentitySha256": parent_behavior,
                "parentPhenotypeAvailability": "evaluated_member_snapshot"
                if parent_behavior is not None
                else "not_retained_in_evaluated_members",
                "side": case["side"],
                "insertion": {
                    "forensicEventId": case["eventId"],
                    "exactBindingId": binding["id"],
                    "bindingIndicatorInstanceId": binding["indicatorInstanceId"],
                    "routeReferencePaths": route_references,
                    "routeAvailability": context_payload["routeAvailability"],
                },
                "component": {
                    "indicatorId": meta.get("id"),
                    "baseIndicatorId": meta.get("baseIndicatorId"),
                    "indicatorInstanceId": meta.get("instanceId"),
                    "fullConfiguration": indicator.get("config"),
                    "signalPersistence": meta.get("signalPersistence"),
                    "signalRole": meta.get("signalRole"),
                    "timeframe": indicator.get("config", {}).get("timeframe"),
                    "lookbackBars": indicator.get("config", {}).get("lookbackBars"),
                    "eventOutputs": {
                        "longOutput": binding.get("longOutput"),
                        "shortOutput": binding.get("shortOutput"),
                    },
                },
                "authorities": {
                    "sourceProfileSnapshotSha256": candidate.get("profileSnapshotSha256"),
                    "sourceProfileSha256": candidate.get("sourceProfileSha256"),
                    "authoredProgramSha256": case.get("authoredProgramSha256"),
                    "resolvedProgramSha256": case.get("resolvedProgramSha256"),
                    "phenotypeIdentitySha256": case.get("phenotypeIdentitySha256"),
                    "proposalEntrySha256": candidate.get("proposalEntrySha256"),
                    "terminalOperatorApplicationSha256": terminal_history.get(
                        "terminalOperatorApplicationSha256"
                    ),
                    "terminalOperatorPlanSha256": terminal_history.get(
                        "terminalOperatorPlanSha256"
                    ),
                    "receiptSha256": None,
                    "receiptAvailability": "not_retained_in_evaluated_member_or_forensic_input",
                    "catalogAuthoritySha256": None,
                    "catalogAuthorityAvailability": "not_retained_in_evaluated_member_or_forensic_input",
                    "evidenceWindowIdentities": case.get("windowIdentities"),
                    "economicsBasis": case.get("metrics", {}).get("economicsBasis"),
                },
            }
        )
        outcome = {
            "candidateId": child_id,
            "componentContextIdentity": context_id,
            "developmentPanelId": development_panel,
            "samePanelComparable": bool(case["relative"].get("comparable")),
            "outcomeAvailability": "exact_parent_comparable"
            if case["relative"].get("comparable")
            else "parent_not_comparable",
            "relative": case["relative"],
            "supportAndDirection": {
                "childCombinedSupportPass": case.get("metrics", {}).get("combinedSupportPass"),
                "childDirectionEligible": case.get("metrics", {}).get("directionEligible"),
                "childQualityLike": case.get("metrics", {}).get("currentPanelQualityLike"),
                "childFrontierLike": case.get("metrics", {}).get("currentPanelFrontierLike"),
            },
            "mechanismClassification": _classify_mechanism(case),
        }
        if outcome["samePanelComparable"]:
            outcome["deltas"] = _metric_deltas(case)
            comparable_cases.append(case)
            mechanism_counts[str(outcome["mechanismClassification"])] += 1
        outcome_rows.append(outcome)
        score_rows.append(
            {
                "componentContextIdentity": context_id,
                "componentIdentity": component_id,
                "scoreProtocol": "S0-S3 frozen before outcome analysis",
                "S0_densityOpportunityRetention": None,
                "S1_contextFreeEventResponse": None,
                "S2_parentConditionedEventResponse": None,
                "S3_smallCombination": None,
                "status": "unavailable",
                "reason": "No frozen event-start timestamps, event-state series, forward response, MFE/MAE, or retained parent opportunity timestamps. Rebuilding them would require prohibited replay or mutable-default evaluation.",
                "baselines": {
                    "randomWithinParent": "unavailable_no_score",
                    "densityAlone": "unavailable_no_density",
                    "parentBaseline": "unavailable_no_preoutcome_signal",
                    "indicatorIdPriors": "unavailable_no_preoutcome_signal",
                },
            }
        )

    if len(comparable_cases) != int(forensic.get("archiveParentComparable", -1)):
        raise ValidationError("forensic archiveParentComparable count does not match cases")
    phenotype_groups = _phenotype_groups(comparable_cases)
    genotype_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    program_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in comparable_cases:
        genotype_groups[str(case["authoredProgramSha256"])].append(case)
        program_groups[str(case["resolvedProgramSha256"])].append(case)
    representative_cases = [
        sorted(group, key=lambda case: str(case["candidateId"]))[0]
        for _, group in sorted(phenotype_groups.items())
    ]
    deltas = [_metric_deltas(case) for case in comparable_cases]
    gross = [row["deltaGrossR"] for row in deltas]
    cost = [row["deltaModeledCostR"] for row in deltas]
    net = [row["deltaNetR"] for row in deltas]
    parent_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for case in comparable_cases:
        parent_groups[str(case["parentCandidateId"])].append(case)

    cross_panel_rows: list[dict[str, Any]] = []
    for child_id in sorted(multi_by_child):
        child = multi_by_child[child_id]
        panels = child.get("panels", {})
        cross_panel_rows.append(
            {
                "candidateId": child_id,
                "parentCandidateId": child.get("parentCandidateId"),
                "side": child.get("side"),
                "enteredBackfillCohort": child.get("enteredBackfillCohort"),
                "panels": panels,
            }
        )

    component_evidence_rows = [
        {
            "componentIdentity": component_id,
            "contextCount": len(context_ids),
            "availability": {
                "eventStartTimestamps": False,
                "eventActiveFraction": False,
                "eventsPerBars": False,
                "spacingOrClustering": False,
                "forwardReturns_1_3_6_12_24": False,
                "MFE_MAE": False,
                "volatilityNormalization": False,
                "parentEntryConditionedOpportunities": False,
            },
            "status": "insufficient_retained_preoutcome_evidence",
            "reason": "The exact V38 accepted-child records retain configuration and strategy outcomes, not frozen component event series. Atlas regeneration is intentionally excluded because its current path invokes replay simulation.",
        }
        for component_id, context_ids in sorted(component_contexts.items())
    ]
    topology = _topology_reference(topology_analysis_path)
    source_manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "eventForensic": _source_entry(
            event_forensic_path,
            role="accepted directional_event_insert identities and development-panel outcomes",
            report_sha256=forensic_hash,
        ),
        "multipanel": _source_entry(
            multipanel_path,
            role="selected retrospective cross-panel outcomes only",
            report_sha256=multipanel_hash,
        ),
        "evaluatedMembers": _source_entry(
            evaluated_members_path,
            role="frozen child/parent profile snapshots and binding provenance",
        ),
        "topology": topology,
        "prohibitedOperationsNotRun": [
            "strategy replay",
            "new child or event generation",
            "worker/gateway/Vast use",
            "calibration launch",
            "archive/support/direction/quality/risk/cost/operator policy change",
        ],
    }
    corpus = {
        "acceptedDirectionalEventInsertChildren": len(cases),
        "samePanelComparableChildren": len(comparable_cases),
        "samePanelUncomparableChildren": len(cases) - len(comparable_cases),
        "multipanelChildren": len(multi_children),
        "developmentPanelId": development_panel,
        "parentCount": len(parent_ids),
        "parentsWithEvaluatedMemberSnapshot": len(parent_records),
        "parentsWithoutEvaluatedMemberSnapshot": len(parent_ids) - len(parent_records),
        "componentCount": len(component_contexts),
    }
    same_panel = {
        "status": "descriptive_outcomes_only_not_surrogate_validation",
        "count": len(comparable_cases),
        "parentBeats": sum(bool(case["relative"].get("beatParent")) for case in comparable_cases),
        "parentLosses": sum(bool(case["relative"].get("lostToParent")) for case in comparable_cases),
        "fullEconomicPhenotypeTies": sum(
            bool(case["relative"].get("fullEconomicPhenotypeTie")) for case in comparable_cases
        ),
        "deltaGrossR": _quantiles(gross),
        "deltaModeledCostR": _quantiles(cost),
        "deltaNetR": _quantiles(net),
        "mechanismClassification": dict(sorted(mechanism_counts.items())),
        "outcomeOnlySpearman": {
            "deltaGrossR_vs_deltaModeledCostR": _spearman(gross, cost),
            "deltaGrossR_vs_deltaNetR": _spearman(gross, net),
            "deltaModeledCostR_vs_deltaNetR": _spearman(cost, net),
        },
        "withinParent": {
            parent_id: {
                "count": len(group),
                "deltaNetR": _quantiles([_metric_deltas(case)["deltaNetR"] for case in group]),
            }
            for parent_id, group in sorted(parent_groups.items())
        },
        "caveat": "These are selected development-panel outcome correlations, not associations between a pre-outcome component score and outcome.",
    }
    phenotype_dedup = {
        "acceptedGenotypeGroups": {
            key: sorted(str(case["candidateId"]) for case in group)
            for key, group in sorted(genotype_groups.items())
        },
        "resolvedProgramGroups": {
            key: sorted(str(case["candidateId"]) for case in group)
            for key, group in sorted(program_groups.items())
        },
        "realizedPhenotypeGroups": {
            key: sorted(str(case["candidateId"]) for case in group)
            for key, group in sorted(phenotype_groups.items())
        },
        "sensitivity": {
            "rawComparableCases": len(comparable_cases),
            "phenotypeDeduplicatedCases": len(representative_cases),
            "representativeRule": "lexicographically first candidateId within each realized phenotype; outcome-blind",
            "surrogateAssociationConclusion": "unchanged_no_preoutcome_score_available",
        },
    }
    cross_panel = {
        "status": "retrospective_selected_cross_panel_check_not_confirmation",
        "selectionBias": multipanel.get("selectionBiasCaveat"),
        "replicationRole": multipanel.get("replicationRole"),
        "developmentPanelId": development_panel,
        "replicationPanelIds": multipanel.get("replicationPanelIds"),
        "knownSummary": {
            key: multipanel.get(key)
            for key in (
                "sameChildAbsolutePositiveOnBothPanel1AndPanel2",
                "sameChildParentSuperiorOnBothPanel1AndPanel2",
                "sameChildRiskQualifiedParentSuperiorOnBothPanel1AndPanel2",
                "sameChildSupportAndDirectionOnBothPanel1AndPanel2",
                "childrenSurvivingFinalCumulativeArchive",
            )
        },
        "surrogateTest": "not_performed_no_frozen_score",
        "rows": cross_panel_rows,
    }
    surrogate_protocol = {
        "status": "frozen_but_not_computable",
        "outcomesUsedToDefineScores": False,
        "S0": "event density/opportunity retention from frozen event starts",
        "S1": "context-free directional forward response, MFE-MAE, breadth, and density penalty",
        "S2": "parent-entry-conditioned response where retained opportunity timestamps exist",
        "S3": "small pre-outcome combination of S1/S2 and retained parent phenotypes",
        "baselines": ["random within parent", "density alone", "parent baseline", "indicator-id priors"],
        "unavailableReason": "Frozen V38 records do not retain the necessary pre-outcome event or parent-opportunity series, and rebuilding it is out of authorization.",
    }
    leave_one_parent_out = {
        "status": "not_performed_no_frozen_score",
        "folds": [
            {"heldOutParentCandidateId": parent_id, "comparableCaseCount": len(group)}
            for parent_id, group in sorted(parent_groups.items())
        ],
        "reason": "No S0-S3 score exists to fit or evaluate without outcome leakage.",
    }
    statistics = {
        "status": "no_confirmatory_statistic_without_preoutcome_score",
        "independentUnitPlan": "cluster by parent phenotype; do not treat genotypes sharing a realized phenotype as independent",
        "availableDescriptiveUnits": {
            "rawComparableCases": len(comparable_cases),
            "realizedPhenotypes": len(phenotype_groups),
            "parents": len(parent_groups),
        },
        "permutations": "not_run_no_score",
        "clusterBootstrap": "not_run_no_score",
        "pValues": None,
        "reason": "Outcome-only calculations would not test the requested predictive association.",
    }
    decision = {
        "taxonomy": "insufficient_retrospective_evidence",
        "componentSelectionAuthorized": False,
        "reason": "No frozen, pre-outcome component score can be calculated under the authorized offline boundary.",
        "notAStandaloneProfitabilityClaim": True,
        "notASelectedPanelConfirmationClaim": True,
        "preserveV37ArchiveConclusion": "V37 archive terminalization remains complete and unchanged.",
        "nextProbeSpecificationOnly": {
            "authorizationRequired": True,
            "smallestUsefulCapture": "For a future frozen parent/event insertion cohort, persist exact event-start timestamps, event semantics/config snapshot, forward-return/MFE/MAE sidecars, parent retained entry-opportunity timestamps, and immutable source/catalog/evidence/cost receipts before child outcomes are inspected.",
            "notLaunched": True,
        },
    }
    summary = {
        "schemaVersion": SCHEMA_VERSION,
        "sources": source_manifest,
        "corpus": corpus,
        "samePanelDescriptive": same_panel,
        "decision": decision,
    }

    _write_json(output_dir / "corpus-manifest.json", source_manifest)
    _write_jsonl(output_dir / "accepted-directional-event-inserts.jsonl", accepted_rows)
    _write_jsonl(output_dir / "component-context-identities.jsonl", contexts)
    _write_jsonl(output_dir / "realized-outcomes.jsonl", outcome_rows)
    _write_json(output_dir / "phenotype-dedup.json", phenotype_dedup)
    _write_jsonl(output_dir / "component-evidence-availability.jsonl", component_evidence_rows)
    _write_json(output_dir / "frozen-surrogate-protocol.json", surrogate_protocol)
    _write_jsonl(output_dir / "frozen-surrogate-scores.jsonl", score_rows)
    _write_json(output_dir / "same-panel-descriptive.json", same_panel)
    _write_json(output_dir / "leave-one-parent-out.json", leave_one_parent_out)
    _write_json(output_dir / "cross-panel-retrospective.json", cross_panel)
    _write_json(output_dir / "factorial-reference.json", topology)
    _write_json(output_dir / "association-and-statistics.json", statistics)
    _write_json(output_dir / "decision.json", decision)
    _write_json(output_dir / "reproducibility.json", summary)
    (output_dir / "README.md").write_text(_markdown_report(summary), encoding="utf-8")
    (output_dir / "run.log").write_text(
        "offline component-surrogate validation completed\n"
        "no replay, generation, worker, gateway, Vast, calibration, or policy mutation was invoked\n",
        encoding="utf-8",
    )
    _write_checksums(output_dir)
    return summary


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-forensic", type=Path, required=True)
    parser.add_argument("--multipanel", type=Path, required=True)
    parser.add_argument("--evaluated-members", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--topology-analysis", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = run_validation(
        event_forensic_path=args.event_forensic,
        multipanel_path=args.multipanel,
        evaluated_members_path=args.evaluated_members,
        output_dir=args.output_dir,
        topology_analysis_path=args.topology_analysis,
    )
    print(canonical_json({"decision": summary["decision"]["taxonomy"], "corpus": summary["corpus"]}))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI boundary
    raise SystemExit(main())
