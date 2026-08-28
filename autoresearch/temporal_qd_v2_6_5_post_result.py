"""Build the no-market V2.6.5 topology result decision package.

The input is the sealed V2.6.3 salvage ZIP.  This module only reads the
authenticated reducer result and already-retained compiled profiles; it never
opens a market replay, changes a candidate, or writes to an archive.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_json, canonical_sha256


SCHEMA_TERMINAL = "temporal_qd_topology_inspected_terminal_decision_v1"
SCHEMA_FORENSIC = "temporal_qd_topology_setup_mechanism_forensic_v1"
SCHEMA_EVENT_DESIGN = "temporal_qd_event_only_support_calibration_design_v1"
SCHEMA_TOPOLOGY_DESIGN = "temporal_qd_bounded_topology_nursery_design_v1"
ANALYSIS_ENTRY = "salvage/reducer/pass-1.json"
VALIDATION_ENTRY = "salvage/reducer/reducer-validation.json"
POPULATION_ENTRY = "sealed-run/inputs/panel-1/cohort-population.json"
PANELS = ("panel-1", "panel-2", "panel-3")
ARMS = ("P", "T", "E", "TE")


class PostResultError(ValueError):
    """The sealed result cannot support a V2.6.5 decision package."""


def _raw_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _read_entry(archive: zipfile.ZipFile, name: str) -> tuple[dict[str, Any], str]:
    try:
        raw = archive.read(name)
    except KeyError as exc:
        raise PostResultError(f"sealed result is missing required entry: {name}") from exc
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise PostResultError(f"sealed result entry must be an object: {name}")
    return value, _raw_bytes(raw)


def _verify_self_hash(value: Mapping[str, Any], field: str, label: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field, None)
    if stored != canonical_sha256(unsigned):
        raise PostResultError(f"{label} self-hash mismatch")


def _contains_guard_kind(value: Any, kind: str) -> bool:
    if isinstance(value, Mapping):
        return value.get("kind") == kind or any(
            _contains_guard_kind(item, kind) for item in value.values()
        )
    if isinstance(value, list):
        return any(_contains_guard_kind(item, kind) for item in value)
    return False


def _profile_index(population: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    candidates = population.get("candidates")
    if not isinstance(candidates, list):
        raise PostResultError("sealed population has no candidates list")
    result: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise PostResultError("sealed population candidate is invalid")
        candidate_id = candidate.get("candidateId")
        profile = candidate.get("sourceProfile")
        if not isinstance(candidate_id, str) or not isinstance(profile, Mapping):
            raise PostResultError("sealed population candidate profile is unavailable")
        result[candidate_id] = profile
    return result


def _arm_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    support = row.get("supportEligibility")
    quality = row.get("qualityLaneEligibility")
    direction = row.get("directionSelection")
    return {
        "candidateId": row.get("candidateId"),
        "conservativeNetR": row.get("conservativeNetR"),
        "tradeCount": row.get("tradeCount"),
        "costDragR": row.get("costDragR"),
        "support": {
            "eligible": support.get("eligible") if isinstance(support, Mapping) else None,
            "reasonCodes": support.get("reasonCodes") if isinstance(support, Mapping) else None,
        },
        "quality": {
            "eligible": quality.get("eligible") if isinstance(quality, Mapping) else None,
            "reasonCode": quality.get("reasonCode") if isinstance(quality, Mapping) else None,
        },
        "direction": {
            "eligible": direction.get("eligible") if isinstance(direction, Mapping) else None,
            "reasonCode": direction.get("reasonCode") if isinstance(direction, Mapping) else None,
        },
    }


def _entry_sequence_summary(value: Any) -> Any:
    """Bind retained exact sequences without copying raw trade records forward."""

    if not isinstance(value, Mapping):
        return value
    result: dict[str, Any] = {}
    for key, item in value.items():
        if key in {"candidateEntrySequence", "parentEntrySequence"} and isinstance(item, list):
            result[f"{key}Count"] = len(item)
            result[f"{key}Sha256"] = canonical_sha256(item)
        else:
            result[key] = item
    return result


def _mechanism_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "addedSetupNodeOccupancy",
        "barsInAddedSetup",
        "transitionPathThroughAddedSetup",
        "changedSideTransitionDistribution",
        "closedTradeCountChangeVersusP",
        "costDragRChangeVersusP",
        "entrySequenceComparison",
        "eventFreshness",
        "eventSpecificActivation",
    )
    result = {key: row.get(key) for key in keys}
    result["entrySequenceComparison"] = _entry_sequence_summary(result["entrySequenceComparison"])
    return result


def _setup_semantics(profile: Mapping[str, Any], node_ids: list[str]) -> list[dict[str, Any]]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise PostResultError("compiled profile graph is unavailable")
    states = graph.get("states")
    transitions = graph.get("transitions")
    if not isinstance(states, list) or not isinstance(transitions, list):
        raise PostResultError("compiled profile graph topology is unavailable")
    states_by_id = {state.get("id"): state for state in states if isinstance(state, Mapping)}
    result: list[dict[str, Any]] = []
    for node_id in node_ids:
        state = states_by_id.get(node_id)
        if not isinstance(state, Mapping):
            raise PostResultError(f"compiled profile is missing added setup node: {node_id}")
        incoming = [
            transition
            for transition in transitions
            if isinstance(transition, Mapping) and transition.get("destinationStateId") == node_id
        ]
        outgoing = [
            transition
            for transition in transitions
            if isinstance(transition, Mapping) and transition.get("sourceStateId") == node_id
        ]
        transition_view = lambda transition: {
            "id": transition.get("id"),
            "sourceStateId": transition.get("sourceStateId"),
            "destinationStateId": transition.get("destinationStateId"),
            "priority": transition.get("priority"),
            "reasonCode": transition.get("reasonCode"),
            "guard": transition.get("guard"),
        }
        result.append(
            {
                "nodeId": node_id,
                "timeoutBars": state.get("timeoutBars"),
                "incomingTransitions": [transition_view(item) for item in incoming],
                "outgoingTransitions": [transition_view(item) for item in outgoing],
                "outgoingRequiresFreshEvent": bool(outgoing)
                and all(_contains_guard_kind(item.get("guard"), "fresh_event") for item in outgoing),
                "sourceLevelFinding": (
                    "The compiled setup state has no timeout and every outgoing transition "
                    "requires a fresh_event guard. This proves the absence of a structural "
                    "timeout/fallback exit; it does not prove how often the guard was evaluated."
                ),
            }
        )
    return result


def _block_forensic(
    block_id: str, block: Mapping[str, Any], profiles: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    arms = block.get("arms")
    reports = block.get("panelReports")
    if not isinstance(arms, Mapping) or set(arms) != set(ARMS):
        raise PostResultError(f"block arm set drifted: {block_id}")
    if not isinstance(reports, Mapping) or set(reports) != set(PANELS):
        raise PostResultError(f"block panel set drifted: {block_id}")
    panel_rows: dict[str, Any] = {}
    te_occupancies: list[float] = []
    te_trade_count = 0
    t_over_p: list[float] = []
    event_increment = 0.0
    event_gain = 0.0
    event_trade_count = 0
    event_evidence_unavailable = True
    setup_node_ids: list[str] | None = None
    for panel_id in PANELS:
        report = reports[panel_id]
        if not isinstance(report, Mapping) or report.get("usefulProgressiveInnovationV2") is not False:
            raise PostResultError(f"panel result is not a complete negative: {block_id}/{panel_id}")
        panel_arms = report.get("arms")
        mechanism = report.get("mechanism")
        if not isinstance(panel_arms, Mapping) or not isinstance(mechanism, Mapping):
            raise PostResultError(f"panel mechanism is unavailable: {block_id}/{panel_id}")
        te = panel_arms.get("TE")
        if not isinstance(te, Mapping):
            raise PostResultError(f"TE arm is unavailable: {block_id}/{panel_id}")
        te_trade_count += int(te.get("tradeCount") or 0)
        event_trade_count += int(panel_arms["E"].get("tradeCount") or 0)
        t_over_p.append(float(panel_arms["T"]["conservativeNetR"]) - float(panel_arms["P"]["conservativeNetR"]))
        event_increment += float(panel_arms["TE"]["conservativeNetR"]) - float(panel_arms["E"]["conservativeNetR"])
        event_gain += float(panel_arms["E"]["conservativeNetR"]) - float(panel_arms["P"]["conservativeNetR"])
        te_mechanism = mechanism.get("TE")
        if not isinstance(te_mechanism, Mapping):
            raise PostResultError(f"TE mechanism is unavailable: {block_id}/{panel_id}")
        occupancy = te_mechanism.get("addedSetupNodeOccupancy")
        if isinstance(occupancy, Mapping) and occupancy.get("status") == "available":
            values = occupancy.get("values")
            if isinstance(values, Mapping):
                te_occupancies.extend(float(value) for value in values.values())
        activation = te_mechanism.get("eventSpecificActivation")
        event_evidence_unavailable = event_evidence_unavailable and isinstance(activation, Mapping) and activation.get("status") == "unavailable"
        current_nodes = report.get("mechanism", {}).get("addedSetupNodeIds")
        if isinstance(current_nodes, list):
            if setup_node_ids is None:
                setup_node_ids = [str(item) for item in current_nodes]
            elif setup_node_ids != [str(item) for item in current_nodes]:
                raise PostResultError(f"added setup identity drifted: {block_id}")
        panel_rows[panel_id] = {
            "arms": {arm: _arm_summary(panel_arms[arm]) for arm in ARMS},
            "mechanism": {arm: _mechanism_summary(mechanism[arm]) for arm in ARMS},
            "usefulProgressiveInnovationV2": False,
        }
    candidate_id = arms["TE"]
    if not isinstance(candidate_id, str) or candidate_id not in profiles or setup_node_ids is None:
        raise PostResultError(f"TE source profile is unavailable: {block_id}")
    labels = []
    if te_occupancies and min(te_occupancies) >= 0.99 and te_trade_count <= 1:
        labels.append("setup_state_near_absorbing")
    if all(value > 0.0 for value in t_over_p):
        labels.append("topology_only_mildly_helpful")
    if abs(event_increment) <= 0.25 and event_gain > 0.0 and event_trade_count > 0:
        labels.append("event_filter_dominates_topology")
    if event_evidence_unavailable:
        labels.append("unknown_due_missing_event_guard_evidence")
    return {
        "blockId": block_id,
        "changedSide": block.get("changedSide"),
        "arms": dict(arms),
        "labels": labels,
        "descriptiveTotals": {
            "topologyMinusParentNetRByPanel": dict(zip(PANELS, t_over_p)),
            "topologyPlusEventMinusEventNetR": event_increment,
            "eventMinusParentNetR": event_gain,
            "eventTradeCount": event_trade_count,
            "topologyPlusEventTradeCount": te_trade_count,
        },
        "sourceSemantics": _setup_semantics(profiles[candidate_id], setup_node_ids),
        "panelEvidence": panel_rows,
    }


def _event_source(profile: Mapping[str, Any], side: str) -> dict[str, Any]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise PostResultError("event source graph is unavailable")
    bindings = graph.get("eventBindings")
    indicators = profile.get("indicators")
    transitions = graph.get("transitions")
    if not isinstance(bindings, list) or not isinstance(indicators, list) or not isinstance(transitions, list):
        raise PostResultError("event source is incomplete")
    binding = next(
        (
            item
            for item in bindings
            if isinstance(item, Mapping) and str(item.get("id", "")).startswith(f"{side}_")
        ),
        None,
    )
    if not isinstance(binding, Mapping):
        raise PostResultError("changed-side event binding is unavailable")
    instance_id = binding.get("indicatorInstanceId")
    indicator = next(
        (item for item in indicators if isinstance(item, Mapping) and item.get("meta", {}).get("instanceId") == instance_id),
        None,
    )
    if not isinstance(indicator, Mapping):
        raise PostResultError("event indicator is unavailable")
    event_id = binding.get("id")
    guarded = [
        item
        for item in transitions
        if isinstance(item, Mapping) and _contains_guard_kind(item.get("guard"), "fresh_event")
        and event_id in canonical_json(item.get("guard"))
    ]
    return {
        "binding": dict(binding),
        "indicator": {
            "baseIndicatorId": indicator.get("meta", {}).get("baseIndicatorId"),
            "id": indicator.get("meta", {}).get("id"),
            "config": indicator.get("config"),
        },
        "guardedTransitionIds": [item.get("id") for item in guarded],
    }


def _require_calibration_baseline(event_source: Mapping[str, Any]) -> None:
    indicator = event_source.get("indicator")
    if not isinstance(indicator, Mapping) or indicator.get("baseIndicatorId") != "CHANNEL":
        raise PostResultError("the frozen sparse-event source is not the expected CHANNEL primitive")
    config = indicator.get("config")
    if not isinstance(config, Mapping) or config.get("lookbackBars") != 1:
        raise PostResultError("the frozen sparse-event persistence baseline drifted")
    talib = config.get("talibConfig")
    if not isinstance(talib, list) or not any(
        isinstance(item, Mapping) and item.get("name") == "timeperiod" and item.get("value") == 20
        for item in talib
    ):
        raise PostResultError("the frozen sparse-event channel baseline drifted")


def _seal(value: dict[str, Any], field: str) -> dict[str, Any]:
    value[field] = canonical_sha256(value)
    return value


def build_post_result_artifacts(sealed_zip: Path) -> dict[str, Any]:
    """Derive the V2.6.5 artifacts from a sealed, already-authenticated result."""

    sealed_zip = Path(sealed_zip)
    if not sealed_zip.is_file():
        raise PostResultError(f"sealed result ZIP is missing: {sealed_zip}")
    with zipfile.ZipFile(sealed_zip) as archive:
        analysis, analysis_raw_sha = _read_entry(archive, ANALYSIS_ENTRY)
        validation, validation_raw_sha = _read_entry(archive, VALIDATION_ENTRY)
        population, population_raw_sha = _read_entry(archive, POPULATION_ENTRY)
    _verify_self_hash(analysis, "analysisSha256", "authenticated analysis")
    if (
        analysis.get("schemaVersion") != "temporal_qd_topology_production_analysis_v3"
        or analysis.get("status") != "complete"
        or validation.get("status") != "complete"
        or validation.get("analysisSha256") != analysis.get("analysisSha256")
        or validation.get("byteIdenticalSecondPass") is not True
    ):
        raise PostResultError("sealed reducer result is not complete and byte-identical")
    blocks = analysis.get("blocks")
    if not isinstance(blocks, Mapping) or len(blocks) != 3:
        raise PostResultError("authenticated analysis must contain exactly three blocks")
    profiles = _profile_index(population)
    forensic_blocks = [_block_forensic(block_id, blocks[block_id], profiles) for block_id in sorted(blocks)]
    source = {
        "sealedZipRawSha256": _raw_bytes(sealed_zip.read_bytes()),
        "entries": {
            ANALYSIS_ENTRY: analysis_raw_sha,
            VALIDATION_ENTRY: validation_raw_sha,
            POPULATION_ENTRY: population_raw_sha,
        },
        "authenticatedAnalysisSha256": analysis["analysisSha256"],
        "reducerValidationSha256": validation.get("reducerValidationSha256"),
    }
    terminal = _seal(
        {
            "schemaVersion": SCHEMA_TERMINAL,
            "status": "complete_negative_inspected_result",
            "source": source,
            "analysisStatus": "complete",
            "completeBlockCount": 3,
            "inspectedPromisingBlockCount": 0,
            "confirmationCandidateBlockCount": 0,
            "projectedConfirmationTaskCount": 0,
            "confirmationExecutionAuthorized": False,
            "resourceStudyAuthorized": False,
            "generationAuthorized": False,
            "familyLevelInferencePermitted": False,
            "productionConfirmed": False,
            "originalScientificContractSha256": analysis.get("originalScientificContractSha256"),
            "resultIndependentConfirmationRuleSha256": analysis.get("originalReplicationRuleSha256"),
            "untouchedConfirmationStatus": analysis.get("untouchedConfirmationStatus"),
            "blockIds": [item["blockId"] for item in forensic_blocks],
            "finding": (
                "The three inspected insert_setup plus matched-event cases are a complete "
                "negative result under their frozen rule. This does not make a family-wide "
                "claim about topology evolution."
            ),
        },
        "terminalDecisionSha256",
    )
    forensic = _seal(
        {
            "schemaVersion": SCHEMA_FORENSIC,
            "status": "derived_from_retained_authenticated_evidence",
            "source": source,
            "taxonomyRules": {
                "setup_state_near_absorbing": "all TE added-setup occupancies are at least 0.99 and TE has at most one trade across panels",
                "topology_only_mildly_helpful": "T exceeds P in every panel, descriptive only and not a quality qualification",
                "event_filter_dominates_topology": "absolute summed TE-minus-E net is at most 0.25R while summed E-minus-P net and E trade count are positive; descriptive only",
                "unknown_due_missing_event_guard_evidence": "retained attribution says event-specific activation is unavailable",
            },
            "blocks": forensic_blocks,
            "limits": [
                "Transition distributions prove retained route mass, not individual fresh-event guard evaluations.",
                "Source graph structure proves the absence of a timeout/fallback edge in these TE programs, not a runtime cause beyond the retained evidence.",
                "No replay, market evaluation, candidate rewrite, or result reinterpretation occurred.",
            ],
        },
        "mechanismForensicSha256",
    )
    sparse_block = next(item for item in forensic_blocks if "qd_69e5a3407ab21e82d787eb48c8d5" in item["blockId"])
    event_profile = profiles[sparse_block["arms"]["E"]]
    event_source = _event_source(event_profile, str(sparse_block["changedSide"]))
    _require_calibration_baseline(event_source)
    event_design = _seal(
        {
            "schemaVersion": SCHEMA_EVENT_DESIGN,
            "status": "design_only_not_launched",
            "source": source,
            "parentCandidateId": "qd_69e5a3407ab21e82d787eb48c8d5",
            "baselineEventCandidateId": sparse_block["arms"]["E"],
            "changedSide": sparse_block["changedSide"],
            "exactEventSource": event_source,
            "variants": [
                {"id": "P", "kind": "exact_parent_clone", "changes": {}},
                {"id": "E0", "kind": "exact_event_baseline", "changes": {}},
                {"id": "E1", "kind": "event_persistence", "changes": {"lookbackBars": {"from": 1, "to": 2}}},
                {"id": "E2", "kind": "event_parameter", "changes": {"channel.timeperiod": {"from": 20, "to": 10}}},
                {"id": "E3", "kind": "event_parameter", "changes": {"channel.timeperiod": {"from": 20, "to": 55}}},
            ],
            "panelPlan": {
                "developmentPanel": "panel-1",
                "replicationPanels": ["panel-2", "panel-3"],
                "windowsPerPanel": 4,
                "allVariantsRunOnAllPanels": True,
                "derivedCandidateCount": 5,
                "derivedTaskCount": 60,
            },
            "frozenSemantics": [
                "No topology mutation in this lane.",
                "Support, quality, direction, and risk semantics remain unchanged.",
                "No fixed R margin, population increase, or automatic production advancement is introduced.",
                "Any future confirmation remains independent and separately authorized.",
            ],
        },
        "designSha256",
    )
    topology_block = next(item for item in forensic_blocks if "qd_ed27f99ba0a8dfd7c76c69687efb" in item["blockId"])
    topology_design = _seal(
        {
            "schemaVersion": SCHEMA_TOPOLOGY_DESIGN,
            "status": "design_only_not_implemented_or_launched",
            "source": source,
            "parentCandidateId": "qd_ed27f99ba0a8dfd7c76c69687efb",
            "changedSide": topology_block["changedSide"],
            "motif": {
                "name": "bounded_setup_rearm",
                "basis": "The current insert_setup splits a pre-position edge but the observed TE setup has no timeout and no outgoing transition without fresh_event.",
                "requiredStructure": [
                    "Keep the original guarded pre-position path into the added setup state.",
                    "Keep the event-plus-evidence advance path from setup to the original downstream target.",
                    "Add one explicit bounded timeout/rearm fallback from setup to the pre-position safe state.",
                    "Do not use the current insert_timeout_rearm alone: its existing implementation only appends a recovery node and does not wire an escape edge.",
                ],
                "notAuthorized": "This is a future source-grounded specification, not a new operator implementation or market study.",
            },
            "requiredNoMarketLivenessChecks": [
                "Static reachability from the original source through both normal advance and fallback paths.",
                "No setup dead end and bounded residence through the explicit fallback.",
                "Deterministic synthetic event-present sequence reaches the original downstream target.",
                "Deterministic event-absent sequence reaches the fallback safe state.",
                "Deterministic fresh-event-without-evidence sequence reaches the fallback safe state.",
            ],
            "futureFactorial": ["P", "T2", "E", "T2E"],
            "futureStudyBoundary": [
                "The factorial must be one structural change relative to the exact parent.",
                "No hidden pruning, rescue, or automatic breeding after results.",
                "Market task cost is intentionally not claimed until an executable, source-valid task matrix exists.",
            ],
            "ranking": {
                "eventOnlyCalibration": {"expectedQualityImpact": "higher", "informationGain": "high", "implementationRisk": "lower"},
                "boundedTopologyNursery": {"expectedQualityImpact": "uncertain", "informationGain": "high", "implementationRisk": "higher"},
                "recommendation": "Event-only calibration first unless a later no-market source test proves a simple correctness defect.",
            },
        },
        "designSha256",
    )
    return {
        "source-evidence-manifest.json": _seal({"schemaVersion": "temporal_qd_v2_6_5_source_evidence_manifest_v1", "source": source}, "manifestSha256"),
        "terminal-inspected-decision.json": terminal,
        "topology-mechanism-forensic.json": forensic,
        "event-only-support-calibration-design.json": event_design,
        "bounded-topology-nursery-design.json": topology_design,
    }


def _memo(artifacts: Mapping[str, Mapping[str, Any]]) -> str:
    terminal = artifacts["terminal-inspected-decision.json"]
    forensic = artifacts["topology-mechanism-forensic.json"]
    return "\n".join(
        [
            "# Post-inspected topology decision",
            "",
            "The sealed 144-task topology study is complete and valid. None of its three exact `insert_setup` + matched-event blocks qualified on any panel, so it produces no confirmation cohort and authorizes no resource study, generation, or archive action.",
            "",
            "This is a narrow result: it rejects useful co-adaptation for these three frozen cases. It does not prove that all structural evolution is impossible.",
            "",
            "The forensic distinguishes observed behavior from mechanism certainty. Both short TE cases spent at least 99% of retained occupancy in the added setup state with zero or one total trade. The compiled source graphs show no timeout and no outgoing transition from that setup without `fresh_event`; retained output does not count fresh-event guard evaluations, so the exact runtime activation cause remains unproven.",
            "",
            "The next quality study is event-only support calibration: the sparse event signal remains separate from topology, with exact parent clones and one parameter change at a time. The topology lane remains a no-market nursery specification until a bounded setup/fallback structure passes static and synthetic liveness checks.",
            "",
            f"Authenticated analysis: `{terminal['source']['authenticatedAnalysisSha256']}`",
            f"Terminal decision: `{terminal['terminalDecisionSha256']}`",
            f"Mechanism forensic: `{forensic['mechanismForensicSha256']}`",
            "",
        ]
    )


def write_post_result_artifacts(*, sealed_zip: Path, output_dir: Path) -> dict[str, Any]:
    artifacts = build_post_result_artifacts(sealed_zip)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in artifacts.items():
        (output_dir / name).write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")
    (output_dir / "human-decision-memo.md").write_text(_memo(artifacts), encoding="utf-8", newline="\n")
    checksums = [
        f"{_raw_bytes((output_dir / name).read_bytes()).removeprefix('sha256:')}  {name}"
        for name in sorted((*artifacts, "human-decision-memo.md"))
    ]
    (output_dir / "checksums.sha256").write_text("\n".join(checksums) + "\n", encoding="utf-8", newline="\n")
    return artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sealed-result-zip", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    write_post_result_artifacts(sealed_zip=args.sealed_result_zip, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
