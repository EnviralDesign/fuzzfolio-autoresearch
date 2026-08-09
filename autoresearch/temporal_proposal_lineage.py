"""Immutable proposal-lineage attribution for temporal QD campaigns.

The QD controller deliberately does not import this module.  A campaign opts
in by emitting a sealed sidecar after its existing proposal, replay-behavior
and archive artifacts are complete.  That keeps historical campaigns readable
and makes this observer incapable of changing construction, scoring, archive
selection, or execution semantics.

The sidecar answers a question a fitness score cannot: did a material authored
mutation actually alter observed execution/liveness, realized behavior, or
archive retention?  Single-parent mutations with no change at any of those
boundaries are labelled *passenger candidates*, not automatically bad.  The
record is a measurement result, never a selection instruction.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256


PROPOSAL_LINEAGE_INPUT_SCHEMA = "temporal_proposal_lineage_input_v1"
PROPOSAL_LINEAGE_REPORT_SCHEMA = "temporal_proposal_lineage_report_v1"
PROPOSAL_LINEAGE_RECORD_SCHEMA = "temporal_proposal_lineage_record_v1"
PROPOSAL_LINEAGE_EVIDENCE_SCHEMA = "temporal_proposal_lineage_evidence_identity_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_RETENTION_OUTCOMES = frozenset({"retained", "not_retained", "not_evaluated", "not_eligible"})
_OBSERVED_COUNTERS = (
    "guardEvaluatedCount", "guardTrueCount", "transitionSelectedCount",
    "priorityShadowedCount", "actionScheduledCount", "actionAppliedCount",
    "actionRejectedCount", "actionCanceledCount", "activationCount",
    "acceptedIntentOrEffectCount", "rejectedIntentOrEffectCount",
    "positionChangeCount", "tradeCloseCount",
)


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be finite canonical JSON") from exc


def _sha(value: Any, *, name: str) -> str:
    text = str(value or "")
    if not _SHA.fullmatch(text):
        raise TemporalDiscoveryContractError(f"{name} must be a lowercase sha256 identity")
    return text


def _token(value: Any, *, name: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 240:
        raise TemporalDiscoveryContractError(f"{name} must be a nonempty explicit identifier")
    return text


def _count(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be a nonnegative integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be a nonnegative integer") from exc
    if number < 0:
        raise TemporalDiscoveryContractError(f"{name} must be a nonnegative integer")
    return number


def _generation(value: Any, *, name: str) -> int:
    return _count(value, name=name)


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def _rows(value: Any, *, name: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise TemporalDiscoveryContractError(f"{name} must be an array")
    return value


def _normalized_observed(value: Mapping[str, Any]) -> dict[str, Any]:
    """The liveness/execution surface is explicit and intentionally finite."""

    source = _mapping(value, name="proposal observed execution")
    counters_source = source.get("counters")
    if counters_source is not None and not isinstance(counters_source, Mapping):
        raise TemporalDiscoveryContractError("proposal observed execution counters must be an object")
    result = {
        "attributionSha256": _sha(source.get("attributionSha256"), name="observed attributionSha256"),
        "evidenceIdentity": _normalized_evidence_identity(
            _mapping(source.get("evidenceIdentity"), name="observed evidenceIdentity")
        ),
        "counters": {
            key: _count((counters_source or source).get(key, 0), name=f"observed {key}")
            for key in _OBSERVED_COUNTERS
        },
        "neverActivated": source.get("neverActivated") is True,
    }
    # A non-firing candidate cannot claim applied actions or a position change.
    if result["neverActivated"] and any(result["counters"][key] for key in ("activationCount", "acceptedIntentOrEffectCount", "positionChangeCount", "tradeCloseCount")):
        raise TemporalDiscoveryContractError("never-activated proposal has contradictory observed execution")
    result["observedSemanticSha256"] = canonical_sha256({"counters": result["counters"], "neverActivated": result["neverActivated"]})
    supplied = source.get("observedSemanticSha256")
    if supplied is not None and _sha(supplied, name="observed semantic SHA-256") != result["observedSemanticSha256"]:
        raise TemporalDiscoveryContractError("proposal observed execution semantic identity is stale")
    return result


def _normalized_evidence_identity(value: Mapping[str, Any]) -> dict[str, str]:
    """Freeze the only evidence dimensions allowed in a causal comparison."""

    source = _mapping(value, name="proposal evidence identity")
    result = {
        "schemaVersion": PROPOSAL_LINEAGE_EVIDENCE_SCHEMA,
        "panelWindowSetSha256": _sha(source.get("panelWindowSetSha256"), name="evidence panel/window-set SHA-256"),
        "costViewSha256": _sha(source.get("costViewSha256"), name="evidence cost-view SHA-256"),
        "observationStreamSetSha256": _sha(source.get("observationStreamSetSha256"), name="evidence observation-stream-set SHA-256"),
    }
    calculated = canonical_sha256(result)
    if _sha(source.get("canonicalEvidenceIdentitySha256"), name="canonical evidence identity SHA-256") != calculated:
        raise TemporalDiscoveryContractError("proposal canonical evidence identity is stale")
    result["canonicalEvidenceIdentitySha256"] = calculated
    return result


def _normalized_behavior(value: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(value, name="proposal realized behavior")
    # This identity is generated by temporal_realized_behavior.py and binds
    # directions, costs, execution/transition distributions and timing.
    return {
        "identitySha256": _sha(source.get("identitySha256"), name="realized behavior identitySha256"),
        "evidenceIdentity": _normalized_evidence_identity(
            _mapping(source.get("evidenceIdentity"), name="realized behavior evidenceIdentity")
        ),
    }


def _normalized_retention(value: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(value, name="proposal archive retention")
    outcome = _token(source.get("outcome"), name="archive retention outcome")
    if outcome not in _RETENTION_OUTCOMES:
        raise TemporalDiscoveryContractError("archive retention outcome is unsupported")
    result: dict[str, Any] = {
        "outcome": outcome,
        "retentionEvidenceSha256": _sha(source.get("retentionEvidenceSha256"), name="retention evidence SHA-256"),
    }
    member = source.get("archiveMemberIdentitySha256")
    if outcome == "retained":
        result["archiveMemberIdentitySha256"] = _sha(member, name="retained archive member identity")
    elif member is not None:
        # A non-retained candidate must not borrow a member identity from an
        # unrelated archive row.
        raise TemporalDiscoveryContractError("non-retained proposal may not carry an archive member identity")
    return result


def _normalized_operator(value: Any, *, parents: Sequence[str]) -> dict[str, Any] | None:
    if not parents:
        if value is not None:
            raise TemporalDiscoveryContractError("immigrant proposal must not carry a parent operator application")
        return None
    source = _mapping(value, name="proposal operator application")
    bound_parents = [_token(item, name="operator parent candidate ID") for item in _rows(source.get("parentCandidateIds"), name="operator parentCandidateIds")]
    if bound_parents != list(parents):
        raise TemporalDiscoveryContractError("operator application parent ordering disagrees with lineage")
    delta = _rows(source.get("semanticDelta"), name="operator semanticDelta")
    if not delta or not all(isinstance(item, Mapping) for item in delta):
        raise TemporalDiscoveryContractError("derived proposal requires a nonempty authored semanticDelta")
    result = {
        "operatorId": _token(source.get("operatorId"), name="operator ID"),
        "planSha256": _sha(source.get("planSha256"), name="operator plan SHA-256"),
        "applicationSha256": _sha(source.get("applicationSha256"), name="operator application SHA-256"),
        "parentCandidateIds": bound_parents,
        "semanticDelta": _clone(delta, name="operator semanticDelta"),
    }
    result["semanticDeltaSha256"] = canonical_sha256(result["semanticDelta"])
    return result


def _normalized_entry(value: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(value, name="proposal lineage entry")
    candidate_id = _token(source.get("candidateId"), name="candidate ID")
    parents = [_token(item, name="parent candidate ID") for item in _rows(source.get("parentCandidateIds", []), name="parentCandidateIds")]
    if len(set(parents)) != len(parents) or candidate_id in parents:
        raise TemporalDiscoveryContractError("proposal lineage parents must be unique and cannot self-reference")
    parent_programs = [_sha(item, name="parent program SHA-256") for item in _rows(source.get("parentProgramSha256s", []), name="parentProgramSha256s")]
    if len(parent_programs) != len(parents):
        raise TemporalDiscoveryContractError("proposal lineage parent program count disagrees with parents")
    origin = _token(source.get("originKind"), name="origin kind")
    if not parents and origin not in {"immigrant", "seed", "g0_immigrant"}:
        raise TemporalDiscoveryContractError("parentless proposal must identify an immigrant/seed origin")
    if parents and origin in {"immigrant", "seed", "g0_immigrant"}:
        raise TemporalDiscoveryContractError("derived proposal may not identify as an immigrant/seed")
    result = {
        "schemaVersion": PROPOSAL_LINEAGE_RECORD_SCHEMA,
        "candidateId": candidate_id,
        "candidateIdentitySha256": _sha(source.get("candidateIdentitySha256"), name="candidate identity SHA-256"),
        "programSha256": _sha(source.get("programSha256"), name="candidate program SHA-256"),
        "generationIndex": _generation(source.get("generationIndex", 0), name="generation index"),
        "originKind": origin,
        "parentCandidateIds": parents,
        "parentProgramSha256s": parent_programs,
    }
    result["operator"] = _normalized_operator(source.get("operator"), parents=parents)
    result["observedExecution"] = _normalized_observed(_mapping(source.get("observedExecution"), name="observedExecution"))
    result["realizedBehavior"] = _normalized_behavior(_mapping(source.get("realizedBehavior"), name="realizedBehavior"))
    if result["observedExecution"]["evidenceIdentity"] != result["realizedBehavior"]["evidenceIdentity"]:
        raise TemporalDiscoveryContractError("observed execution and realized behavior evidence identities disagree")
    result["archiveRetention"] = _normalized_retention(_mapping(source.get("archiveRetention"), name="archiveRetention"))
    return result


def _normalized_external_parent(value: Mapping[str, Any]) -> dict[str, Any]:
    """A sealed parent snapshot for a sliced campaign cohort.

    A caller can instead seal every generation in one full-run ancestry corpus.
    This explicit form is for a compact generation sidecar whose parent lives
    outside that current cohort; it cannot silently invent parent evidence.
    """

    source = _mapping(value, name="external parent evidence")
    result = {
        "candidateId": _token(source.get("candidateId"), name="external parent candidate ID"),
        "candidateIdentitySha256": _sha(source.get("candidateIdentitySha256"), name="external parent candidate identity SHA-256"),
        "programSha256": _sha(source.get("programSha256"), name="external parent program SHA-256"),
        "generationIndex": _generation(source.get("generationIndex"), name="external parent generation index"),
        "observedExecution": _normalized_observed(_mapping(source.get("observedExecution"), name="external parent observedExecution")),
        "realizedBehavior": _normalized_behavior(_mapping(source.get("realizedBehavior"), name="external parent realizedBehavior")),
        "archiveRetention": _normalized_retention(_mapping(source.get("archiveRetention"), name="external parent archiveRetention")),
    }
    if result["observedExecution"]["evidenceIdentity"] != result["realizedBehavior"]["evidenceIdentity"]:
        raise TemporalDiscoveryContractError("external parent observed/behavior evidence identities disagree")
    result["externalParentEvidenceSha256"] = canonical_sha256(result)
    supplied = source.get("externalParentEvidenceSha256")
    if supplied is not None and _sha(supplied, name="external parent evidence SHA-256") != result["externalParentEvidenceSha256"]:
        raise TemporalDiscoveryContractError("external parent evidence identity is stale")
    return result


def _check_parent_graph(entries: Mapping[str, Mapping[str, Any]], external: Mapping[str, Mapping[str, Any]]) -> None:
    """Prove all parents are in the sealed cohort, bound and acyclic."""

    for candidate_id, entry in entries.items():
        for parent_id, program in zip(entry["parentCandidateIds"], entry["parentProgramSha256s"], strict=True):
            parent = entries.get(parent_id) or external.get(parent_id)
            if parent is None:
                raise TemporalDiscoveryContractError("proposal lineage parent is absent from sealed cohort")
            if parent["programSha256"] != program:
                raise TemporalDiscoveryContractError("proposal lineage parent program identity is stale")
            if parent["generationIndex"] >= entry["generationIndex"]:
                raise TemporalDiscoveryContractError("proposal lineage generation ordering is cyclic or non-forward")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(candidate_id: str) -> None:
        if candidate_id in visiting:
            raise TemporalDiscoveryContractError("proposal lineage contains a parent cycle")
        if candidate_id in visited:
            return
        visiting.add(candidate_id)
        for parent_id in entries[candidate_id]["parentCandidateIds"]:
            if parent_id in entries:
                visit(parent_id)
        visiting.remove(candidate_id)
        visited.add(candidate_id)

    for candidate_id in sorted(entries):
        visit(candidate_id)


def seal_proposal_lineage_inputs(
    entries: Sequence[Mapping[str, Any]], *, external_parent_evidence: Sequence[Mapping[str, Any]] = ()
) -> dict[str, Any]:
    """Normalize and seal a new-campaign lineage sidecar.

    Sealing is deliberately explicit.  No legacy run is scanned, rewritten or
    made to pretend it recorded observability it did not have.
    """

    if not entries:
        raise TemporalDiscoveryContractError("proposal lineage requires at least one entry")
    normalized = [_normalized_entry(_mapping(entry, name="proposal lineage entry")) for entry in entries]
    by_id = {entry["candidateId"]: entry for entry in normalized}
    if len(by_id) != len(normalized):
        raise TemporalDiscoveryContractError("proposal lineage candidate IDs must be unique")
    normalized_external = [
        _normalized_external_parent(_mapping(entry, name="external parent evidence"))
        for entry in external_parent_evidence
    ]
    external_by_id = {entry["candidateId"]: entry for entry in normalized_external}
    if len(external_by_id) != len(normalized_external):
        raise TemporalDiscoveryContractError("external parent evidence candidate IDs must be unique")
    if set(external_by_id) & set(by_id):
        raise TemporalDiscoveryContractError("external parent evidence duplicates a cohort candidate")
    _check_parent_graph(by_id, external_by_id)
    payload = {
        "schemaVersion": PROPOSAL_LINEAGE_INPUT_SCHEMA,
        "records": [by_id[candidate_id] for candidate_id in sorted(by_id)],
        "externalParentEvidence": [external_by_id[candidate_id] for candidate_id in sorted(external_by_id)],
    }
    payload["inputSha256"] = canonical_sha256(payload)
    return payload


def _verified_sealed_input(value: Mapping[str, Any]) -> dict[str, Any]:
    source = _clone(_mapping(value, name="proposal lineage input"), name="proposal lineage input")
    supplied = _sha(source.pop("inputSha256", None), name="proposal lineage input SHA-256")
    if source.get("schemaVersion") != PROPOSAL_LINEAGE_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("proposal lineage input schema is unsupported")
    if canonical_sha256(source) != supplied:
        raise TemporalDiscoveryContractError("proposal lineage input is stale or tampered")
    # Re-seal revalidates every nested identity and rejects noncanonical order.
    replay = seal_proposal_lineage_inputs(
        source.get("records") or [],
        external_parent_evidence=source.get("externalParentEvidence") or [],
    )
    if replay != {**source, "inputSha256": supplied}:
        raise TemporalDiscoveryContractError("proposal lineage input is not canonical")
    return replay


def _comparative_record(
    entry: Mapping[str, Any],
    entries: Mapping[str, Mapping[str, Any]],
    external: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    parents = [entries[parent_id] if parent_id in entries else external[parent_id] for parent_id in entry["parentCandidateIds"]]
    observed = entry["observedExecution"]
    behavior = entry["realizedBehavior"]
    retention = entry["archiveRetention"]
    parent_sources = ["cohort" if parent_id in entries else "external_parent_evidence" for parent_id in entry["parentCandidateIds"]]
    if len(parents) != 1:
        classification = "not_assessable_multi_parent" if parents else "not_applicable_immigrant"
        comparison = {
            "parentCount": len(parents),
            "parentEvidenceSources": parent_sources,
            "observedExecutionChanged": None,
            "realizedBehaviorChanged": None,
            "archiveRetentionChanged": None,
            "passengerMutation": None,
            "classification": classification,
        }
    else:
        parent = parents[0]
        evidence_comparable = observed["evidenceIdentity"] == parent["observedExecution"]["evidenceIdentity"]
        comparison = {
            "parentCount": 1,
            "parentCandidateId": parent["candidateId"],
            "parentEvidenceSources": parent_sources,
            "evidenceComparable": evidence_comparable,
        }
        if not evidence_comparable:
            comparison.update({
                "observedExecutionChanged": None,
                "realizedBehaviorChanged": None,
                "archiveRetentionChanged": None,
                "passengerMutation": None,
                "classification": "not_assessable_evidence_changed",
            })
        else:
            observed_changed = observed["observedSemanticSha256"] != parent["observedExecution"]["observedSemanticSha256"]
            behavior_changed = behavior["identitySha256"] != parent["realizedBehavior"]["identitySha256"]
            retention_changed = retention["outcome"] != parent["archiveRetention"]["outcome"]
            passenger = not observed_changed and not behavior_changed and not retention_changed
            comparison.update({
                "observedExecutionChanged": observed_changed,
                "realizedBehaviorChanged": behavior_changed,
                "archiveRetentionChanged": retention_changed,
                "passengerMutation": passenger,
                "classification": "passenger_candidate" if passenger else "measured_effect",
            })
    record = {
        "schemaVersion": PROPOSAL_LINEAGE_RECORD_SCHEMA,
        "candidateId": entry["candidateId"],
        "candidateIdentitySha256": entry["candidateIdentitySha256"],
        "programSha256": entry["programSha256"],
        "generationIndex": entry["generationIndex"],
        "originKind": entry["originKind"],
        "parentCandidateIds": entry["parentCandidateIds"],
        "parentProgramSha256s": entry["parentProgramSha256s"],
        "operator": entry["operator"],
        "authoredSemanticDeltaSha256": entry["operator"]["semanticDeltaSha256"] if entry["operator"] is not None else None,
        "observedExecution": observed,
        "realizedBehavior": behavior,
        "archiveRetention": retention,
        "comparison": comparison,
    }
    record["recordSha256"] = canonical_sha256(record)
    return record


def _operator_summary(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for record in records:
        operator = record.get("operator")
        if not isinstance(operator, Mapping):
            continue
        operator_id = str(operator["operatorId"])
        bucket = buckets.setdefault(operator_id, {"operatorId": operator_id, "derivedProposalCount": 0, "singleParentMeasuredCount": 0, "passengerCandidateCount": 0, "observedExecutionChangedCount": 0, "realizedBehaviorChangedCount": 0, "archiveRetentionChangedCount": 0})
        bucket["derivedProposalCount"] += 1
        comparison = record["comparison"]
        if comparison["parentCount"] == 1:
            bucket["singleParentMeasuredCount"] += 1
            for field, count in (("passengerMutation", "passengerCandidateCount"), ("observedExecutionChanged", "observedExecutionChangedCount"), ("realizedBehaviorChanged", "realizedBehaviorChangedCount"), ("archiveRetentionChanged", "archiveRetentionChangedCount")):
                if comparison[field] is True:
                    bucket[count] += 1
    result = []
    for operator_id, bucket in sorted(buckets.items()):
        denominator = bucket["singleParentMeasuredCount"]
        result.append({
            **bucket,
            "passengerCandidateRate": bucket["passengerCandidateCount"] / denominator if denominator else None,
        })
    return result


def build_proposal_lineage_report(sealed_input: Mapping[str, Any]) -> dict[str, Any]:
    """Build a deterministic, immutable diagnostic report from a sealed sidecar."""

    sealed = _verified_sealed_input(sealed_input)
    entries = {str(entry["candidateId"]): entry for entry in sealed["records"]}
    external = {str(entry["candidateId"]): entry for entry in sealed["externalParentEvidence"]}
    records = [_comparative_record(entries[candidate_id], entries, external) for candidate_id in sorted(entries)]
    summary = {
        "candidateCount": len(records),
        "derivedProposalCount": sum(record["operator"] is not None for record in records),
        "singleParentMeasuredCount": sum(record["comparison"]["parentCount"] == 1 for record in records),
        "passengerCandidateCount": sum(record["comparison"]["classification"] == "passenger_candidate" for record in records),
        "multiParentNotAssessableCount": sum(record["comparison"]["classification"] == "not_assessable_multi_parent" for record in records),
        "evidenceChangedNotAssessableCount": sum(record["comparison"]["classification"] == "not_assessable_evidence_changed" for record in records),
        "externalParentEvidenceCount": len(external),
    }
    summary["passengerCandidateRate"] = summary["passengerCandidateCount"] / summary["singleParentMeasuredCount"] if summary["singleParentMeasuredCount"] else None
    report = {
        "schemaVersion": PROPOSAL_LINEAGE_REPORT_SCHEMA,
        "inputSha256": sealed["inputSha256"],
        "records": records,
        "operatorSummaries": _operator_summary(records),
        "summary": summary,
        "selectionImpact": "none_observation_only",
    }
    report["reportSha256"] = canonical_sha256(report)
    return report


def verify_proposal_lineage_report(sealed_input: Mapping[str, Any], report: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed if either report or its sealed input has drifted."""

    value = _clone(_mapping(report, name="proposal lineage report"), name="proposal lineage report")
    supplied = _sha(value.pop("reportSha256", None), name="proposal lineage report SHA-256")
    if value.get("schemaVersion") != PROPOSAL_LINEAGE_REPORT_SCHEMA or canonical_sha256(value) != supplied:
        raise TemporalDiscoveryContractError("proposal lineage report is stale or tampered")
    expected = build_proposal_lineage_report(sealed_input)
    if expected != {**value, "reportSha256": supplied}:
        raise TemporalDiscoveryContractError("proposal lineage report does not replay from its sealed input")
    return {"allChecksPassed": True, "inputSha256": expected["inputSha256"], "reportSha256": supplied}


__all__ = [
    "PROPOSAL_LINEAGE_INPUT_SCHEMA",
    "PROPOSAL_LINEAGE_EVIDENCE_SCHEMA",
    "PROPOSAL_LINEAGE_RECORD_SCHEMA",
    "PROPOSAL_LINEAGE_REPORT_SCHEMA",
    "build_proposal_lineage_report",
    "seal_proposal_lineage_inputs",
    "verify_proposal_lineage_report",
]
