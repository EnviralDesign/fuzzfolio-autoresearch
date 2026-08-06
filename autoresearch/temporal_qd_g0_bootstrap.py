"""Verified, pre-economic G0 bootstrap selection.

This boundary has no constructor for validation authority.  It consumes an
immutable accepted pair-generation journal entry, rehydrates its ``FrozenPair``
and candidate identities, recomputes native/static proof projections, then
selects a first-pass market cohort.  Every constructed candidate remains in the
ledger even if not selected for market evidence.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
import heapq
import json
from pathlib import Path, PurePosixPath
from typing import Any

from .temporal_bidirectional_genome import BidirectionalGenomeError, FrozenModule, FrozenPair
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_search_policy_v2 import inspect_management_reachability


G0_ACCEPTED_REFERENCE_SCHEMA = "temporal_qd_g0_accepted_reference_v4"
G0_ACCEPTED_POOL_SCHEMA = "temporal_qd_g0_accepted_pool_v4"
G0_DESCRIPTOR_PROJECTION_SCHEMA = "temporal_qd_g0_descriptor_projection_v4"
G0_LEDGER_SCHEMA = "temporal_qd_g0_campaign_ledger_v1"
G0_BOOTSTRAP_RESULT_SCHEMA = "temporal_qd_g0_bootstrap_selection_v3"
G0_POLICY_SCHEMA = "temporal_qd_g0_bootstrap_policy_v3"

DESCRIPTOR_AXES = (
    "long.topology", "short.topology", "long.graphSize", "short.graphSize",
    "long.indicatorSemantics", "short.indicatorSemantics",
    "long.fuzzyMembershipShape", "short.fuzzyMembershipShape",
    "long.entryGuardEventEvidenceSemantics", "short.entryGuardEventEvidenceSemantics",
    "long.holdKindBucket", "short.holdKindBucket",
    "long.initialStopKindBucket", "short.initialStopKindBucket",
    "long.initialTargetKindBucket", "short.initialTargetKindBucket",
    "long.graphManagementTrailingModes", "short.graphManagementTrailingModes",
    "staticLongShortActivationPotential",
)
DEFAULT_POLICY: dict[str, Any] = {
    "schemaVersion": G0_POLICY_SCHEMA,
    "policyVersion": "temporal_qd_g0_verified_indexed_coverage_v3",
    "selectionMethod": "indexed_incremental_marginal_coverage",
    "secondaryTieBreak": "lower_global_bucket_frequency_then_canonical_identity",
    "descriptorAxes": list(DESCRIPTOR_AXES),
    "marketEvidenceRead": False,
}


def _clone(value: Any) -> Any:
    return deepcopy(value)


def _plain(value: Any) -> Any:
    """Return a canonical-JSON-compatible projection of frozen mapping payloads."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_plain(item) for item in value]
    return value


def _sha(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise TemporalDiscoveryContractError(f"{name} must be a SHA-256 identity")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise TemporalDiscoveryContractError(f"{name} must be a SHA-256 identity") from exc
    return value


def _exact(value: Mapping[str, Any], fields: set[str], *, label: str) -> None:
    # A closed projection schema makes all economic-field spelling/case variants
    # invalid by construction, instead of relying on a deny-list.
    if set(value) != fields:
        raise TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _integer(value: Any, *, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise TemporalDiscoveryContractError(f"{name} must be an integer >= {minimum}")
    return value


def _list_of_text(value: Any, *, label: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or any(not isinstance(item, str) for item in value):
        raise TemporalDiscoveryContractError(f"{label} must be a list of text")
    return list(value)


_ENTRY_REQUIRED = {
    "schemaVersion", "configSha256", "generationIndex", "proposalOrdinal", "originKind",
    "proposal", "operatorImplementationSha256", "disposition", "candidate", "entrySha256",
}
# G0 is pre-economic: lake/identity-check payloads have no role here.  The only
# historical optional record we retain admission for is the closed static
# funnel audit, which is independently verified against the frozen pair.
_ENTRY_OPTIONAL = {"funnelCandidate"}
_CANDIDATE_REQUIRED = {
    "candidateId", "sourceMode", "seedId", "generationIndex", "birthOrdinal", "proposalOrdinal",
    "sourceProfile", "sourceProfileSha256", "profileSnapshotSha256", "programSha256",
    "validationReportSha256", "candidateIdentityMaterial", "candidateIdentitySha256",
    "structuralDepth", "structuralOperatorHistory", "mutationTrace", "activationAwareRepairs",
    "constructionEvidenceScope", "bidirectionalGenome", "lineage", "pairProposal",
    "pairProposalSha256",
}
# G0 has no evidence authority yet.  A canonical evidence identity belongs to
# the later economic boundary and is therefore rejected rather than trusted.
_CANDIDATE_OPTIONAL: set[str] = set()
_CANDIDATE_IDENTITY_FIELDS = {
    "schemaVersion", "qdEngineVersion", "originKind", "bidirectionalGenomeIdentitySha256",
    "pairPolicySha256", "longModuleIdentitySha256", "shortModuleIdentitySha256",
    "longGrammarContextSha256", "shortGrammarContextSha256", "longCatalogSha256",
    "shortCatalogSha256", "longPolicySha256", "shortPolicySha256",
    "longNativeAuthoritySha256", "shortNativeAuthoritySha256", "pairCompilerAuthoritySha256",
    "compiledRawPairSha256", "compiledProfileSha256", "compiledProgramSha256",
    "compiledValidationReportSha256", "orderedSideLineage", "materializedPairProposalSha256",
}
MAX_CANONICAL_GRAPH_STATES = 32
MAX_CANONICAL_GRAPH_TRANSITIONS = 128


def _closed_keys(value: Any, *, required: set[str], optional: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not required.issubset(value) or not set(value).issubset(required | optional):
        raise TemporalDiscoveryContractError(f"{label} has an unexpected schema")
    return value


def _validate_g0_accepted_entry_schema(entry: Mapping[str, Any]) -> None:
    """Close the G0 immigrant journal surface before inspecting its payload."""
    row = _closed_keys(entry, required=_ENTRY_REQUIRED, optional=_ENTRY_OPTIONAL, label="G0 accepted journal entry")
    if row.get("schemaVersion") != "temporal_qd_proposal_entry_v3" or row.get("disposition") != "accepted":
        raise TemporalDiscoveryContractError("G0 journal entry is not an accepted canonical v3 entry")
    _integer(row.get("generationIndex"), name="journal generationIndex")
    _integer(row.get("proposalOrdinal"), name="journal proposalOrdinal")
    if row.get("originKind") != "random_immigrant":
        raise TemporalDiscoveryContractError("G0 bootstrap accepts only random immigrant journal entries")
    _sha(row.get("configSha256"), name="configSha256")
    _sha(row.get("operatorImplementationSha256"), name="operatorImplementationSha256")
    proposal = _closed_keys(
        row.get("proposal"),
        required={"schemaVersion", "proposalSeed", "originKind", "side", "factoryPair", "pairIdentitySha256", "disposition", "proposalSha256"},
        optional={"factoryConstructionAudit"}, label="G0 immigrant proposal",
    )
    if proposal.get("schemaVersion") != "temporal_qd_pair_proposal_v2" or proposal.get("originKind") != "random_immigrant" or proposal.get("disposition") != "materialized" or not isinstance(proposal.get("proposalSeed"), str) or not isinstance(proposal.get("side"), str):
        raise TemporalDiscoveryContractError("G0 immigrant proposal fields are invalid")
    _sha(proposal.get("pairIdentitySha256"), name="proposal pairIdentitySha256")
    candidate = _closed_keys(row.get("candidate"), required=_CANDIDATE_REQUIRED, optional=_CANDIDATE_OPTIONAL, label="G0 accepted candidate")
    material = _closed_keys(candidate.get("candidateIdentityMaterial"), required=_CANDIDATE_IDENTITY_FIELDS, optional=set(), label="G0 candidate identity material")
    if material.get("originKind") != row["originKind"] or candidate.get("pairProposal") != proposal:
        raise TemporalDiscoveryContractError("G0 candidate does not bind its journal immigrant proposal")
    if candidate.get("pairProposalSha256") != proposal.get("proposalSha256") or material.get("materializedPairProposalSha256") != proposal.get("proposalSha256"):
        raise TemporalDiscoveryContractError("G0 candidate proposal identity binding drift")
    if candidate.get("sourceMode") != "qd_random_immigrant_bidirectional_pair" or candidate.get("seedId") != "bidirectional_pair":
        raise TemporalDiscoveryContractError("G0 random immigrant source semantics drifted")
    if candidate.get("mutationTrace") != [] or candidate.get("activationAwareRepairs") != []:
        raise TemporalDiscoveryContractError("G0 random immigrant must not carry mutation or activation repair history")
    lineage = _closed_keys(candidate.get("lineage"), required={"schemaVersion", "candidateId", "candidateIdentitySha256", "pairIdentitySha256", "orderedSideLineage"}, optional=set(), label="G0 candidate lineage")
    if lineage.get("schemaVersion") != "temporal_qd_bidirectional_candidate_lineage_v1" or not isinstance(lineage.get("orderedSideLineage"), list):
        raise TemporalDiscoveryContractError("G0 candidate lineage is invalid")
    scope = _closed_keys(candidate.get("constructionEvidenceScope"), required={"schemaVersion", "evidencePlanRotationRequired", "lakeScopeRegenerationRequired", "reasons", "timeframeMutationTraceSha256s", "evidenceScopeSha256"}, optional=set(), label="G0 construction evidence scope")
    if scope.get("schemaVersion") != "temporal_qd_construction_evidence_scope_v1" or scope.get("evidencePlanRotationRequired") is not False or scope.get("lakeScopeRegenerationRequired") is not False or scope.get("reasons") != [] or scope.get("timeframeMutationTraceSha256s") != [] or scope.get("evidenceScopeSha256") != canonical_sha256({key: value for key, value in scope.items() if key != "evidenceScopeSha256"}):
        raise TemporalDiscoveryContractError("G0 construction evidence scope is invalid")


def _verify_accepted_entry(entry: Mapping[str, Any]) -> tuple[Mapping[str, Any], FrozenPair]:
    """Rehydrate actual pair-generation output; never accept a partial profile."""
    _validate_g0_accepted_entry_schema(entry)
    if _sha(entry.get("entrySha256"), name="entrySha256") != canonical_sha256(
        {key: value for key, value in entry.items() if key != "entrySha256"}
    ):
        raise TemporalDiscoveryContractError("accepted pair journal entry identity drift")
    candidate = entry.get("candidate")
    proposal = entry.get("proposal")
    if not isinstance(candidate, Mapping) or not isinstance(proposal, Mapping):
        raise TemporalDiscoveryContractError("accepted pair journal entry lacks candidate/proposal")
    try:
        pair = FrozenPair.from_payload(candidate.get("bidirectionalGenome"))
    except (BidirectionalGenomeError, TypeError) as exc:
        raise TemporalDiscoveryContractError("accepted candidate frozen pair is invalid") from exc
    try:
        proposal_pair = FrozenPair.from_payload(proposal.get("factoryPair"))
    except (BidirectionalGenomeError, TypeError) as exc:
        raise TemporalDiscoveryContractError("accepted proposal frozen pair is invalid") from exc
    if proposal.get("pairIdentitySha256") != pair.identity_sha256 or proposal_pair.identity_sha256 != pair.identity_sha256 or proposal_pair.canonical_payload() != pair.canonical_payload():
        raise TemporalDiscoveryContractError("accepted proposal frozen pair does not bind candidate genome")
    material = candidate.get("candidateIdentityMaterial")
    if not isinstance(material, Mapping) or candidate.get("candidateIdentitySha256") != canonical_sha256(material):
        raise TemporalDiscoveryContractError("accepted candidate identity material drift")
    if material.get("bidirectionalGenomeIdentitySha256") != pair.identity_sha256:
        raise TemporalDiscoveryContractError("accepted candidate identity does not bind frozen pair")
    canonical_side_lineage = _plain(pair.canonical_payload()["sideTargetedLineage"])
    if material.get("orderedSideLineage") != canonical_side_lineage or candidate.get("structuralOperatorHistory") != canonical_side_lineage or candidate.get("structuralDepth") != len(canonical_side_lineage):
        raise TemporalDiscoveryContractError("accepted candidate structural lineage drift")
    lineage = candidate["lineage"]
    if lineage.get("candidateId") != candidate.get("candidateId") or lineage.get("candidateIdentitySha256") != candidate.get("candidateIdentitySha256") or lineage.get("pairIdentitySha256") != pair.identity_sha256 or lineage.get("orderedSideLineage") != canonical_side_lineage:
        raise TemporalDiscoveryContractError("accepted candidate lineage does not bind frozen pair")
    expected = {
        "sourceProfileSha256": pair.raw_pair_sha256,
        "profileSnapshotSha256": pair.profile_sha256,
        "programSha256": pair.native_program_sha256,
        "validationReportSha256": pair.native_validation_report_sha256,
    }
    if not isinstance(candidate.get("sourceProfile"), Mapping) or canonical_sha256(candidate["sourceProfile"]) != pair.raw_pair_sha256 or any(candidate.get(key) != value for key, value in expected.items()):
        raise TemporalDiscoveryContractError("accepted candidate compiled/native identities diverged")
    candidate_id = candidate.get("candidateId")
    if not isinstance(candidate_id, str) or candidate_id != "qd_" + str(candidate["candidateIdentitySha256"])[7:35]:
        raise TemporalDiscoveryContractError("accepted candidate ID does not bind canonical identity")
    proposal_sha = proposal.get("proposalSha256")
    if _sha(proposal_sha, name="proposalSha256") != canonical_sha256({key: value for key, value in proposal.items() if key != "proposalSha256"}):
        raise TemporalDiscoveryContractError("accepted pair proposal identity drift")
    if candidate.get("pairProposalSha256") != proposal_sha or candidate.get("pairProposal") != proposal:
        raise TemporalDiscoveryContractError("accepted candidate does not bind exact pair proposal")
    if material.get("materializedPairProposalSha256") != proposal_sha:
        raise TemporalDiscoveryContractError("accepted candidate identity does not bind pair proposal")
    audit = proposal.get("factoryConstructionAudit")
    if audit is not None:
        audit = _closed_keys(audit, required={"schemaVersion", "pairIdentitySha256", "sides", "auditSha256"}, optional=set(), label="G0 factory construction audit")
        if audit.get("schemaVersion") != "temporal_qd_rich_immigrant_pair_construction_v1" or audit.get("pairIdentitySha256") != pair.identity_sha256 or audit.get("auditSha256") != canonical_sha256({key: value for key, value in audit.items() if key != "auditSha256"}):
            raise TemporalDiscoveryContractError("G0 factory construction audit identity drift")
        expected_sides = {}
        for module in (pair.long, pair.short):
            construction = next((item.get("audit") for item in reversed(module.lineage) if item.get("operation") == "rich_immigrant_construction"), None)
            if not isinstance(construction, Mapping):
                raise TemporalDiscoveryContractError("G0 factory construction audit lacks frozen lineage authority")
            expected_sides[module.direction] = _plain(construction)
        if audit.get("sides") != expected_sides:
            raise TemporalDiscoveryContractError("G0 factory construction audit diverged from frozen lineage")
    funnel = entry.get("funnelCandidate")
    if funnel is not None:
        funnel = _closed_keys(funnel, required={"schemaVersion", "candidateId", "rawSourceProfileSha256", "staticReachability", "nativeValidation", "admission"}, optional=set(), label="G0 funnel candidate")
        expected_funnel = {
            "schemaVersion": "temporal_qd_proposal_funnel_stage_v1", "candidateId": candidate["candidateId"],
            "rawSourceProfileSha256": pair.raw_pair_sha256,
            "staticReachability": {"outcome": "reachable", "reasons": []},
            "nativeValidation": {"outcome": "valid", "reasons": [], "resolvedProfileSha256": pair.profile_sha256, "programSha256": pair.native_program_sha256, "validationReportSha256": pair.native_validation_report_sha256},
            "admission": {"outcome": "admitted", "reasons": [], "canonicalEvidenceIdentitySha256": candidate.get("canonicalEvidenceIdentitySha256")},
        }
        if funnel != expected_funnel:
            raise TemporalDiscoveryContractError("G0 funnel candidate diverged from frozen authority")
    return candidate, pair


def _catalog_indicator_semantics(module: FrozenModule) -> dict[str, dict[str, str]]:
    profile = module.profile
    indicators = profile.get("indicators")
    if not isinstance(indicators, Sequence) or isinstance(indicators, (str, bytes)):
        raise TemporalDiscoveryContractError("frozen module lacks exact compiled indicators")
    payload = module.catalog.payload
    catalog = payload.get("catalog") if isinstance(payload, Mapping) else None
    primitives = catalog.get("indicators") if isinstance(catalog, Mapping) else None
    if not isinstance(primitives, Sequence) or isinstance(primitives, (str, bytes)):
        raise TemporalDiscoveryContractError("frozen module catalog snapshot lacks indicator primitives")
    primitive_by_id: dict[str, Mapping[str, Any]] = {}
    for primitive in primitives:
        if not isinstance(primitive, Mapping):
            continue
        meta = primitive.get("meta")
        nested_id = meta.get("id") if isinstance(meta, Mapping) else None
        flat_id = primitive.get("id")
        if isinstance(nested_id, str) and isinstance(flat_id, str) and nested_id != flat_id:
            raise TemporalDiscoveryContractError("frozen module catalog indicator identity is ambiguous")
        primitive_id = nested_id if isinstance(nested_id, str) else flat_id
        if not isinstance(primitive_id, str) or not primitive_id:
            continue
        if primitive_id in primitive_by_id:
            raise TemporalDiscoveryContractError("frozen module catalog indicator identities duplicate")
        primitive_by_id[primitive_id] = primitive
    result: dict[str, dict[str, str]] = {}
    for item in indicators:
        if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping):
            raise TemporalDiscoveryContractError("frozen module indicator is malformed")
        meta = item["meta"]
        instance, implementation = meta.get("instanceId"), meta.get("id")
        if not all(isinstance(value, str) and value for value in (instance, implementation)):
            raise TemporalDiscoveryContractError("frozen module indicator identity is incomplete")
        base = meta.get("baseIndicatorId")
        if base is None:
            # Standalone catalog primitives (especially directional events)
            # are their own semantic family and legitimately omit an alias.
            base = implementation
        if not isinstance(base, str) or not base:
            raise TemporalDiscoveryContractError("frozen module indicator base identity is malformed")
        primitive = primitive_by_id.get(implementation)
        if primitive is None:
            raise TemporalDiscoveryContractError("module indicator implementation is absent from frozen catalog")
        result[instance] = {
            "baseIndicatorId": base,
            "implementationIdentitySha256": canonical_sha256(
                {"catalogSha256": module.catalog.sha256, "catalogPrimitive": _plain(primitive)}
            ),
        }
    if len(result) != len(indicators):
        raise TemporalDiscoveryContractError("frozen module indicators duplicate an instance")
    return result


def _semantic(value: Any, *, event_map: Mapping[str, Any], group_map: Mapping[str, Any], indicator_map: Mapping[str, Any], plan_map: Mapping[str, Any], transition_map: Mapping[str, Any] | None = None, state_map: Mapping[str, Any] | None = None) -> Any:
    """Normalize declarations/references consistently, retaining native semantics."""
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"id", "label", "description", "reasonCode"}:
                continue
            if key in {"sourceStateId", "destinationStateId", "stateId"}:
                result[key] = (state_map or {}).get(str(item), "unbound_state")
            elif key in {"eventId", "eventBindingId"}:
                result[key] = event_map.get(str(item), "unbound_event")
            elif key in {"groupId", "evidenceGroupId"}:
                result[key] = group_map.get(str(item), "unbound_group")
            elif key in {"indicatorInstanceId", "indicatorId"}:
                result[key] = indicator_map.get(str(item), "unbound_indicator")
            elif key in {"indicatorInstanceIds", "indicatorIds"}:
                result[key] = [indicator_map.get(str(part), "unbound_indicator") for part in _list_of_text(item, label=key)]
            elif key in {"managementPlanId", "planId"}:
                result[key] = plan_map.get(str(item), "unbound_plan")
            elif key == "transitionId":
                # Declaration IDs are dropped; a reference binds to the
                # referenced transition's ID-independent semantic signature.
                result[key] = (transition_map or {}).get(str(item), "unbound_transition")
            else:
                result[key] = _semantic(item, event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map=plan_map, transition_map=transition_map, state_map=state_map)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_semantic(item, event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map=plan_map, transition_map=transition_map, state_map=state_map) for item in value]
    return value


_MANAGEMENT_KINDS = frozenset({
    "move_stop_to_break_even_next_open", "tighten_stop_next_open", "set_target_next_open",
    "cancel_target_next_open", "activate_trailing_stop_next_open", "deactivate_trailing_stop_next_open",
})


def _management_modes(*, plan: Mapping[str, Any], transitions: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    modes: set[str] = set()
    trailing = plan.get("trailingStop")
    if isinstance(trailing, Mapping):
        modes.add("trailing")
    # Only closed native action kinds count.  Names, labels, arbitrary fields,
    # and values such as "trailing" do not create a descriptor mode.
    for transition in transitions:
        for action in transition.get("actions") or []:
            if not isinstance(action, Mapping):
                raise TemporalDiscoveryContractError("native transition action is malformed")
            kind = action.get("kind")
            if kind in _MANAGEMENT_KINDS:
                modes.add(str(kind))
    return tuple(sorted(modes))


def _bucket(value: Mapping[str, Any], *, fallback: str) -> str:
    for key in ("percent", "multiple", "value", "bars", "hours"):
        number = value.get(key)
        if isinstance(number, (int, float)) and not isinstance(number, bool):
            return f"{key}:{number:g}"
    return fallback


def _side_liveness(pair: FrozenPair, report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    profile = pair.profile
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise TemporalDiscoveryContractError("frozen pair lacks graph")
    modules = ((graph.get("entryArbitration") or {}).get("modules"))
    transitions = graph.get("transitions")
    reachable = set(report.get("reachableStates") or [])
    if not isinstance(modules, Sequence) or not isinstance(transitions, Sequence):
        raise TemporalDiscoveryContractError("frozen pair graph lacks native liveness material")
    by_id = {str(item.get("id")): item for item in transitions if isinstance(item, Mapping) and isinstance(item.get("id"), str)}
    proof: dict[str, dict[str, Any]] = {}
    for module in modules:
        if not isinstance(module, Mapping) or module.get("direction") not in {"long", "short"}:
            raise TemporalDiscoveryContractError("frozen pair module is malformed")
        side = str(module["direction"])
        entry_routes = []
        for transition_id in _list_of_text(module.get("transitionIds"), label="module transitionIds"):
            transition = by_id.get(transition_id)
            if transition is None:
                raise TemporalDiscoveryContractError("frozen pair module references missing transition")
            for action in transition.get("actions") or []:
                if isinstance(action, Mapping) and action.get("kind") == "enter_next_open":
                    entry_routes.append(transition)
        reachable_entry = [route for route in entry_routes if route.get("sourceStateId") in reachable]
        proof[side] = {
            "entryActionRouteCount": len(entry_routes),
            "reachableEntryActionRouteCount": len(reachable_entry),
            "potential": bool(reachable_entry),
        }
    if set(proof) != {"long", "short"} or not all(row["potential"] for row in proof.values()):
        raise TemporalDiscoveryContractError("per-side entry liveness proof is incomplete")
    return proof


def _descriptor_projection(entry: Mapping[str, Any]) -> dict[str, Any]:
    candidate, pair = _verify_accepted_entry(entry)
    # This is the actual existing native static checker; its content hash is
    # recomputed rather than accepted as a caller-provided status boolean.
    report = inspect_management_reachability(pair.profile)
    if report.get("reachabilitySha256") != canonical_sha256({key: value for key, value in report.items() if key != "reachabilitySha256"}) or report.get("acceptable") is not True:
        raise TemporalDiscoveryContractError("canonical static reachability report is not acceptable")
    liveness = _side_liveness(pair, report)
    vector = _pair_descriptor_vector(pair=pair, liveness=liveness)
    projection = {
        "schemaVersion": G0_DESCRIPTOR_PROJECTION_SCHEMA,
        "candidateId": candidate["candidateId"], "candidateIdentitySha256": candidate["candidateIdentitySha256"],
        "pairIdentitySha256": pair.identity_sha256, "longCatalogSha256": pair.long.catalog.sha256,
        "shortCatalogSha256": pair.short.catalog.sha256,
        # The authoritative reports remain in the canonical journal entry.  The
        # bootstrap pool carries only their independently recomputed identities.
        "nativeValidationReportSha256": pair.native_validation_report_sha256,
        "staticReachabilityReportSha256": report["reachabilitySha256"],
        "perSideLivenessProof": liveness, "descriptorVector": vector,
    }
    projection["descriptorProjectionSha256"] = canonical_sha256(projection)
    return projection


def _canonical_graph_topology(*, states: Sequence[Mapping[str, Any]], transitions: Sequence[Mapping[str, Any]], event_map: Mapping[str, Any], group_map: Mapping[str, Any], indicator_map: Mapping[str, Any], plan_map: Mapping[str, Any]) -> str:
    """Linear ID-free encoding using validated compiled declaration order.

    State and transition array ordinals are construction semantics at this
    boundary.  Renaming an ID cannot change the descriptor; reordering a
    declaration intentionally can.  This avoids factorial graph-isomorphism
    work while preserving every executable edge, priority, and reference.
    """
    if len(states) > MAX_CANONICAL_GRAPH_STATES or len(transitions) > MAX_CANONICAL_GRAPH_TRANSITIONS:
        raise TemporalDiscoveryContractError("compiled graph exceeds G0 canonical runtime bound")
    state_rows: dict[str, Mapping[str, Any]] = {}
    for row in states:
        if not isinstance(row, Mapping) or not isinstance(row.get("id"), str) or row["id"] in state_rows:
            raise TemporalDiscoveryContractError("compiled graph states are malformed")
        state_rows[row["id"]] = row
    if not state_rows:
        raise TemporalDiscoveryContractError("compiled graph has no states")
    for row in transitions:
        if not isinstance(row, Mapping) or not isinstance(row.get("id"), str):
            raise TemporalDiscoveryContractError("compiled graph transitions are malformed")
        source, destination = row.get("sourceStateId"), row.get("destinationStateId")
        if not isinstance(source, str) or not isinstance(destination, str) or source not in state_rows or destination not in state_rows:
            raise TemporalDiscoveryContractError("compiled graph transition wiring is malformed")
    state_ordinals = {state_id: index for index, state_id in enumerate(state_rows)}
    transition_ordinals = {str(row["id"]): index for index, row in enumerate(transitions)}
    if len(transition_ordinals) != len(transitions):
        raise TemporalDiscoveryContractError("compiled graph transition declarations duplicate IDs")
    encoding = {
        "schemaVersion": "temporal_qd_g0_declared_graph_semantics_v1",
        "declarationOrderIsSemantic": True,
        "states": [
            _semantic(row, event_map=event_map, group_map=group_map, indicator_map=indicator_map,
                      plan_map=plan_map, state_map=state_ordinals, transition_map=transition_ordinals)
            for row in state_rows.values()
        ],
        "transitions": [
            _semantic(row, event_map=event_map, group_map=group_map, indicator_map=indicator_map,
                      plan_map=plan_map, state_map=state_ordinals, transition_map=transition_ordinals)
            for row in transitions
        ],
    }
    return canonical_sha256(encoding)


def _module_descriptor(module: FrozenModule, *, compiled_transitions: Sequence[Mapping[str, Any]], compiled_states: Sequence[Mapping[str, Any]] | None = None) -> dict[str, str]:
    profile = module.profile
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise TemporalDiscoveryContractError("frozen module profile lacks graph")
    transitions = list(compiled_transitions)
    if any(not isinstance(item, Mapping) for item in transitions):
        raise TemporalDiscoveryContractError("frozen module transitions are malformed")
    states = compiled_states if compiled_states is not None else graph.get("states")
    if not isinstance(states, Sequence) or isinstance(states, (str, bytes)):
        raise TemporalDiscoveryContractError("frozen module profile lacks states")
    indicator_map = _catalog_indicator_semantics(module)
    event_map = {}
    for event in graph.get("eventBindings") or []:
        if isinstance(event, Mapping) and isinstance(event.get("id"), str):
            event_map[str(event["id"])] = _semantic(event, event_map={}, group_map={}, indicator_map=indicator_map, plan_map={})
    group_map = {}
    for group in graph.get("evidenceGroups") or []:
        if isinstance(group, Mapping) and isinstance(group.get("id"), str):
            group_map[str(group["id"])] = _semantic(group, event_map=event_map, group_map={}, indicator_map=indicator_map, plan_map={})
    library = ((profile.get("executionConfig") or {}).get("managementLibrary") or {})
    plans = library.get("plans")
    default_plan = library.get("defaultPlanId")
    if not isinstance(plans, Sequence) or not isinstance(default_plan, str):
        raise TemporalDiscoveryContractError("frozen module lacks closed management plan binding")
    by_plan = {str(item.get("id")): item for item in plans if isinstance(item, Mapping) and isinstance(item.get("id"), str)}
    if default_plan not in by_plan or len(by_plan) != len(plans):
        raise TemporalDiscoveryContractError("frozen module management plan binding is invalid")
    plan_map = {key: _semantic(value, event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map={}) for key, value in by_plan.items()}
    # Construct reference targets without recursively following references;
    # this makes renamed declaration/reference pairs hash-identically while a
    # genuinely different target remains distinguishable.
    transition_map = {
        str(item["id"]): canonical_sha256(_semantic(
            {key: value for key, value in item.items() if key not in {"id", "transitionId"}},
            event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map=plan_map,
        ))
        for item in transitions if isinstance(item.get("id"), str)
    }
    topology = _canonical_graph_topology(states=states, transitions=transitions, event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map=plan_map)
    normalized = [_semantic(item, event_map=event_map, group_map=group_map, indicator_map=indicator_map, plan_map=plan_map, transition_map=transition_map) for item in transitions]
    edge_hashes = sorted(canonical_sha256(item) for item in normalized)
    plan = by_plan[default_plan]
    stop = plan.get("initialStop")
    if not isinstance(stop, Mapping) or not isinstance(stop.get("kind"), str):
        raise TemporalDiscoveryContractError("frozen module plan lacks initial stop")
    target = plan.get("initialTarget")
    if target is None:
        target_bucket = "no_target|none"
    elif isinstance(target, Mapping) and isinstance(target.get("kind"), str):
        kind = str(target["kind"])
        category = "coupled" if kind == "reward_multiple" else "decoupled" if kind == "fixed_percent" else "dynamic"
        target_bucket = f"{category}:{kind}|{_bucket(target, fallback='dynamic')}"
    else:
        raise TemporalDiscoveryContractError("frozen module target is malformed")
    hold = plan.get("holdPolicy") or {"kind": "none"}
    if not isinstance(hold, Mapping):
        raise TemporalDiscoveryContractError("frozen module hold policy is malformed")
    all_indicator_semantics = sorted(canonical_sha256(value) for value in indicator_map.values())
    all_group_semantics = sorted(canonical_sha256(value) for value in group_map.values())
    all_event_semantics = sorted(canonical_sha256(value) for value in event_map.values())
    # Management actions execute on the compiled bidirectional graph, not on a
    # source-module name or an author-provided label.
    modes = _management_modes(plan=plan, transitions=compiled_transitions)
    return {
        "topology": topology,
        "graphSize": f"states:{len(states)}|transitions:{len(transitions)}",
        "indicatorSemantics": canonical_sha256({"indicators": all_indicator_semantics}),
        "fuzzyMembershipShape": canonical_sha256({"groups": all_group_semantics}),
        "entryGuardEventEvidenceSemantics": canonical_sha256({"events": all_event_semantics, "groups": all_group_semantics, "edges": edge_hashes}),
        "holdKindBucket": f"{hold.get('kind', 'none')}|{_bucket(hold, fallback='default')}",
        "initialStopKindBucket": f"{stop['kind']}|{_bucket(stop, fallback='dynamic')}",
        "initialTargetKindBucket": target_bucket,
        "graphManagementTrailingModes": ",".join(modes) or "none",
    }


def _pair_descriptor_vector(*, pair: FrozenPair, liveness: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    graph = pair.profile.get("graph")
    if not isinstance(graph, Mapping):
        raise TemporalDiscoveryContractError("frozen pair lacks compiled graph")
    initial_state_id = graph.get("initialStateId")
    if not isinstance(initial_state_id, str) or not initial_state_id:
        raise TemporalDiscoveryContractError("frozen pair compiled graph lacks initialStateId")
    compiled = graph.get("transitions")
    modules = ((graph.get("entryArbitration") or {}).get("modules"))
    if not isinstance(compiled, Sequence) or isinstance(compiled, (str, bytes)) or not isinstance(modules, Sequence):
        raise TemporalDiscoveryContractError("frozen pair compiled graph lacks transitions/modules")
    by_id = {str(row.get("id")): row for row in compiled if isinstance(row, Mapping) and isinstance(row.get("id"), str)}

    def side_graph(side: str) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        module = next((row for row in modules if isinstance(row, Mapping) and row.get("direction") == side), None)
        if module is None:
            raise TemporalDiscoveryContractError("frozen pair compiled module is missing")
        transition_ids = _list_of_text(module.get("transitionIds"), label="compiled module transitionIds")
        if len(set(transition_ids)) != len(transition_ids) or any(transition_id not in by_id for transition_id in transition_ids):
            raise TemporalDiscoveryContractError("frozen pair compiled module transition binding is incomplete")
        # Preserve the authoritative compiled transition-array declaration order,
        # not a caller-provided reference ordering in the module manifest.
        requested = set(transition_ids)
        transitions = [row for row in compiled if isinstance(row, Mapping) and row.get("id") in requested]
        if len(transitions) != len(transition_ids):
            raise TemporalDiscoveryContractError("frozen pair compiled module lacks transitions")
        # The compiler owns the shared entry-state identifier. Module manifests
        # intentionally enumerate only their direction-local states.
        state_ids = {initial_state_id, *_list_of_text(module.get("stateIds"), label="compiled module stateIds")}
        # Preserve the compiled graph's declared state-array order.  It is an
        # authoritative construction choice, unlike opaque state IDs.
        states = [row for row in graph.get("states") or [] if isinstance(row, Mapping) and row.get("id") in state_ids]
        referenced = {str(transition.get("sourceStateId")) for transition in transitions} | {str(transition.get("destinationStateId")) for transition in transitions}
        if referenced != {str(row.get("id")) for row in states}:
            raise TemporalDiscoveryContractError("frozen pair compiled module state binding is incomplete")
        return transitions, states

    long_transitions, long_states = side_graph("long")
    short_transitions, short_states = side_graph("short")
    long = _module_descriptor(pair.long, compiled_transitions=long_transitions, compiled_states=long_states)
    short = _module_descriptor(pair.short, compiled_transitions=short_transitions, compiled_states=short_states)
    vector: dict[str, str] = {}
    for side, row in (("long", long), ("short", short)):
        for key, value in row.items():
            vector[f"{side}.{key}"] = value
    vector["staticLongShortActivationPotential"] = "|".join(
        f"{side}:{str(bool(liveness[side]['potential'])).lower()}" for side in ("long", "short")
    )
    return {axis: vector[axis] for axis in DESCRIPTOR_AXES}


def _journal_relative_path(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TemporalDiscoveryContractError("journal relative path must be non-empty text")
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts or path == PurePosixPath(".") or (path.parts and len(path.parts[0]) == 2 and path.parts[0][1] == ":"):
        raise TemporalDiscoveryContractError("journal relative path must stay below the journal root")
    return path.as_posix()


def project_accepted_pair_entry(*, construction_pool_identity_sha256: str, proposal_ordinal: int, journal_path: str, accepted_pair_entry: Mapping[str, Any]) -> dict[str, Any]:
    """Project one journal entry into a compact, lazy-hydratable G0 reference.

    ``accepted_pair_entry`` is consumed only at projection time.  It is never
    retained by a reference or pool: a 4k pool must not duplicate multi-MiB
    candidate/proposal journal records in memory or on disk.
    """
    _sha(construction_pool_identity_sha256, name="constructionPoolIdentitySha256")
    _integer(proposal_ordinal, name="proposalOrdinal")
    if not isinstance(journal_path, str) or not journal_path.strip():
        raise TemporalDiscoveryContractError("journalPath must be non-empty text")
    # Do not clone a journal payload here.  It can be MiB-scale; verification
    # is read-only and the returned reference deliberately excludes it.
    entry = accepted_pair_entry
    candidate, _ = _verify_accepted_entry(entry)
    if proposal_ordinal != entry.get("proposalOrdinal") or proposal_ordinal != candidate.get("proposalOrdinal"):
        raise TemporalDiscoveryContractError("G0 proposal ordinal does not bind journal/candidate")
    if candidate.get("generationIndex") != entry.get("generationIndex"):
        raise TemporalDiscoveryContractError("G0 generation index does not bind journal/candidate")
    _integer(candidate.get("birthOrdinal"), name="candidate birthOrdinal")
    lineage = {
        "schemaVersion": "temporal_qd_g0_construction_lineage_v1",
        "entrySha256": entry["entrySha256"], "proposalOrdinal": proposal_ordinal,
        "generationIndex": entry["generationIndex"], "birthOrdinal": candidate["birthOrdinal"],
        "originKind": entry["originKind"], "candidateId": candidate["candidateId"],
        "candidateIdentitySha256": candidate["candidateIdentitySha256"],
    }
    lineage["constructionLineageSha256"] = canonical_sha256(lineage)
    projection = _descriptor_projection(entry)
    reference = {
        "schemaVersion": G0_ACCEPTED_REFERENCE_SCHEMA,
        "constructionPoolIdentitySha256": construction_pool_identity_sha256,
        "proposalOrdinal": proposal_ordinal,
        "journalReference": {
            "schemaVersion": "temporal_qd_g0_journal_reference_v1",
            "journalRelativePath": _journal_relative_path(journal_path),
            "entrySha256": entry["entrySha256"],
        },
        "acceptedPairEntrySha256": entry["entrySha256"],
        "candidateId": candidate["candidateId"],
        "candidateIdentitySha256": candidate["candidateIdentitySha256"],
        "constructionLineage": lineage,
        "descriptorProjection": projection,
        "descriptorProjectionSha256": projection["descriptorProjectionSha256"],
    }
    reference["referenceSha256"] = canonical_sha256(reference)
    _validate_reference(reference)
    return reference


def _validate_reference(reference: Mapping[str, Any]) -> None:
    _exact(reference, {"schemaVersion", "constructionPoolIdentitySha256", "proposalOrdinal", "journalReference", "acceptedPairEntrySha256", "candidateId", "candidateIdentitySha256", "constructionLineage", "descriptorProjection", "descriptorProjectionSha256", "referenceSha256"}, label="G0 accepted reference")
    if reference.get("schemaVersion") != G0_ACCEPTED_REFERENCE_SCHEMA:
        raise TemporalDiscoveryContractError("G0 accepted reference schema version is invalid")
    _sha(reference.get("constructionPoolIdentitySha256"), name="constructionPoolIdentitySha256")
    _integer(reference.get("proposalOrdinal"), name="proposalOrdinal")
    journal = reference.get("journalReference")
    if not isinstance(journal, Mapping) or set(journal) != {"schemaVersion", "journalRelativePath", "entrySha256"} or journal.get("schemaVersion") != "temporal_qd_g0_journal_reference_v1" or journal.get("journalRelativePath") != _journal_relative_path(journal.get("journalRelativePath")) or journal.get("entrySha256") != reference.get("acceptedPairEntrySha256"):
        raise TemporalDiscoveryContractError("G0 accepted reference entry binding drift")
    _sha(reference.get("acceptedPairEntrySha256"), name="acceptedPairEntrySha256")
    if not isinstance(reference.get("candidateId"), str) or not isinstance(reference.get("candidateIdentitySha256"), str):
        raise TemporalDiscoveryContractError("G0 accepted reference candidate binding drift")
    _sha(reference["candidateIdentitySha256"], name="candidateIdentitySha256")
    lineage = reference.get("constructionLineage")
    if not isinstance(lineage, Mapping) or set(lineage) != {"schemaVersion", "entrySha256", "proposalOrdinal", "generationIndex", "birthOrdinal", "originKind", "candidateId", "candidateIdentitySha256", "constructionLineageSha256"} or lineage.get("schemaVersion") != "temporal_qd_g0_construction_lineage_v1" or lineage.get("entrySha256") != reference["acceptedPairEntrySha256"] or lineage.get("proposalOrdinal") != reference["proposalOrdinal"] or lineage.get("candidateId") != reference["candidateId"] or lineage.get("candidateIdentitySha256") != reference["candidateIdentitySha256"]:
        raise TemporalDiscoveryContractError("G0 construction lineage binding drift")
    _integer(lineage.get("generationIndex"), name="construction lineage generationIndex")
    _integer(lineage.get("birthOrdinal"), name="construction lineage birthOrdinal")
    if lineage.get("originKind") != "random_immigrant" or _sha(lineage.get("constructionLineageSha256"), name="constructionLineageSha256") != canonical_sha256({key: value for key, value in lineage.items() if key != "constructionLineageSha256"}):
        raise TemporalDiscoveryContractError("G0 construction lineage identity drift")
    claimed_projection = reference.get("descriptorProjection")
    if not isinstance(claimed_projection, Mapping):
        raise TemporalDiscoveryContractError("G0 accepted reference lacks descriptor projection")
    _exact(claimed_projection, {
        "schemaVersion", "candidateId", "candidateIdentitySha256", "pairIdentitySha256",
        "longCatalogSha256", "shortCatalogSha256", "nativeValidationReportSha256",
        "staticReachabilityReportSha256", "perSideLivenessProof", "descriptorVector",
        "descriptorProjectionSha256",
    }, label="G0 descriptor projection")
    if claimed_projection.get("schemaVersion") != G0_DESCRIPTOR_PROJECTION_SCHEMA or claimed_projection.get("candidateId") != reference["candidateId"] or claimed_projection.get("candidateIdentitySha256") != reference["candidateIdentitySha256"] or claimed_projection.get("nativeValidationReportSha256") is None or claimed_projection.get("staticReachabilityReportSha256") is None:
        raise TemporalDiscoveryContractError("G0 descriptor projection binding drift")
    for field in ("pairIdentitySha256", "longCatalogSha256", "shortCatalogSha256", "nativeValidationReportSha256", "staticReachabilityReportSha256"):
        _sha(claimed_projection.get(field), name=field)
    liveness = claimed_projection.get("perSideLivenessProof")
    if not isinstance(liveness, Mapping) or set(liveness) != {"long", "short"}:
        raise TemporalDiscoveryContractError("G0 descriptor liveness proof schema drift")
    for side in ("long", "short"):
        if not isinstance(liveness[side], Mapping) or set(liveness[side]) != {"entryActionRouteCount", "reachableEntryActionRouteCount", "potential"} or liveness[side].get("potential") is not True:
            raise TemporalDiscoveryContractError("G0 descriptor liveness proof is invalid")
        _integer(liveness[side].get("entryActionRouteCount"), name=f"{side} entryActionRouteCount")
        _integer(liveness[side].get("reachableEntryActionRouteCount"), name=f"{side} reachableEntryActionRouteCount")
    vector = claimed_projection.get("descriptorVector")
    # References are persisted through canonical JSON with sorted object keys.
    # Descriptor-axis membership is closed, but declaration/insertion order is
    # not recoverable after that round trip and is not semantic.
    if not isinstance(vector, Mapping) or set(vector) != set(DESCRIPTOR_AXES) or any(not isinstance(vector[axis], str) for axis in DESCRIPTOR_AXES):
        raise TemporalDiscoveryContractError("G0 descriptor vector schema drift")
    claimed_hash = claimed_projection.get("descriptorProjectionSha256")
    if _sha(claimed_hash, name="descriptorProjectionSha256") != canonical_sha256({key: value for key, value in claimed_projection.items() if key != "descriptorProjectionSha256"}) or reference.get("descriptorProjectionSha256") != claimed_hash:
        raise TemporalDiscoveryContractError("G0 descriptor projection drift")
    if _sha(reference.get("referenceSha256"), name="referenceSha256") != canonical_sha256({key: value for key, value in reference.items() if key != "referenceSha256"}):
        raise TemporalDiscoveryContractError("G0 accepted reference identity drift")


def verify_selected_reference_against_entry(*, reference: Mapping[str, Any], accepted_pair_entry: Mapping[str, Any]) -> dict[str, Any]:
    """Verify a caller-loaded selected entry; use the journal reader to hydrate."""
    _validate_reference(reference)
    entry = accepted_pair_entry
    candidate, _ = _verify_accepted_entry(entry)
    if entry["entrySha256"] != reference["acceptedPairEntrySha256"] or candidate["candidateId"] != reference["candidateId"] or candidate["candidateIdentitySha256"] != reference["candidateIdentitySha256"]:
        raise TemporalDiscoveryContractError("lazy journal hydration does not match selected reference")
    projection = _descriptor_projection(entry)
    if projection != reference["descriptorProjection"]:
        raise TemporalDiscoveryContractError("lazy journal hydration descriptor projection drift")
    return entry


def read_selected_entry_from_journal(*, reference: Mapping[str, Any], journal_root: str | Path) -> Mapping[str, Any]:
    """Read exactly one selected entry from its stable relative journal locator."""
    _validate_reference(reference)
    root = Path(journal_root).resolve()
    relative = PurePosixPath(reference["journalReference"]["journalRelativePath"])
    path = (root.joinpath(*relative.parts)).resolve()
    if root != path and root not in path.parents:
        raise TemporalDiscoveryContractError("selected journal locator escapes journal root")
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError("could not read selected canonical journal entry") from exc
    if not isinstance(loaded, Mapping):
        raise TemporalDiscoveryContractError("selected canonical journal entry must be an object")
    return verify_selected_reference_against_entry(reference=reference, accepted_pair_entry=loaded)


def build_accepted_pool(*, construction_pool_identity_sha256: str, references: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pool = {"schemaVersion": G0_ACCEPTED_POOL_SCHEMA, "constructionPoolSize": len(references), "constructionPoolIdentitySha256": _sha(construction_pool_identity_sha256, name="constructionPoolIdentitySha256"), "acceptedReferences": [_clone(dict(item)) for item in references]}
    _validate_pool(pool, verify_identity=False)
    pool["acceptedPoolSha256"] = canonical_sha256(_pool_material(pool))
    return pool


def _pool_material(pool: Mapping[str, Any]) -> dict[str, Any]:
    refs = pool.get("acceptedReferences")
    if not isinstance(refs, Sequence) or isinstance(refs, (str, bytes)):
        raise TemporalDiscoveryContractError("G0 accepted pool references are malformed")
    return {"schemaVersion": pool.get("schemaVersion"), "constructionPoolSize": pool.get("constructionPoolSize"), "constructionPoolIdentitySha256": pool.get("constructionPoolIdentitySha256"), "acceptedReferences": sorted(refs, key=lambda ref: str(ref.get("referenceSha256") if isinstance(ref, Mapping) else ""))}


def _key(ref: Mapping[str, Any]) -> tuple[str, str, int]:
    return str(ref["candidateIdentitySha256"]), str(ref["candidateId"]), int(ref["proposalOrdinal"])


def _validate_pool(pool: Mapping[str, Any], *, verify_identity: bool = True) -> list[Mapping[str, Any]]:
    _exact(pool, {"schemaVersion", "constructionPoolSize", "constructionPoolIdentitySha256", "acceptedReferences", "acceptedPoolSha256"} if "acceptedPoolSha256" in pool else {"schemaVersion", "constructionPoolSize", "constructionPoolIdentitySha256", "acceptedReferences"}, label="G0 accepted pool")
    if pool.get("schemaVersion") != G0_ACCEPTED_POOL_SCHEMA:
        raise TemporalDiscoveryContractError("G0 accepted pool schema version is invalid")
    size = _integer(pool.get("constructionPoolSize"), name="constructionPoolSize", minimum=1)
    identity = _sha(pool.get("constructionPoolIdentitySha256"), name="constructionPoolIdentitySha256")
    refs = pool.get("acceptedReferences")
    if not isinstance(refs, Sequence) or isinstance(refs, (str, bytes)) or len(refs) != size:
        raise TemporalDiscoveryContractError("G0 accepted pool size does not bind references")
    unique: dict[str, set[Any]] = defaultdict(set)
    valid: list[Mapping[str, Any]] = []
    for ref in refs:
        if not isinstance(ref, Mapping):
            raise TemporalDiscoveryContractError("G0 accepted pool reference is malformed")
        if "referenceSha256" not in ref:
            raise TemporalDiscoveryContractError("G0 accepted pool reference lacks required referenceSha256")
        _validate_reference(ref)
        if ref["constructionPoolIdentitySha256"] != identity:
            raise TemporalDiscoveryContractError("G0 accepted pool contains foreign reference")
        for field in ("candidateIdentitySha256", "candidateId", "proposalOrdinal", "acceptedPairEntrySha256", "referenceSha256"):
            if ref[field] in unique[field]:
                raise TemporalDiscoveryContractError(f"G0 accepted pool duplicates {field}")
            unique[field].add(ref[field])
        valid.append(ref)
    valid.sort(key=_key)
    birth_ordinals = [int(ref["constructionLineage"]["birthOrdinal"]) for ref in valid]
    if len(set(birth_ordinals)) != len(birth_ordinals) or sorted(birth_ordinals) != list(range(len(valid))):
        raise TemporalDiscoveryContractError("G0 accepted pool birth ordinals must be unique and contiguous")
    if verify_identity and _sha(pool.get("acceptedPoolSha256"), name="acceptedPoolSha256") != canonical_sha256(_pool_material(pool)):
        raise TemporalDiscoveryContractError("G0 accepted pool identity drift")
    return valid


def descriptor_vector(reference: Mapping[str, Any]) -> dict[str, str]:
    _validate_reference(reference)
    projection = reference["descriptorProjection"]
    assert isinstance(projection, Mapping)
    vector = projection["descriptorVector"]
    if not isinstance(vector, Mapping) or set(vector) != set(DESCRIPTOR_AXES):
        raise TemporalDiscoveryContractError("G0 descriptor vector schema drift")
    return {axis: str(vector[axis]) for axis in DESCRIPTOR_AXES}


def _ledger_rows(refs: Sequence[Mapping[str, Any]], *, selected_reference_sha256s: set[str] | None = None) -> list[dict[str, Any]]:
    selected = selected_reference_sha256s or set()
    return [{"proposalOrdinal": ref["proposalOrdinal"], "candidateId": ref["candidateId"], "candidateIdentitySha256": ref["candidateIdentitySha256"], "referenceSha256": ref["referenceSha256"], "marketEvidenceRead": False, "evaluationDisposition": ("selected_for_market_evaluation" if ref["referenceSha256"] in selected else "bootstrap_diversity_not_selected")} for ref in refs]


def materialize_campaign_ledger(*, accepted_pool: Mapping[str, Any], selected_reference_sha256s: Sequence[str] | None = None) -> dict[str, Any]:
    refs = _validate_pool(accepted_pool)
    selected = set(selected_reference_sha256s or [])
    known = {str(ref["referenceSha256"]) for ref in refs}
    if not selected.issubset(known):
        raise TemporalDiscoveryContractError("G0 campaign ledger selection contains a foreign reference")
    ledger = {"schemaVersion": G0_LEDGER_SCHEMA, "constructionPoolIdentitySha256": accepted_pool["constructionPoolIdentitySha256"], "acceptedPoolSha256": accepted_pool["acceptedPoolSha256"], "constructionPoolSize": accepted_pool["constructionPoolSize"], "rows": _ledger_rows(refs, selected_reference_sha256s=selected)}
    ledger["ledgerSha256"] = canonical_sha256(ledger)
    return ledger


def verify_campaign_ledger(*, ledger: Mapping[str, Any], accepted_pool: Mapping[str, Any], selected_reference_sha256s: Sequence[str] | None = None) -> dict[str, Any]:
    _exact(ledger, {"schemaVersion", "constructionPoolIdentitySha256", "acceptedPoolSha256", "constructionPoolSize", "rows", "ledgerSha256"}, label="G0 campaign ledger")
    if ledger.get("schemaVersion") != G0_LEDGER_SCHEMA or _sha(ledger.get("ledgerSha256"), name="ledgerSha256") != canonical_sha256({key: value for key, value in ledger.items() if key != "ledgerSha256"}):
        raise TemporalDiscoveryContractError("G0 campaign ledger identity drift")
    expected = materialize_campaign_ledger(accepted_pool=accepted_pool, selected_reference_sha256s=selected_reference_sha256s)
    if ledger != expected:
        raise TemporalDiscoveryContractError("G0 campaign ledger diverged from accepted pool")
    return _clone(expected)


def _policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    result = _clone(DEFAULT_POLICY if policy is None else dict(policy))
    if result != DEFAULT_POLICY:
        raise TemporalDiscoveryContractError("G0 bootstrap policy is unknown or drifted")
    return result


def _distribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, Counter[str]] = {axis: Counter() for axis in DESCRIPTOR_AXES}
    for row in rows:
        for axis, value in row["descriptor"].items():
            result[axis][str(value)] += 1
    return {axis: dict(sorted(count.items())) for axis, count in result.items()}


def select_g0_bootstrap(*, accepted_pool: Mapping[str, Any], evaluation_width: int, policy: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Indexed O(pool × axes) coverage selection; no economic input accepted."""
    refs = _validate_pool(accepted_pool)
    width = _integer(evaluation_width, name="evaluationWidth", minimum=1)
    if width > len(refs):
        raise TemporalDiscoveryContractError("G0 evaluation width exceeds accepted pool")
    frozen_policy = _policy(policy)
    rows = [{"reference": ref, "descriptor": descriptor_vector(ref)} for ref in refs]
    members: dict[tuple[str, str], list[int]] = defaultdict(list); frequencies: Counter[tuple[str, str]] = Counter(); buckets: list[list[tuple[str, str]]] = []
    for index, row in enumerate(rows):
        row_buckets = [(axis, str(row["descriptor"][axis])) for axis in DESCRIPTOR_AXES]
        buckets.append(row_buckets)
        for bucket in row_buckets:
            members[bucket].append(index); frequencies[bucket] += 1
    gains = [len(row) for row in buckets]; costs = [sum(frequencies[bucket] for bucket in row) for row in buckets]; versions = [0] * len(rows)
    heap = [(-gains[index], costs[index], _key(rows[index]["reference"]), versions[index], index) for index in range(len(rows))]
    heapq.heapify(heap); selected: list[int] = []; selected_set: set[int] = set(); covered: set[tuple[str, str]] = set(); trace: list[dict[str, Any]] = []
    while len(selected) < width:
        while True:
            neg_gain, cost, _, version, index = heapq.heappop(heap)
            if version == versions[index] and index not in selected_set:
                break
        selected.append(index); selected_set.add(index)
        for bucket in buckets[index]:
            if bucket in covered:
                continue
            covered.add(bucket)
            for member in members[bucket]:
                if member not in selected_set:
                    gains[member] -= 1; versions[member] += 1
                    heapq.heappush(heap, (-gains[member], costs[member], _key(rows[member]["reference"]), versions[member], member))
        trace.append({"selectionIndex": len(selected) - 1, "candidateIdentitySha256": rows[index]["reference"]["candidateIdentitySha256"], "marginalCoverage": -neg_gain, "globalBucketFrequencyCost": cost})
    chosen = [rows[index] for index in selected]; ledger = materialize_campaign_ledger(accepted_pool=accepted_pool, selected_reference_sha256s=[str(row["reference"]["referenceSha256"]) for row in chosen])
    result: dict[str, Any] = {"schemaVersion": G0_BOOTSTRAP_RESULT_SCHEMA, "policy": frozen_policy, "policySha256": canonical_sha256(frozen_policy), "constructionPoolIdentitySha256": accepted_pool["constructionPoolIdentitySha256"], "completePoolIdentitySha256": accepted_pool["acceptedPoolSha256"], "constructionPoolSize": accepted_pool["constructionPoolSize"], "completePoolCount": len(rows), "evaluationWidth": width, "marketEvidenceRead": False, "campaignLedgerSha256": ledger["ledgerSha256"], "campaignLedgerIntent": {"allConstructedIdentitiesMustEnterCampaignLedger": True, "selectedForMarketEvaluationOnly": True}, "selected": [{"proposalOrdinal": row["reference"]["proposalOrdinal"], "candidateId": row["reference"]["candidateId"], "candidateIdentitySha256": row["reference"]["candidateIdentitySha256"], "referenceSha256": row["reference"]["referenceSha256"]} for row in chosen], "selectionTrace": trace, "poolDistribution": _distribution(rows), "selectedDistribution": _distribution(chosen)}
    result["selectionSha256"] = canonical_sha256(result)
    return result


def verify_g0_bootstrap_selection(*, artifact: Mapping[str, Any], accepted_pool: Mapping[str, Any]) -> dict[str, Any]:
    claimed = _clone(dict(artifact)); identity = claimed.pop("selectionSha256", None)
    if _sha(identity, name="selectionSha256") != canonical_sha256(claimed):
        raise TemporalDiscoveryContractError("G0 selection artifact identity drift")
    expected = select_g0_bootstrap(accepted_pool=accepted_pool, evaluation_width=claimed.get("evaluationWidth"), policy=claimed.get("policy"))
    if claimed != {key: value for key, value in expected.items() if key != "selectionSha256"}:
        raise TemporalDiscoveryContractError("G0 selection artifact diverged from accepted pool")
    return _clone(expected)


__all__ = ["DEFAULT_POLICY", "DESCRIPTOR_AXES", "build_accepted_pool", "descriptor_vector", "materialize_campaign_ledger", "project_accepted_pair_entry", "read_selected_entry_from_journal", "select_g0_bootstrap", "verify_campaign_ledger", "verify_g0_bootstrap_selection", "verify_selected_reference_against_entry"]
