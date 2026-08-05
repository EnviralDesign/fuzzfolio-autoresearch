"""Reproducible multi-objective quality-diversity evolution for temporal graphs.

The worker remains an immutable evaluator.  This controller owns QD archive
selection, family-first reproduction, random immigrants, native validation,
append-only proposal journaling, and exact restart from deterministic proposal
ordinals.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    TemporalDiscoveryGenerationExhausted,
    _clone,
    _get,
    _sha,
    canonical_sha256,
)
from .temporal_discovery_mutation import (
    _apply_option,
    _available_mutations,
    _public_mutation,
)
from .temporal_discovery_results import (
    _aggregate_candidate,
    _require_candidate_execution_binding,
    _result_set_sha256,
    load_stage_results,
)
from .temporal_discovery_validation import (
    SubprocessCandidateValidator,
    build_authored_validation_binding,
    validate_authored_validation_binding,
    validate_legacy_reference_admission_binding,
    validator_provenance,
)
from .temporal_qd_evaluation_population import (
    evaluation_population_path,
    is_optimized_pair_population,
    load_evaluation_population,
)
from .result_codec import fsync_directory
from .lake_window import (
    LakeWindowBinding,
    lake_window_request_contains,
    resolve_replay_lake_window_request,
)
from .temporal_generator_v2_continuation import ExactGeneratorV2Continuation
from .temporal_operator_confirmed_entry import ConfirmedEntryStructuralOperator
from .temporal_operator_construction_v3 import (
    DIRECTION_FLIP,
    GRAPH_BOUND_TIMEFRAME,
    MANAGEMENT_PLAN,
    SCALAR_DYNAMIC_MANAGEMENT,
    GeneratorV3ConstructionRegistry,
    inspect_construction_reachability,
)
from .temporal_operator_expansion import expanded_structural_operators
from .temporal_search_policy_v2 import inspect_management_reachability
from .temporal_bidirectional_genome import (
    BidirectionalGenomeError,
    FrozenPair,
    IdentitySnapshot,
)

QD_VERSION = "temporal_qd_evolution_v3"
QD_ARCHIVE_SCHEMA = "temporal_qd_archive_v3"
_SMALL_POPULATION_FALLBACK_BYTES = 16 * 1024 * 1024
QD_CONFIG_SCHEMA = "temporal_qd_generation_config_v3"
QD_ENTRY_SCHEMA = "temporal_qd_proposal_entry_v3"
QD_CHECKPOINT_SCHEMA = "temporal_qd_generation_checkpoint_v3"
QD_POPULATION_SCHEMA = "temporal_qd_generation_population_v3"
QD_JOURNAL_SCHEMA = "temporal_qd_generation_journal_v3"
QD_MANIFEST_SCHEMA = "temporal_qd_generation_manifest_v3"
QD_IDENTITY_LEDGER_SCHEMA = "temporal_qd_identity_ledger_v3"
BIDIRECTIONAL_QD_POLICY_SCHEMA = "temporal_qd_bidirectional_pair_policy_v1"

QD_POLICY_NAME = "stage5e7_v3_robust_quality_archive"
QD_POLICY = {
    "schemaVersion": "temporal_qd_policy_v3",
    "policyName": QD_POLICY_NAME,
    "economicObjectives": [
        {"name": "worstWindowConservativeNetR", "direction": "max"},
        {"name": "maximumDrawdownR", "direction": "min"},
        {"name": "structuralComplexity", "direction": "min"},
    ],
    "tradeSupport": {
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "role": "eligibility_then_capped_tie_break",
    },
    "archive": {
        "defaultCellCapacity": 4,
        "lanes": {
            "quality": "finite_support_and_nonnegative_robust_return",
            "observational": "retained_without_quality_breeding_rights",
            "negativeNovelty": "finite_supported_negative_robust_return",
        },
        "negativeNoveltyMaxMembersPerCell": 1,
    },
    "parentSelection": {
        "quality": "pareto_front_then_crowding_then_robust_return_then_capped_support_then_complexity",
        "negativeNoveltyMaxFraction": 0.10,
        "negativeNoveltySchedule": "every_tenth_structural_parent_selection",
    },
    "identity": {
        "candidateIdentity": "reject_exact_repeat",
        "sourceProfile": "reject_same_evidence_repeat",
        "program": "allow_only_for_different_canonical_evidence",
        "canonicalEvidence": "candidate_program_ordered_window_semantic_cost_execution",
    },
    "resolvedExecutionDeduplication": {
        "required": True,
        "stage": "before_archive_reduction",
        "identity": "aggregate.resolvedProgramSha256",
        "representativeOrdering": [
            {"field": "finiteDataValidity.validForQuality", "direction": "max"},
            {"field": "objectives.worstWindowConservativeNetR", "direction": "max"},
            {"field": "cappedTradeSupport", "direction": "max"},
            {"field": "objectives.maximumDrawdownR", "direction": "min"},
            {"field": "objectives.structuralComplexity", "direction": "min"},
            {"field": "candidateId", "direction": "min"},
        ],
    },
}
QD_POLICY_SHA256 = canonical_sha256(QD_POLICY)

QD_OBJECTIVES = (
    ("worstWindowConservativeNetR", "max"),
    ("maximumDrawdownR", "min"),
    ("structuralComplexity", "min"),
)

PARAMETRIC_OPERATOR_VERSION = "temporal_parametric_mutation_operator_v1"
PARAMETRIC_FAMILY_IDS = {
    "entry_context": "parametric_entry_context_v1",
    "graph_structure": "parametric_graph_structure_v1",
    "management_closure": "parametric_management_closure_v1",
}
PARAMETRIC_OPERATOR_SPECS = {
    family_id: canonical_sha256(
        {
            "schemaVersion": "temporal_parametric_operator_spec_v1",
            "operatorId": family_id,
            "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
            "sourceFamily": source_family,
            "selectionPolicy": "uniform_occurrence_then_uniform_parameter_plan",
        }
    )
    for source_family, family_id in PARAMETRIC_FAMILY_IDS.items()
}

CONSTRUCTION_OPERATOR_IDS = frozenset(
    {
        SCALAR_DYNAMIC_MANAGEMENT,
        MANAGEMENT_PLAN,
        DIRECTION_FLIP,
        GRAPH_BOUND_TIMEFRAME,
    }
)
QD_CONSTRUCTION_POLICY_SCHEMA = "temporal_qd_construction_operator_policy_v1"

DEFAULT_QD_PARAMETERS: dict[str, Any] = {
    "version": QD_VERSION,
    "seed": 2026080101,
    "targetUniqueCandidates": 1024,
    "immigrantProposalFraction": 0.20,
    "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
    "maxCumulativeStructuralDepth": 16,
    "maxProposalAttempts": 20000,
    "minimumTotalTrades": 8,
    "minimumTradesPerWindow": 4,
    "capTrades": 20,
    "cellCapacity": 4,
}


def _bidirectional_pair_policy(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read the explicit opt-in gate; never infer pair mode from v3 profiles."""
    raw = payload.get("bidirectionalPairPolicy")
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or set(raw) != {"schemaVersion", "enabled", "compilerAuthority"}:
        raise TemporalDiscoveryContractError("QD bidirectional pair policy fields are not exact")
    if raw.get("schemaVersion") != BIDIRECTIONAL_QD_POLICY_SCHEMA or raw.get("enabled") is not True:
        raise TemporalDiscoveryContractError("QD bidirectional pair policy is not an enabled known version")
    try:
        compiler = IdentitySnapshot.from_payload(raw["compilerAuthority"], expected_kind="pairCompiler")
    except (BidirectionalGenomeError, KeyError, TypeError) as exc:
        raise TemporalDiscoveryContractError("QD bidirectional compiler authority is invalid") from exc
    result = {
        "schemaVersion": BIDIRECTIONAL_QD_POLICY_SCHEMA,
        "enabled": True,
        "compilerAuthority": compiler.canonical_payload(),
    }
    result["policySha256"] = canonical_sha256(result)
    return result


def _require_bidirectional_candidate(candidate: Mapping[str, Any], policy: Mapping[str, Any]) -> FrozenPair:
    """Admit only fully materialized, immutable v3/both economic candidates."""
    raw = candidate.get("bidirectionalGenome")
    if not isinstance(raw, Mapping):
        raise TemporalDiscoveryContractError("QD bidirectional candidate lacks frozen pair material")
    try:
        pair = FrozenPair.from_payload(raw)
    except BidirectionalGenomeError as exc:
        raise TemporalDiscoveryContractError("QD bidirectional pair material is invalid") from exc
    if pair.pair_compiler.canonical_payload() != policy["compilerAuthority"]:
        raise TemporalDiscoveryContractError("QD bidirectional pair compiler authority mismatch")
    profile = candidate.get("sourceProfile")
    if not isinstance(profile, Mapping) or canonical_sha256(profile) != pair.raw_pair_sha256 or profile.get("version") != "v3" or profile.get("directionMode") != "both":
        raise TemporalDiscoveryContractError("QD economic candidates must be the exact frozen v3/both pair")
    if candidate.get("sourceProfileSha256") != pair.raw_pair_sha256 or candidate.get("programSha256") != pair.native_program_sha256:
        raise TemporalDiscoveryContractError("QD bidirectional candidate compiled identities mismatch")
    material = candidate.get("candidateIdentityMaterial")
    if not isinstance(material, Mapping) or material.get("bidirectionalGenomeIdentitySha256") != pair.identity_sha256 or material.get("pairPolicySha256") != policy["policySha256"]:
        raise TemporalDiscoveryContractError("QD bidirectional candidate identity does not bind frozen pair material")
    return pair


def materialize_bidirectional_qd_candidate(
    *,
    pair: FrozenPair,
    pair_policy: Mapping[str, Any],
    origin_kind: str,
    generation_index: int,
    birth_ordinal: int,
    proposal_ordinal: int,
) -> dict[str, Any]:
    """Convert a frozen pair, never a v2 module, into one economic QD member.

    Factories and structural operators hand QD a fully compiled ``FrozenPair``.
    This boundary is deliberately small: it has no fallback that can accidentally
    emit a standalone module task or recover missing opposite-side material.
    """
    policy = _bidirectional_pair_policy({"bidirectionalPairPolicy": pair_policy})
    assert policy is not None
    if pair.pair_compiler.canonical_payload() != policy["compilerAuthority"]:
        raise TemporalDiscoveryContractError("bidirectional pair compiler authority does not match policy")
    if origin_kind not in {"random_immigrant", "structural_offspring"}:
        raise TemporalDiscoveryContractError("bidirectional QD origin kind is unknown")
    frozen_pair_payload = pair.canonical_payload()
    lineage = _clone(
        frozen_pair_payload["sideTargetedLineage"], name="bidirectional side lineage"
    )
    identity_material = {
        "schemaVersion": "temporal_qd_bidirectional_candidate_identity_v1",
        "qdEngineVersion": QD_VERSION,
        "originKind": origin_kind,
        "bidirectionalGenomeIdentitySha256": pair.identity_sha256,
        "pairPolicySha256": policy["policySha256"],
        "longModuleIdentitySha256": pair.long.identity_sha256,
        "shortModuleIdentitySha256": pair.short.identity_sha256,
        "longGrammarContextSha256": pair.long.grammar_context.sha256,
        "shortGrammarContextSha256": pair.short.grammar_context.sha256,
        "longCatalogSha256": pair.long.catalog.sha256,
        "shortCatalogSha256": pair.short.catalog.sha256,
        "longPolicySha256": pair.long.policy.sha256,
        "shortPolicySha256": pair.short.policy.sha256,
        "longNativeAuthoritySha256": pair.long.native_authority.sha256,
        "shortNativeAuthoritySha256": pair.short.native_authority.sha256,
        "pairCompilerAuthoritySha256": pair.pair_compiler.sha256,
        "compiledRawPairSha256": pair.raw_pair_sha256,
        "compiledProfileSha256": pair.profile_sha256,
        "compiledProgramSha256": pair.native_program_sha256,
        "compiledValidationReportSha256": pair.native_validation_report_sha256,
        "orderedSideLineage": lineage,
    }
    identity_sha = canonical_sha256(identity_material)
    candidate_id = "qd_" + identity_sha.removeprefix("sha256:")[:28]
    candidate = {
        "candidateId": candidate_id,
        "sourceMode": "qd_" + origin_kind + "_bidirectional_pair",
        "seedId": "bidirectional_pair",
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "sourceProfile": frozen_pair_payload["profile"],
        "sourceProfileSha256": pair.raw_pair_sha256,
        "profileSnapshotSha256": pair.profile_sha256,
        "programSha256": pair.native_program_sha256,
        "validationReportSha256": pair.native_validation_report_sha256,
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "structuralDepth": len(lineage),
        "structuralOperatorHistory": lineage,
        "mutationTrace": [],
        "activationAwareRepairs": [],
        "constructionEvidenceScope": _construction_evidence_scope([]),
        "bidirectionalGenome": frozen_pair_payload,
        "lineage": {
            "schemaVersion": "temporal_qd_bidirectional_candidate_lineage_v1",
            "candidateId": candidate_id,
            "candidateIdentitySha256": identity_sha,
            "pairIdentitySha256": pair.identity_sha256,
            "orderedSideLineage": lineage,
        },
    }
    _require_bidirectional_candidate(candidate, policy)
    return candidate


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _encoded(value: Mapping[str, Any]) -> str:
    return (
        json.dumps(
            dict(value),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(
                f"refusing to overwrite divergent immutable file: {path}"
            )
        return
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        try:
            # Hard-link publication preserves write-once semantics when a
            # concurrent process wins the name between our initial existence
            # check and publication.
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != encoded:
                raise TemporalDiscoveryContractError(
                    f"refusing to overwrite divergent immutable file: {path}"
                )
        else:
            fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _replace(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _identity_payload(payload: Mapping[str, Any], field: str, *, name: str) -> str:
    current = _clone(payload, name=name)
    supplied = _sha(current.pop(field, None), name=f"{name} {field}")
    if canonical_sha256(current) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    return supplied


def _load_population(path: Path) -> tuple[list[dict[str, Any]], str]:
    payload = _read(path, name="QD source population")
    if "populationSha256" in payload:
        identity = _identity_payload(
            payload, "populationSha256", name="QD source population"
        )
        values = payload.get("candidates")
        expected = payload.get("candidateCount")
    elif "setSha256" in payload:
        identity = _identity_payload(payload, "setSha256", name="QD source set")
        values = payload.get("values")
        expected = payload.get("count")
    else:
        raise TemporalDiscoveryContractError(
            "QD source population lacks a canonical population identity"
        )
    if not isinstance(values, list) or int(expected or -1) != len(values):
        raise TemporalDiscoveryContractError("QD source population count mismatch")
    require_authored_binding = payload.get("authoredValidationBindingRequired") is True
    require_legacy_binding = (
        payload.get("legacyReferenceAdmissionBindingRequired") is True
    )
    if require_authored_binding and require_legacy_binding:
        raise TemporalDiscoveryContractError(
            "QD source population cannot require both authored and legacy admission bindings"
        )
    bidirectional_policy = _bidirectional_pair_policy(payload)
    candidates = []
    for raw in values:
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError("QD candidate must be an object")
        candidate = _clone(raw, name="QD candidate")
        profile = candidate.get("sourceProfile")
        if not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("QD candidate profile is required")
        if canonical_sha256(profile) != _sha(
            candidate.get("sourceProfileSha256"),
            name="QD candidate source profile SHA-256",
        ):
            raise TemporalDiscoveryContractError(
                "QD candidate profile identity mismatch"
            )
        _sha(candidate.get("programSha256"), name="QD candidate program SHA-256")
        if bidirectional_policy is not None:
            _require_bidirectional_candidate(candidate, bidirectional_policy)
        if require_authored_binding:
            validate_authored_validation_binding(candidate)
        elif (
            "authoredValidationBinding" in candidate
            or "authoredValidationBindingSha256" in candidate
        ):
            validate_authored_validation_binding(candidate)
        if require_legacy_binding:
            validate_legacy_reference_admission_binding(candidate)
        elif (
            "legacyReferenceAdmissionBinding" in candidate
            or "legacyReferenceAdmissionBindingSha256" in candidate
        ):
            validate_legacy_reference_admission_binding(candidate)
        candidates.append(candidate)
    candidates.sort(key=lambda item: str(item["candidateId"]))
    if len({item["candidateId"] for item in candidates}) != len(candidates):
        raise TemporalDiscoveryContractError("QD candidate identities are not unique")
    return candidates, identity


def _finite(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric") from exc
    if not math.isfinite(result):
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric")
    return result


def _bucket(value: float, bounds: Sequence[float], labels: Sequence[str]) -> str:
    for bound, label in zip(bounds, labels, strict=True):
        if value < bound:
            return label
    return labels[-1]


def _walk_guards(value: Any):
    if not isinstance(value, Mapping):
        return
    yield value
    child = value.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guards(child)
    for child in value.get("guards") or []:
        if isinstance(child, Mapping):
            yield from _walk_guards(child)


def _graph_structure(candidate: Mapping[str, Any]) -> dict[str, Any]:
    profile = candidate.get("sourceProfile") or {}
    graph = profile.get("graph") or {}
    transitions = list(graph.get("transitions") or [])
    history = list(candidate.get("structuralOperatorHistory") or [])
    operator_ids = sorted(
        {
            str(item.get("operatorId"))
            for item in history
            if isinstance(item, Mapping) and item.get("operatorId")
        }
    )
    entry_event_ids: set[str] = set()
    management_action_count = 0
    guard_count = 0
    for transition in transitions:
        if not isinstance(transition, Mapping):
            continue
        actions = transition.get("actions") or []
        is_entry = any(
            isinstance(action, Mapping) and action.get("kind") == "enter_next_open"
            for action in actions
        )
        management_action_count += sum(
            isinstance(action, Mapping)
            and action.get("kind")
            in {
                "exit_next_open",
                "move_stop_to_break_even_next_open",
                "move_stop_next_open",
            }
            for action in actions
        )
        for guard in _walk_guards(transition.get("guard")):
            guard_count += 1
            if is_entry and guard.get("kind") in {
                "fresh_event",
                "event_age_at_most",
                "event_age_window",
            }:
                event_id = str(guard.get("eventId") or "")
                if event_id:
                    entry_event_ids.add(event_id)
    state_count = len(graph.get("states") or [])
    transition_count = len(transitions)
    indicator_count = len(profile.get("indicators") or [])
    return {
        "operatorFamilyIds": operator_ids,
        "operatorFamilyCount": len(operator_ids),
        "mutationDepth": len(history),
        "entryEventCount": len(entry_event_ids),
        "managementActionCount": int(management_action_count),
        "stateCount": state_count,
        "transitionCount": transition_count,
        "graphNodeCount": state_count + transition_count,
        "guardCount": guard_count,
        "indicatorCount": indicator_count,
        "structuralComplexity": (
            state_count + transition_count + 0.25 * guard_count + 0.5 * indicator_count
        ),
    }


def qd_behavior_descriptor(
    candidate: Mapping[str, Any], aggregate: Mapping[str, Any]
) -> dict[str, Any]:
    trades = int(aggregate.get("totalTrades") or 0)
    trade_frequency = _finite(
        aggregate.get("entryFrequencyPerThousand", 0.0),
        name="trade frequency",
    )
    holding = _finite(aggregate.get("medianHoldingBars", 0.0), name="holding")
    structure = _graph_structure(candidate)
    descriptor = {
        "operatorFamilies": _bucket(
            structure["operatorFamilyCount"],
            (1, 2, 3, math.inf),
            ("none", "one", "two", "three_plus"),
        ),
        "mutationDepth": _bucket(
            structure["mutationDepth"],
            (1, 2, 3, math.inf),
            ("root", "one", "two", "three_plus"),
        ),
        "entryEvents": _bucket(
            structure["entryEventCount"],
            (1, 2, math.inf),
            ("none", "one", "two_plus"),
        ),
        "managementActions": _bucket(
            structure["managementActionCount"],
            (1, 2, 3, math.inf),
            ("none", "one", "two", "three_plus"),
        ),
        "graphNodes": _bucket(
            structure["graphNodeCount"],
            (9, 13, 19, math.inf),
            ("small", "medium", "large", "very_large"),
        ),
        "tradeFrequency": (
            "dormant"
            if trades == 0
            else _bucket(
                trade_frequency,
                (1.0, 4.0, 12.0, math.inf),
                ("very_sparse", "sparse", "moderate", "active"),
            )
        ),
        "medianHolding": (
            "none"
            if trades == 0
            else _bucket(
                holding,
                (24.0, 96.0, 384.0, 1536.0, math.inf),
                ("short", "medium", "long", "very_long", "extreme"),
            )
        ),
    }
    descriptor["structuralMeasurements"] = structure
    descriptor["cellId"] = "|".join(
        str(descriptor[key])
        for key in (
            "operatorFamilies",
            "mutationDepth",
            "entryEvents",
            "managementActions",
            "graphNodes",
            "tradeFrequency",
            "medianHolding",
        )
    )
    return descriptor


def _objective_row(
    candidate: Mapping[str, Any], aggregate: Mapping[str, Any]
) -> dict[str, float]:
    # Invalid observations remain auditable in the broad archive.  Use neutral
    # finite placeholders here and let finiteDataValidity deny them all quality
    # retention and reproduction rights rather than serializing NaN/Infinity.
    def finite_or_neutral(value: Any) -> float:
        try:
            return _finite(value, name="QD economic objective")
        except TemporalDiscoveryContractError:
            return 0.0

    drawdown = max(0.0, finite_or_neutral(aggregate.get("maxWindowDrawdownR")))
    robust_return = finite_or_neutral(aggregate.get("worstWindowConservativeNetR"))
    return {
        "worstWindowConservativeNetR": robust_return,
        "maximumDrawdownR": drawdown,
        "structuralComplexity": float(
            _graph_structure(candidate)["structuralComplexity"]
        ),
    }


def _finite_data_validity(
    aggregate: Mapping[str, Any],
    *,
    minimum_total_trades: int,
    minimum_trades_per_window: int,
    cap_trades: int = 20,
) -> dict[str, Any]:
    try:
        counts = [int(value) for value in aggregate.get("tradeCountsByWindow") or []]
        total = int(aggregate.get("totalTrades") or 0)
        observations = int(aggregate.get("totalObservations") or 0)
        finite_economics = all(
            math.isfinite(float(aggregate.get(key)))
            for key in ("worstWindowConservativeNetR", "maxWindowDrawdownR")
        )
    except (TypeError, ValueError):
        counts = []
        total = 0
        observations = 0
        finite_economics = False
    checks = {
        "minimumTotalTrades": total >= minimum_total_trades,
        "minimumTradesEveryWindow": bool(counts)
        and all(value >= minimum_trades_per_window for value in counts),
        "positiveObservationSupport": observations > 0,
        "finiteEconomicMetrics": finite_economics,
    }
    passes_support_gate = (
        checks["minimumTotalTrades"]
        and checks["minimumTradesEveryWindow"]
        and checks["positiveObservationSupport"]
    )
    return {
        "minimumTotalTrades": minimum_total_trades,
        "minimumTradesPerWindow": minimum_trades_per_window,
        "capTrades": cap_trades,
        "tradeCountsByWindow": counts,
        "totalTrades": total,
        "checks": checks,
        "isFiniteData": finite_economics,
        "passesSupportGate": passes_support_gate,
        "validForQuality": finite_economics and passes_support_gate,
    }


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    no_worse = True
    better = False
    for key, direction in QD_OBJECTIVES:
        lvalue = float(left["objectives"][key])
        rvalue = float(right["objectives"][key])
        if direction == "max":
            no_worse = no_worse and lvalue >= rvalue
            better = better or lvalue > rvalue
        else:
            no_worse = no_worse and lvalue <= rvalue
            better = better or lvalue < rvalue
    return no_worse and better


def _pareto_fronts(rows: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    remaining = [_clone(item, name="QD archive member") for item in rows]
    remaining.sort(key=lambda item: str(item["candidateId"]))
    fronts = []
    while remaining:
        front = [
            row
            for row in remaining
            if not any(
                other["candidateId"] != row["candidateId"] and _dominates(other, row)
                for other in remaining
            )
        ]
        front.sort(key=lambda item: str(item["candidateId"]))
        fronts.append(front)
        selected = {item["candidateId"] for item in front}
        remaining = [item for item in remaining if item["candidateId"] not in selected]
    return fronts


def _crowding_order(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) <= 2:
        return [
            {
                **_clone(item, name="crowding member"),
                "crowdingDistance": None,
            }
            for item in sorted(
                rows,
                key=lambda item: (
                    -float(item["objectives"]["worstWindowConservativeNetR"]),
                    -_capped_trade_support(item),
                    float(item["objectives"]["structuralComplexity"]),
                    str(item["candidateId"]),
                ),
            )
        ]
    distances = {str(item["candidateId"]): 0.0 for item in rows}
    for key, direction in QD_OBJECTIVES:
        ordered = sorted(
            rows,
            key=lambda item: (
                float(item["objectives"][key]),
                str(item["candidateId"]),
            ),
        )
        distances[str(ordered[0]["candidateId"])] = math.inf
        distances[str(ordered[-1]["candidateId"])] = math.inf
        low = float(ordered[0]["objectives"][key])
        high = float(ordered[-1]["objectives"][key])
        scale = high - low
        if scale <= 0:
            continue
        for index in range(1, len(ordered) - 1):
            candidate_id = str(ordered[index]["candidateId"])
            if math.isinf(distances[candidate_id]):
                continue
            before = float(ordered[index - 1]["objectives"][key])
            after = float(ordered[index + 1]["objectives"][key])
            distances[candidate_id] += abs(after - before) / scale
    return sorted(
        (
            {
                **_clone(item, name="crowding member"),
                "crowdingDistance": (
                    None
                    if math.isinf(distances[str(item["candidateId"])])
                    else distances[str(item["candidateId"])]
                ),
            }
            for item in rows
        ),
        key=lambda item: (
            -distances[str(item["candidateId"])],
            -float(item["objectives"]["worstWindowConservativeNetR"]),
            -_capped_trade_support(item),
            float(item["objectives"]["structuralComplexity"]),
            str(item["candidateId"]),
        ),
    )


def _capped_trade_support(member: Mapping[str, Any]) -> float:
    value = member.get("cappedTradeSupport")
    if value is not None:
        return float(value)
    validity = member.get("finiteDataValidity") or {}
    cap = int(validity.get("capTrades") or QD_POLICY["tradeSupport"]["capTrades"])
    return float(min(max(0, int(validity.get("totalTrades") or 0)), cap))


def _quality_member(member: Mapping[str, Any]) -> bool:
    validity = member.get("finiteDataValidity") or {}
    return (
        validity.get("isFiniteData") is True
        and validity.get("passesSupportGate") is True
        and validity.get("validForQuality") is True
        and float(member["objectives"]["worstWindowConservativeNetR"]) >= 0.0
    )


def _negative_novelty_member(member: Mapping[str, Any]) -> bool:
    validity = member.get("finiteDataValidity") or {}
    return (
        validity.get("isFiniteData") is True
        and validity.get("passesSupportGate") is True
        and validity.get("validForQuality") is True
        and float(member["objectives"]["worstWindowConservativeNetR"]) < 0.0
    )


def _observational_order(member: Mapping[str, Any]) -> tuple[Any, ...]:
    validity = member.get("finiteDataValidity") or {}
    return (
        -(validity.get("isFiniteData") is True),
        -(validity.get("passesSupportGate") is True),
        -float(member["objectives"]["worstWindowConservativeNetR"]),
        -_capped_trade_support(member),
        float(member["objectives"]["structuralComplexity"]),
        str(member["candidateId"]),
    )


def resolved_execution_program_sha256(member: Mapping[str, Any]) -> str:
    """Return the execution program identity used for archive diversity.

    New-policy archive reduction accepts only the explicit resolved identity,
    which is intentionally distinct from the candidate's authored program.
    """
    aggregate = member.get("aggregate")
    if not isinstance(aggregate, Mapping):
        raise TemporalDiscoveryContractError("QD archive member lacks an aggregate")
    return _sha(
        aggregate.get("resolvedProgramSha256"),
        name="QD archive member resolved program SHA-256",
    )


def _resolved_execution_representative_key(
    member: Mapping[str, Any],
) -> tuple[Any, ...]:
    """Deterministic pre-reduction winner for identical execution programs."""
    validity = member.get("finiteDataValidity") or {}
    objectives = member.get("objectives") or {}
    return (
        -(validity.get("validForQuality") is True),
        -float(objectives.get("worstWindowConservativeNetR", 0.0)),
        -_capped_trade_support(member),
        float(objectives.get("maximumDrawdownR", 0.0)),
        float(objectives.get("structuralComplexity", 0.0)),
        str(member["candidateId"]),
    )


def deduplicate_resolved_execution_members(
    members: Sequence[Mapping[str, Any]],
    *,
    representative_key: Any | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep one deterministic member per resolved program before reduction.

    The result preserves authored candidates in the provenance record while
    ensuring execution-equivalent rows cannot consume multiple archive slots
    or become duplicate breeding parents.
    """
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        groups[resolved_execution_program_sha256(member)].append(member)
    key = representative_key or _resolved_execution_representative_key
    retained: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    for resolved_program_sha256 in sorted(groups):
        group = groups[resolved_program_sha256]
        winner = min(group, key=key)
        winner_id = str(winner["candidateId"])
        discarded_ids = sorted(
            str(row["candidateId"])
            for row in group
            if str(row["candidateId"]) != winner_id
        )
        row = _clone(winner, name="resolved execution deduplicated member")
        if discarded_ids:
            row["resolvedExecutionDuplicateCandidateIds"] = discarded_ids
            provenance.append(
                {
                    "resolvedProgramSha256": resolved_program_sha256,
                    "retainedCandidateId": winner_id,
                    "discardedCandidateIds": discarded_ids,
                }
            )
        retained.append(row)
    retained.sort(key=lambda item: str(item["candidateId"]))
    return retained, provenance


def select_qd_archive(
    members: Sequence[Mapping[str, Any]], *, cell_capacity: int = 4
) -> list[dict[str, Any]]:
    if not 1 <= cell_capacity <= 32:
        raise TemporalDiscoveryContractError("QD cell capacity must be 1..32")
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        groups[str(member["descriptor"]["cellId"])].append(member)
    cells = []
    for cell_id in sorted(groups):
        selected: list[dict[str, Any]] = []
        quality = [row for row in groups[cell_id] if _quality_member(row)]
        negative_novelty = [
            row for row in groups[cell_id] if _negative_novelty_member(row)
        ]
        observational = [
            row
            for row in groups[cell_id]
            if row not in quality and row not in negative_novelty
        ]
        if quality:
            fronts = _pareto_fronts(quality)
            for front_index, front in enumerate(fronts):
                ranked = _crowding_order(front)
                remaining = cell_capacity - len(selected)
                for row in ranked[:remaining]:
                    row["paretoFront"] = front_index
                    row["archiveLane"] = "quality"
                    row["retentionReason"] = "quality_pareto"
                    selected.append(row)
                if len(selected) == cell_capacity:
                    break
        if len(selected) < cell_capacity and negative_novelty:
            row = sorted(negative_novelty, key=_observational_order)[0]
            selected.append(
                {
                    **_clone(row, name="negative novelty archive member"),
                    "paretoFront": None,
                    "crowdingDistance": None,
                    "archiveLane": "negative_novelty",
                    "retentionReason": "negative_novelty_exploration",
                }
            )
        for row in sorted(observational, key=_observational_order)[
            : max(0, cell_capacity - len(selected))
        ]:
            selected.append(
                {
                    **_clone(row, name="observational archive member"),
                    "paretoFront": None,
                    "crowdingDistance": None,
                    "archiveLane": "observational",
                    "retentionReason": "observational_retention",
                }
            )
        cells.append(
            {
                "cellId": cell_id,
                "descriptor": _clone(selected[0]["descriptor"], name="QD descriptor"),
                "candidateCountBeforeCapacity": len(groups[cell_id]),
                "qualityEligibleCountBeforeCapacity": len(quality),
                "negativeNoveltyEligibleCountBeforeCapacity": len(negative_novelty),
                "observationalCountBeforeCapacity": len(observational),
                "breedingEligibleMemberCount": sum(
                    item.get("archiveLane") == "quality" for item in selected
                ),
                "negativeNoveltyMemberCount": sum(
                    item.get("archiveLane") == "negative_novelty" for item in selected
                ),
                "members": sorted(selected, key=lambda item: str(item["candidateId"])),
            }
        )
    return cells


def _hydrate_selected_pair_members(
    *,
    cells: Sequence[dict[str, Any]],
    projection: Mapping[str, Any],
    generation_journal_path: Path,
    pair_policy: Mapping[str, Any],
) -> None:
    """Restore rich pair provenance only for archive survivors.

    The compact evaluation sidecar owns reduction.  The append-only proposal
    journal remains the source of full pair lineage and is read one selected
    member at a time here.
    """

    by_id = {
        str(row["candidateId"]): row
        for row in projection.get("candidates") or []
        if isinstance(row, Mapping)
    }
    proposal_root = generation_journal_path.parent / "proposal-journal"
    for cell in cells:
        for member in cell["members"]:
            candidate_id = str(member["candidateId"])
            compact = by_id.get(candidate_id)
            if compact is None:
                existing = member.get("candidate")
                if not isinstance(existing, Mapping):
                    raise TemporalDiscoveryContractError(
                        "selected QD member is absent from evaluation population"
                    )
                # Previous-archive survivors were already hydrated from their
                # own immutable generation journal.  They must remain rich
                # provenance, but are deliberately absent from this generation's
                # compact evaluation population.
                _require_bidirectional_candidate(existing, pair_policy)
                continue
            ordinal = compact.get("proposalOrdinal")
            if not isinstance(ordinal, int) or ordinal < 0:
                raise TemporalDiscoveryContractError(
                    "selected QD member proposal reference is invalid"
                )
            path = proposal_root / f"{ordinal:08d}.json"
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise TemporalDiscoveryContractError(
                    f"could not read selected pair proposal journal entry: {path}"
                ) from exc
            if not isinstance(entry, Mapping) or entry.get("entrySha256") != compact.get(
                "proposalEntrySha256"
            ):
                raise TemporalDiscoveryContractError(
                    "selected QD member journal identity mismatch"
                )
            if canonical_sha256(
                {key: value for key, value in entry.items() if key != "entrySha256"}
            ) != entry.get("entrySha256"):
                raise TemporalDiscoveryContractError(
                    "selected QD member journal entry identity mismatch"
                )
            candidate = entry.get("candidate")
            if not isinstance(candidate, Mapping) or any(
                candidate.get(field) != compact.get(field)
                for field in (
                    "candidateId",
                    "sourceMode",
                    "seedId",
                    "candidateIdentitySha256",
                    "programSha256",
                    "sourceProfileSha256",
                    "profileSnapshotSha256",
                    "canonicalEvidenceIdentitySha256",
                )
            ):
                raise TemporalDiscoveryContractError(
                    "selected QD member rich provenance identity mismatch"
                )
            if candidate.get("sourceProfile") != compact.get("sourceProfile"):
                raise TemporalDiscoveryContractError(
                    "selected QD member rich provenance profile mismatch"
                )
            _require_bidirectional_candidate(candidate, pair_policy)
            member["candidate"] = _clone(candidate, name="selected pair candidate")


def build_qd_archive(
    *,
    population_path: Path | str,
    result_root: Path | str,
    output_path: Path | str,
    generation_index: int,
    previous_archive_path: Path | str | None = None,
    generation_journal_path: Path | str | None = None,
    cell_capacity: int = 4,
    minimum_total_trades: int = 8,
    minimum_trades_per_window: int = 4,
    cap_trades: int = 20,
) -> dict[str, Any]:
    if generation_index < 0:
        raise TemporalDiscoveryContractError("QD generation index must be nonnegative")
    if (
        minimum_total_trades != 8
        or minimum_trades_per_window != 4
        or cap_trades != 20
    ):
        raise TemporalDiscoveryContractError(
            "QD archive must use the frozen Stage 5E7-v3 trade-support policy"
        )
    population_file = Path(population_path)
    projection_file = evaluation_population_path(population_file)
    evaluation_population: Mapping[str, Any] | None = None
    if projection_file.is_file():
        if generation_journal_path is None:
            raise TemporalDiscoveryContractError(
                "optimized QD pair archive requires its generation journal"
            )
        evaluation_population = load_evaluation_population(
            population_path=population_file,
            journal_path=Path(generation_journal_path),
        )
        bidirectional_policy = _bidirectional_pair_policy(
            {"bidirectionalPairPolicy": evaluation_population["bidirectionalPairPolicy"]}
        )
        candidates = [
            _clone(row, name="QD evaluation population candidate")
            for row in evaluation_population["candidates"]
        ]
        population_sha = str(evaluation_population["populationSha256"])
    else:
        if population_file.stat().st_size > _SMALL_POPULATION_FALLBACK_BYTES:
            raise TemporalDiscoveryContractError(
                "optimized pre-sidecar QD pair population requires a fresh truthful root"
            )
        population_payload = _read(population_file, name="QD source population")
        if is_optimized_pair_population(population_payload):
            raise TemporalDiscoveryContractError(
                "optimized pre-sidecar QD pair population requires a fresh truthful root"
            )
        bidirectional_policy = _bidirectional_pair_policy(population_payload)
        candidates, population_sha = _load_population(population_file)
    candidate_map = {item["candidateId"]: item for item in candidates}
    results = load_stage_results(result_root)
    if set(results) != set(candidate_map):
        raise TemporalDiscoveryContractError(
            "QD result set must exactly cover the source population"
        )
    members: dict[str, dict[str, Any]] = {}
    previous_sha = None
    previous_cell_ids: set[str] = set()
    prior_cell_accounting: dict[str, dict[str, int]] = {}
    prior_candidate_count_seen = 0
    previous_member_ids: set[str] = set()
    if previous_archive_path is not None:
        previous, previous_sha = _load_archive(Path(previous_archive_path))
        if _bidirectional_pair_policy(previous) != bidirectional_policy:
            raise TemporalDiscoveryContractError("QD archive cannot mix bidirectional and legacy candidate policies")
        prior_candidate_count_seen = int(previous.get("candidateCountSeen") or 0)
        for cell in previous.get("cells") or []:
            cell_id = str(cell["cellId"])
            previous_cell_ids.add(cell_id)
            prior_cell_accounting[cell_id] = {
                "selectionVisitCount": int(cell.get("selectionVisitCount") or 0),
                "offspringAttemptCount": int(cell.get("offspringAttemptCount") or 0),
            }
            for member in cell.get("members") or []:
                previous_member_ids.add(str(member["candidateId"]))
                members[str(member["candidateId"])] = _clone(
                    member, name="previous QD member"
                )
    for candidate_id in sorted(candidate_map):
        candidate = candidate_map[candidate_id]
        windows = results[candidate_id]
        execution_binding = _require_candidate_execution_binding(candidate, windows)
        if any(window.get("v3Admissible") is not True for window in windows):
            raise TemporalDiscoveryContractError(
                "QD v3 archive requires terminal-adjusted Stage 5E7-v3 result evidence"
            )
        aggregate = _aggregate_candidate(candidate, windows)
        if (
            aggregate.get("authoredProgramSha256")
            != execution_binding["authoredProgramSha256"]
            or aggregate.get("sourceProfileSnapshotSha256")
            != execution_binding["sourceProfileSnapshotSha256"]
            or aggregate.get("resolvedProfileSnapshotSha256")
            != execution_binding["resolvedProfileSnapshotSha256"]
            or aggregate.get("resolvedProgramSha256")
            != execution_binding["resolvedProgramSha256"]
            or aggregate.get("programSha256")
            != execution_binding["resolvedProgramSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD aggregate execution identity does not match result execution binding"
            )
        if aggregate.get("v3Admissible") is not True:
            raise TemporalDiscoveryContractError(
                "QD v3 archive requires terminal-adjusted Stage 5E7-v3 result evidence"
            )
        descriptor = qd_behavior_descriptor(candidate, aggregate)
        members[candidate_id] = {
            "candidateId": candidate_id,
            "generationIndex": generation_index,
            "candidate": candidate,
            "aggregate": aggregate,
            "descriptor": descriptor,
            "objectives": _objective_row(candidate, aggregate),
            "finiteDataValidity": _finite_data_validity(
                aggregate,
                minimum_total_trades=minimum_total_trades,
                minimum_trades_per_window=minimum_trades_per_window,
                cap_trades=cap_trades,
            ),
            "cappedTradeSupport": float(
                min(max(0, int(aggregate.get("totalTrades") or 0)), cap_trades)
            ),
        }
    members_before_resolved_deduplication = list(members.values())
    reduced_members, resolved_duplicate_provenance = (
        deduplicate_resolved_execution_members(
            members_before_resolved_deduplication
        )
    )
    cells = select_qd_archive(reduced_members, cell_capacity=cell_capacity)
    if evaluation_population is not None:
        assert generation_journal_path is not None and bidirectional_policy is not None
        _hydrate_selected_pair_members(
            cells=cells,
            projection=evaluation_population,
            generation_journal_path=Path(generation_journal_path),
            pair_policy=bidirectional_policy,
        )
    generation_accounting: dict[str, Any] = {}
    if generation_journal_path is not None:
        journal = _read(Path(generation_journal_path), name="QD generation journal")
        _identity_payload(journal, "journalSha256", name="QD generation journal")
        generation_accounting = {
            key: _clone(journal.get(key) or {}, name=f"QD journal {key}")
            for key in (
                "originProposalCounts",
                "originAcceptedCounts",
                "parentSelectionModeCounts",
                "parentLaneCounts",
                "parentLaneReasonCounts",
                "negativeNoveltyParentSelectionCount",
                "structuralParentSelectionCount",
                "negativeNoveltyParentSelectionFraction",
                "parentCellSelectionCounts",
                "parentCellOffspringAttemptCounts",
                "operatorFamilyAttemptCounts",
                "operatorFamilyApplicationCounts",
                "mutationDepthAttemptCounts",
                "dispositionCounts",
            )
        }
    new_cell_counts = generation_accounting.get("parentCellSelectionCounts") or {}
    new_attempt_counts = (
        generation_accounting.get("parentCellOffspringAttemptCounts") or {}
    )
    for cell in cells:
        cell_id = str(cell["cellId"])
        previous_counts = prior_cell_accounting.get(cell_id, {})
        cell["selectionVisitCount"] = int(
            previous_counts.get("selectionVisitCount", 0)
        ) + int(new_cell_counts.get(cell_id, 0))
        cell["offspringAttemptCount"] = int(
            previous_counts.get("offspringAttemptCount", 0)
        ) + int(new_attempt_counts.get(cell_id, 0))
    selected_member_ids = {
        str(member["candidateId"]) for cell in cells for member in cell["members"]
    }
    admitted_ids = selected_member_ids - previous_member_ids
    evicted_ids = previous_member_ids - selected_member_ids
    family_survivors: Counter[str] = Counter()
    for cell in cells:
        for member in cell["members"]:
            if str(member["candidateId"]) not in admitted_ids:
                continue
            history = member["candidate"].get("structuralOperatorHistory") or []
            for operator_id in {
                str(item.get("operatorId"))
                for item in history
                if isinstance(item, Mapping) and item.get("operatorId")
            }:
                family_survivors[operator_id] += 1
    authored_programs_seen = {
        str(
            member["aggregate"].get("authoredProgramSha256")
            or member["candidate"].get("programSha256")
        )
        for member in members_before_resolved_deduplication
    }
    resolved_programs_seen = {
        resolved_execution_program_sha256(member)
        for member in members_before_resolved_deduplication
    }
    archive = {
        "schemaVersion": QD_ARCHIVE_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="frozen QD policy"),
        "generationIndex": generation_index,
        "populationSha256": population_sha,
        **(
            {"evaluationPopulationSha256": evaluation_population["evaluationPopulationSha256"]}
            if evaluation_population is not None
            else {}
        ),
        "resultSetSha256": _result_set_sha256(results),
        "previousArchiveSha256": previous_sha,
        "cellCapacity": cell_capacity,
        "qualityEligibilityPolicy": {
            "minimumTotalTrades": minimum_total_trades,
            "minimumTradesPerWindow": minimum_trades_per_window,
            "capTrades": cap_trades,
            "qualityRequiresFiniteData": True,
            "qualityRequiresNonnegativeRobustReturn": True,
            "undersupportedRetainedOnlyInObservationalLane": True,
        },
        "archiveRetentionPolicy": _clone(QD_POLICY["archive"], name="QD archive policy"),
        "parentSelectionPolicy": _clone(
            QD_POLICY["parentSelection"], name="QD parent policy"
        ),
        "objectives": [
            {"name": name, "direction": direction} for name, direction in QD_OBJECTIVES
        ],
        "candidateCountSeen": prior_candidate_count_seen + len(candidate_map),
        "candidateCountReducedThisGeneration": len(candidate_map),
        "authoredCandidateCountBeforeResolvedDeduplication": len(
            members_before_resolved_deduplication
        ),
        "authoredProgramCountBeforeResolvedDeduplication": len(
            authored_programs_seen
        ),
        "resolvedProgramCountBeforeReduction": len(resolved_programs_seen),
        "resolvedProgramDuplicateCount": sum(
            len(row["discardedCandidateIds"])
            for row in resolved_duplicate_provenance
        ),
        "resolvedExecutionDeduplication": {
            "schemaVersion": "temporal_qd_resolved_execution_deduplication_v1",
            "stage": "before_archive_reduction",
            "frozenPolicy": _clone(
                QD_POLICY["resolvedExecutionDeduplication"],
                name="resolved execution deduplication policy",
            ),
            "inputMemberCount": len(members_before_resolved_deduplication),
            "uniqueResolvedProgramCount": len(reduced_members),
            "duplicates": resolved_duplicate_provenance,
        },
        **({"bidirectionalPairPolicy": {key: value for key, value in bidirectional_policy.items() if key != "policySha256"}} if bidirectional_policy is not None else {}),
        "occupiedCellCount": len(cells),
        "newCellCount": len(
            {str(cell["cellId"]) for cell in cells} - previous_cell_ids
        ),
        "memberCount": sum(len(cell["members"]) for cell in cells),
        "qualityMemberCount": sum(
            member.get("archiveLane") == "quality"
            for cell in cells
            for member in cell["members"]
        ),
        "observationalMemberCount": sum(
            member.get("archiveLane") == "observational"
            for cell in cells
            for member in cell["members"]
        ),
        "negativeNoveltyMemberCount": sum(
            member.get("archiveLane") == "negative_novelty"
            for cell in cells
            for member in cell["members"]
        ),
        "paretoAdmissionCount": len(admitted_ids),
        "paretoEvictionCount": len(evicted_ids),
        "survivingDescendantsByOperatorFamily": dict(sorted(family_survivors.items())),
        "generationProposalAccounting": generation_accounting,
        "cells": cells,
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    _write_once(Path(output_path), archive)
    return {
        "schemaVersion": "temporal_qd_archive_result_v3",
        "archiveSha256": archive["archiveSha256"],
        "candidateCountSeen": archive["candidateCountSeen"],
        "occupiedCellCount": archive["occupiedCellCount"],
        "memberCount": archive["memberCount"],
        "qualityMemberCount": archive["qualityMemberCount"],
        "observationalMemberCount": archive["observationalMemberCount"],
        "negativeNoveltyMemberCount": archive["negativeNoveltyMemberCount"],
        "newCellCount": archive["newCellCount"],
        "paretoAdmissionCount": archive["paretoAdmissionCount"],
        "paretoEvictionCount": archive["paretoEvictionCount"],
    }


def _normalize_parameters(parameters: Mapping[str, Any] | None) -> dict[str, Any]:
    value = _clone(parameters or DEFAULT_QD_PARAMETERS, name="QD parameters")
    required = {
        "version",
        "seed",
        "targetUniqueCandidates",
        "immigrantProposalFraction",
        "mutationDepthProbabilities",
        "maxCumulativeStructuralDepth",
        "maxProposalAttempts",
        "minimumTotalTrades",
        "minimumTradesPerWindow",
        "capTrades",
        "cellCapacity",
    }
    if set(value) != required or value["version"] != QD_VERSION:
        raise TemporalDiscoveryContractError("QD parameters have an unknown schema")
    value["seed"] = int(value["seed"])
    value["targetUniqueCandidates"] = int(value["targetUniqueCandidates"])
    value["immigrantProposalFraction"] = _finite(
        value["immigrantProposalFraction"], name="immigrant proposal fraction"
    )
    value["maxCumulativeStructuralDepth"] = int(value["maxCumulativeStructuralDepth"])
    value["maxProposalAttempts"] = int(value["maxProposalAttempts"])
    value["minimumTotalTrades"] = int(value["minimumTotalTrades"])
    value["minimumTradesPerWindow"] = int(value["minimumTradesPerWindow"])
    value["capTrades"] = int(value["capTrades"])
    value["cellCapacity"] = int(value["cellCapacity"])
    probabilities = value["mutationDepthProbabilities"]
    if not isinstance(probabilities, Mapping) or set(probabilities) != {"1", "2", "3"}:
        raise TemporalDiscoveryContractError(
            "QD mutation depth probabilities require depths 1, 2, and 3"
        )
    value["mutationDepthProbabilities"] = {
        key: _finite(probabilities[key], name=f"mutation depth {key} probability")
        for key in ("1", "2", "3")
    }
    if not 1 <= value["targetUniqueCandidates"] <= 100_000:
        raise TemporalDiscoveryContractError("QD target is outside 1..100000")
    if value["immigrantProposalFraction"] != 0.20:
        raise TemporalDiscoveryContractError(
            "the initial QD immigrant proposal fraction is frozen at 0.20"
        )
    if abs(sum(value["mutationDepthProbabilities"].values()) - 1.0) > 1e-12 or value[
        "mutationDepthProbabilities"
    ] != {"1": 0.70, "2": 0.25, "3": 0.05}:
        raise TemporalDiscoveryContractError(
            "the initial QD mutation depth distribution is frozen at 70/25/5"
        )
    if not 3 <= value["maxCumulativeStructuralDepth"] <= 32:
        raise TemporalDiscoveryContractError(
            "maximum cumulative structural depth is outside 3..32"
        )
    if value["maxProposalAttempts"] < value["targetUniqueCandidates"]:
        raise TemporalDiscoveryContractError("QD proposal ceiling is below its target")
    if (
        value["minimumTotalTrades"] != 8
        or value["minimumTradesPerWindow"] != 4
        or value["capTrades"] != 20
    ):
        raise TemporalDiscoveryContractError(
            "the Stage 5E7-v3 trade-support policy is frozen at 8 total / 4 per window / cap 20"
        )
    if value["minimumTotalTrades"] < 1 or value["minimumTradesPerWindow"] < 1:
        raise TemporalDiscoveryContractError("QD finite-data floors must be positive")
    if not 1 <= value["cellCapacity"] <= 32:
        raise TemporalDiscoveryContractError("QD cell capacity is outside 1..32")
    return value


def qd_construction_operator_policy(
    construction_catalog_path: Path | str | None,
) -> tuple[dict[str, Any], GeneratorV3ConstructionRegistry | None]:
    """Load the opt-in v3 construction registry as frozen QD input.

    A missing catalog intentionally retains the pre-construction QD operator
    set for already-frozen runs.  New supervisor invocations require a catalog
    at the CLI boundary, so the live QD path always records this policy.
    """

    if construction_catalog_path is None:
        policy: dict[str, Any] = {
            "schemaVersion": QD_CONSTRUCTION_POLICY_SCHEMA,
            "enabled": False,
            "catalog": None,
            "enabledOperatorIds": [],
            "deferredOperators": [],
            "disabledReason": "no_construction_catalog_supplied",
        }
        policy["policySha256"] = canonical_sha256(policy)
        return policy, None
    catalog_path = Path(construction_catalog_path).resolve()
    catalog = _read(catalog_path, name="QD construction catalog")
    registry = GeneratorV3ConstructionRegistry(catalog)
    policy = {
        "schemaVersion": QD_CONSTRUCTION_POLICY_SCHEMA,
        "enabled": True,
        "catalog": {
            "path": str(catalog_path),
            "catalogSha256": registry.catalog.catalog_sha256,
        },
        "enabledOperatorIds": list(registry.enabled_operator_ids),
        "conditionalOperatorEligibility": [
            {
                "operatorId": GRAPH_BOUND_TIMEFRAME,
                "eligibility": (
                    "candidate_derived_request_is_contained_by_every_"
                    "immutable_pre_attested_window_binding"
                ),
                "outOfScopeDisposition": "predeclared_lake_scope_rejected",
                "bindingPolicy": "reuse_only_no_local_semantic_rehash",
            }
        ],
        "deferredOperators": _clone(
            registry.policy["deferredOperators"], name="deferred construction operators"
        ),
        "registryPolicy": _clone(registry.policy, name="construction registry policy"),
    }
    policy["policySha256"] = canonical_sha256(policy)
    return policy, registry


def _operators(
    construction_registry: GeneratorV3ConstructionRegistry | None = None,
    *,
    construction_operator_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    values = [ConfirmedEntryStructuralOperator(), *expanded_structural_operators()]
    if construction_registry is not None:
        allowed = (
            tuple(construction_operator_ids)
            if construction_operator_ids is not None
            else construction_registry.enabled_operator_ids
        )
        values.extend(
            construction_registry.get(operator_id)
            for operator_id in allowed
        )
    result = {item.operator_id: item for item in values}
    if len(result) != len(values):
        raise TemporalDiscoveryContractError("QD operator registry has duplicate IDs")
    return result


def _static_reachability(
    profile: Mapping[str, Any], *, operator_id: str | None = None
) -> dict[str, Any]:
    """Use the v3 closure checker for construction transactions only."""

    if operator_id in CONSTRUCTION_OPERATOR_IDS:
        return inspect_construction_reachability(profile)
    return inspect_management_reachability(profile)


def _construction_evidence_scope(steps: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Make evidence/lake invalidation explicit in candidate lineage."""

    timeframe_steps = [
        step
        for step in steps
        if step.get("operatorId") == GRAPH_BOUND_TIMEFRAME
        and step.get("disposition") == "applied"
    ]
    scope = {
        "schemaVersion": "temporal_qd_construction_evidence_scope_v1",
        "evidencePlanRotationRequired": bool(timeframe_steps),
        "lakeScopeRegenerationRequired": bool(timeframe_steps),
        "reasons": (
            ["graph_bound_indicator_timeframe_changed"] if timeframe_steps else []
        ),
        "timeframeMutationTraceSha256s": [
            canonical_sha256(step.get("mutationTrace") or [])
            for step in timeframe_steps
        ],
    }
    scope["evidenceScopeSha256"] = canonical_sha256(scope)
    return scope


def _predeclared_lake_scope_report(
    profile: Mapping[str, Any],
    evidence_context: Mapping[str, Any] | None,
    *,
    frozen_construction_catalog: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Prove that frozen evidence can evaluate a constructed profile.

    The window semantic digest is a remote attestation, so this check never
    mutates a binding or synthesizes one.  A graph-bound timeframe construction
    is eligible only when its canonical request is contained by *every*
    pre-attested development-window request.
    """

    if not isinstance(evidence_context, Mapping):
        return {
            "acceptable": False,
            "reason": "predeclared_lake_scope_absent",
            "windows": [],
        }
    base_timeframe = str(
        evidence_context.get("baseDecisionTimeframe") or ""
    ).strip().upper()
    pairs = profile.get("instruments")
    windows = evidence_context.get("orderedWindowPlanSemantic")
    if not base_timeframe or not isinstance(pairs, list) or not pairs or not isinstance(windows, list) or not windows:
        return {
            "acceptable": False,
            "reason": "predeclared_lake_scope_context_incomplete",
            "windows": [],
        }

    reports: list[dict[str, Any]] = []
    for raw_window in windows:
        if not isinstance(raw_window, Mapping):
            return {
                "acceptable": False,
                "reason": "predeclared_lake_scope_context_malformed",
                "windows": reports,
            }
        window_id = str(raw_window.get("windowId") or "")
        window = raw_window.get("window")
        plan = raw_window.get("evidencePlanSemantic")
        if not window_id or not isinstance(window, Mapping) or not isinstance(plan, Mapping):
            return {
                "acceptable": False,
                "reason": "predeclared_lake_scope_context_malformed",
                "windows": reports,
            }
        try:
            binding = LakeWindowBinding.model_validate(plan.get("lake_window_binding"))
            required = resolve_replay_lake_window_request(
                pairs=[str(pair) for pair in pairs],
                base_timeframe=base_timeframe,
                profile_snapshot=profile,
                analysis_window_start=str(window.get("analysisWindowStart") or ""),
                analysis_window_end=str(window.get("analysisWindowEnd") or ""),
                frozen_catalog=frozen_construction_catalog,
            )
        except (TypeError, ValueError):
            return {
                "acceptable": False,
                "reason": "predeclared_lake_scope_context_malformed",
                "windows": reports,
            }
        contained = lake_window_request_contains(binding.request, required)
        reports.append(
            {
                "windowId": window_id,
                "contained": contained,
                "requiredRequest": required.canonical_payload(),
                "frozenWindowSemanticSha256": binding.window_semantic_sha256,
            }
        )
        if not contained:
            return {
                "acceptable": False,
                "reason": "candidate_derived_request_outside_pre_attested_scope",
                "windows": reports,
            }
    return {"acceptable": True, "reason": None, "windows": reports}


def _load_archive(path: Path) -> tuple[dict[str, Any], str]:
    archive = _read(path, name="QD parent archive")
    archive_sha = _identity_payload(archive, "archiveSha256", name="QD parent archive")
    archive["archiveSha256"] = archive_sha
    if (
        archive.get("schemaVersion") != QD_ARCHIVE_SCHEMA
        or archive.get("qdVersion") != QD_VERSION
        or archive.get("policyName") != QD_POLICY_NAME
        or archive.get("policySha256") != QD_POLICY_SHA256
        or archive.get("frozenPolicy") != QD_POLICY
    ):
        raise TemporalDiscoveryContractError("unknown QD archive schema")
    bidirectional_policy = _bidirectional_pair_policy(archive)
    if bidirectional_policy is not None:
        for cell in archive.get("cells") or []:
            if not isinstance(cell, Mapping):
                raise TemporalDiscoveryContractError("QD bidirectional archive cell is invalid")
            for member in cell.get("members") or []:
                if not isinstance(member, Mapping) or not isinstance(member.get("candidate"), Mapping):
                    raise TemporalDiscoveryContractError("QD bidirectional archive member lacks candidate")
                _require_bidirectional_candidate(member["candidate"], bidirectional_policy)
    return archive, archive_sha


def initialize_empty_bidirectional_archive(
    template: Mapping[str, Any], pair_policy: Mapping[str, Any]
) -> dict[str, Any]:
    """Bind a verified empty generation-zero archive to one pair authority."""

    archive = _clone(template, name="empty QD archive template")
    supplied = archive.pop("archiveSha256", None)
    if supplied != canonical_sha256(archive):
        raise TemporalDiscoveryContractError("empty QD archive template identity mismatch")
    if (
        archive.get("schemaVersion") != QD_ARCHIVE_SCHEMA
        or archive.get("qdVersion") != QD_VERSION
        or archive.get("policyName") != QD_POLICY_NAME
        or archive.get("policySha256") != QD_POLICY_SHA256
        or archive.get("frozenPolicy") != QD_POLICY
    ):
        raise TemporalDiscoveryContractError("unknown empty QD archive template schema")
    if (
        archive.get("generationIndex") != 0
        or archive.get("candidateCountSeen") != 0
        or archive.get("occupiedCellCount") != 0
        or archive.get("memberCount") != 0
        or archive.get("qualityMemberCount") != 0
        or archive.get("observationalMemberCount") != 0
        or archive.get("negativeNoveltyMemberCount") != 0
        or archive.get("cells") != []
    ):
        raise TemporalDiscoveryContractError("pair archive initialization requires an exact empty generation-zero template")
    policy = _clone(pair_policy, name="bidirectional pair policy")
    if _bidirectional_pair_policy({"bidirectionalPairPolicy": policy}) is None:
        raise TemporalDiscoveryContractError("pair archive initialization requires an enabled pair policy")
    archive["bidirectionalPairPolicy"] = policy
    archive["archiveSha256"] = canonical_sha256(archive)
    return archive


def canonical_empty_bidirectional_archive_template() -> dict[str, Any]:
    """Return the canonical, unbound generation-zero QD archive template.

    This deliberately contains no candidate, result, quality, or archive-lane
    material.  The two content hashes identify canonical empty inputs rather
    than pretending that an evaluator ran.  ``initialize_empty_bidirectional_archive``
    supplies the only run-specific value: the closed pair compiler authority.
    """

    empty_population = {
        "schemaVersion": QD_POPULATION_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "generationIndex": 0,
        "candidateCount": 0,
        "candidates": [],
    }
    empty_results = {
        "schemaVersion": "temporal_qd_empty_result_set_v1",
        "generationIndex": 0,
        "results": [],
    }
    archive = {
        "schemaVersion": QD_ARCHIVE_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="frozen QD policy"),
        "generationIndex": 0,
        "populationSha256": canonical_sha256(empty_population),
        "resultSetSha256": canonical_sha256(empty_results),
        "previousArchiveSha256": None,
        "cellCapacity": int(QD_POLICY["archive"]["defaultCellCapacity"]),
        "candidateCountSeen": 0,
        "occupiedCellCount": 0,
        "memberCount": 0,
        "qualityMemberCount": 0,
        "observationalMemberCount": 0,
        "negativeNoveltyMemberCount": 0,
        "cells": [],
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    return archive


def _reproduction_cells(
    archive: Mapping[str, Any], *, allow_empty_quality_bootstrap: bool = False
) -> list[dict[str, Any]]:
    eligible = []
    for cell in archive.get("cells") or []:
        members = [
            member
            for member in cell.get("members") or []
            if member.get("archiveLane") == "quality" and _quality_member(member)
        ]
        if members:
            filtered = _clone(cell, name="eligible QD reproduction cell")
            filtered["members"] = members
            eligible.append(filtered)
    if not eligible and not allow_empty_quality_bootstrap:
        raise TemporalDiscoveryContractError(
            "QD archive has no quality-eligible reproduction members"
        )
    return sorted(eligible, key=lambda item: str(item["cellId"]))


def _negative_novelty_cells(archive: Mapping[str, Any]) -> list[dict[str, Any]]:
    cells = []
    for cell in archive.get("cells") or []:
        members = [
            member
            for member in cell.get("members") or []
            if member.get("archiveLane") == "negative_novelty"
            and _negative_novelty_member(member)
        ]
        if len(members) > 1:
            raise TemporalDiscoveryContractError(
                "QD negative-novelty lane exceeds one member per cell"
            )
        if members:
            copied = _clone(cell, name="negative novelty reproduction cell")
            copied["members"] = members
            cells.append(copied)
    return sorted(cells, key=lambda item: str(item["cellId"]))


def _proposal_seed(config_sha: str, generation_index: int, ordinal: int) -> str:
    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_proposal_seed_v1",
            "configSha256": config_sha,
            "generationIndex": generation_index,
            "proposalOrdinal": ordinal,
        }
    )


def _proposal_rng(seed_sha: str) -> random.Random:
    return random.Random(int(seed_sha.removeprefix("sha256:"), 16))


def _origin_kind(
    proposal_ordinal: int, *, random_immigrant_only: bool = False
) -> str:
    if random_immigrant_only:
        return "random_immigrant"
    # One exact immigrant slot in every five proposals.  Accepted composition is
    # intentionally not quota-forced; both raw and accepted counts are reported.
    return "random_immigrant" if proposal_ordinal % 5 == 4 else "structural_offspring"


def _negative_novelty_slot(
    proposal_ordinal: int, *, random_immigrant_only: bool = False
) -> bool:
    """Reserve at most one in ten structural parent selections for the lane."""
    if (
        _origin_kind(
            proposal_ordinal, random_immigrant_only=random_immigrant_only
        )
        != "structural_offspring"
    ):
        return False
    structural_selection_count = (proposal_ordinal + 1) - (
        (proposal_ordinal + 1) // 5
    )
    return structural_selection_count % 10 == 0


def _descriptor_coordinates(cell: Mapping[str, Any]) -> tuple[str, ...]:
    descriptor = cell.get("descriptor") or {}
    return tuple(
        str(descriptor.get(key))
        for key in (
            "operatorFamilies",
            "mutationDepth",
            "entryEvents",
            "managementActions",
            "graphNodes",
            "tradeFrequency",
            "medianHolding",
        )
    )


def _boundary_cell_ids(cells: Sequence[Mapping[str, Any]]) -> set[str]:
    coordinates = {str(cell["cellId"]): _descriptor_coordinates(cell) for cell in cells}
    neighbor_counts: dict[str, int] = {}
    for cell_id, left in coordinates.items():
        neighbor_counts[cell_id] = sum(
            sum(a != b for a, b in zip(left, right, strict=True)) == 1
            for other_id, right in coordinates.items()
            if other_id != cell_id
        )
    minimum = min(neighbor_counts.values(), default=0)
    return {key for key, value in neighbor_counts.items() if value == minimum}


def _initial_selection_state(
    cells: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    return {
        str(cell["cellId"]): {
            "selectionVisitCount": int(cell.get("selectionVisitCount") or 0),
            "offspringAttemptCount": int(cell.get("offspringAttemptCount") or 0),
        }
        for cell in cells
    }


def _parent_member_order(member: Mapping[str, Any]) -> tuple[Any, ...]:
    crowding = member.get("crowdingDistance")
    crowding_value = float(crowding) if crowding is not None else math.inf
    return (
        int(member.get("paretoFront") or 0),
        -crowding_value,
        -float(member["objectives"]["worstWindowConservativeNetR"]),
        -_capped_trade_support(member),
        float(member["objectives"]["structuralComplexity"]),
        str(member["candidateId"]),
    )


def _rank_aware_parent_member(
    members: Sequence[Mapping[str, Any]], *, rng: random.Random
) -> dict[str, Any]:
    ordered = sorted(members, key=_parent_member_order)
    # Descending rank weights retain seeded exploration without letting a flat
    # uniform draw erase front/crowding/support ordering.
    weights = list(range(len(ordered), 0, -1))
    draw = rng.randrange(sum(weights))
    for member, weight in zip(ordered, weights, strict=True):
        if draw < weight:
            return member
        draw -= weight
    raise AssertionError("rank-aware parent draw exhausted")


def _select_parent(
    *,
    rng: random.Random,
    cells: Sequence[Mapping[str, Any]],
    negative_novelty_cells: Sequence[Mapping[str, Any]],
    selection_state: dict[str, dict[str, int]],
    negative_novelty_slot: bool,
) -> tuple[dict[str, Any], dict[str, Any], str, str, str]:
    if negative_novelty_slot and negative_novelty_cells:
        cell = negative_novelty_cells[rng.randrange(len(negative_novelty_cells))]
        cell_id = str(cell["cellId"])
        member = _rank_aware_parent_member(cell["members"], rng=rng)
        selection_state[cell_id]["selectionVisitCount"] += 1
        selection_state[cell_id]["offspringAttemptCount"] += 1
        return (
            cell,
            member,
            "negative_novelty_exploration",
            "negative_novelty",
            "scheduled_every_tenth_structural_parent_selection",
        )
    draw = rng.random()
    if draw < 0.50:
        mode = "uniform_occupied_cell"
        pool = list(cells)
    elif draw < 0.80:
        mode = "low_visit_cell"
        minimum = min(
            selection_state[str(cell["cellId"])]["selectionVisitCount"]
            for cell in cells
        )
        pool = [
            cell
            for cell in cells
            if selection_state[str(cell["cellId"])]["selectionVisitCount"] == minimum
        ]
    else:
        mode = "sparse_descriptor_boundary"
        boundary = _boundary_cell_ids(cells)
        pool = [cell for cell in cells if str(cell["cellId"]) in boundary]
    cell = pool[rng.randrange(len(pool))]
    cell_id = str(cell["cellId"])
    member = _rank_aware_parent_member(cell["members"], rng=rng)
    selection_state[cell_id]["selectionVisitCount"] += 1
    selection_state[cell_id]["offspringAttemptCount"] += 1
    return (
        cell,
        member,
        mode,
        "quality",
        (
            "negative_novelty_slot_unavailable_quality_fallback"
            if negative_novelty_slot
            else "quality_eligible_parent"
        ),
    )


def _mutation_depth(rng: random.Random) -> int:
    draw = rng.random()
    if draw < 0.70:
        return 1
    if draw < 0.95:
        return 2
    return 3


def _plan_occurrence_key(plan: Mapping[str, Any]) -> str:
    occurrence = {
        "operatorId": plan.get("operatorId"),
        "targetTransitionId": plan.get("targetTransitionId"),
        "targetGuardPath": plan.get("targetGuardPath"),
        "sourceGuardSha256": plan.get("sourceGuardSha256"),
        "confirmationClauseIndex": plan.get("confirmationClauseIndex"),
        "setupAnchorPath": plan.get("setupAnchorPath"),
        "setupClauseIndex": plan.get("setupClauseIndex"),
        "setupOccurrenceSha256": plan.get("setupOccurrenceSha256"),
    }
    return canonical_sha256(occurrence)


def _parametric_occurrence_key(option: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            "family": option.get("family"),
            "operator": option.get("operator"),
            "path": option.get("path"),
        }
    )


def _validation_record(validation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidateAcceptable": validation.get("candidateAcceptable"),
        "status": validation.get("status"),
        "programSha256": validation.get("programSha256"),
        "profileSnapshotSha256": validation.get("profileSnapshotSha256"),
        "validationReportSha256": validation.get("validationReportSha256"),
        "issueCodes": sorted(
            str(item.get("code"))
            for item in validation.get("issues") or []
            if isinstance(item, Mapping) and item.get("code")
        ),
    }


def _root_candidate_identity(candidate: Mapping[str, Any]) -> str:
    supplied = candidate.get("candidateIdentitySha256")
    if supplied is not None:
        return _sha(supplied, name="parent candidate identity")
    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_imported_root_identity_v1",
            "canonicalGraphSha256": canonical_sha256(
                candidate["sourceProfile"]["graph"]
            ),
            "sourceProfileSha256": candidate["sourceProfileSha256"],
            "programSha256": candidate["programSha256"],
        }
    )


def _candidate_identity(
    *,
    origin_kind: str,
    parent: Mapping[str, Any] | None,
    profile: Mapping[str, Any],
    ordered_lineage: Sequence[Mapping[str, Any]],
    origin_contract: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str]:
    graph_sha = canonical_sha256(profile["graph"])
    material = {
        "schemaVersion": "temporal_qd_candidate_identity_v1",
        "qdEngineVersion": QD_VERSION,
        "originKind": origin_kind,
        "canonicalParentGraphSha256": (
            canonical_sha256(parent["sourceProfile"]["graph"])
            if parent is not None
            else graph_sha
        ),
        "parentCandidateIdentitySha256": (
            _root_candidate_identity(parent) if parent is not None else None
        ),
        "completeOrderedMutationLineage": _clone(
            list(ordered_lineage), name="candidate ordered mutation lineage"
        ),
        "originContract": _clone(origin_contract, name="candidate origin contract"),
        "finalCanonicalGraphSha256": graph_sha,
        "finalSourceProfileSha256": canonical_sha256(profile),
    }
    identity_sha = canonical_sha256(material)
    return material, identity_sha, "qd_" + identity_sha.removeprefix("sha256:")[:28]


def qd_predeclared_evidence_context(
    template: Mapping[str, Any],
    *,
    worker_contract_sha256: str | None = None,
    construction_catalog: Mapping[str, Any] | None = None,
    construction_catalog_path: Path | str | None = None,
) -> dict[str, Any]:
    """Return the candidate-independent, frozen part of QD evaluation evidence."""
    windows = list(template.get("developmentWindows") or [])
    candidates = list(template.get("candidates") or [])
    base_timeframes = {
        str(item.get("timeframe") or "").strip().upper()
        for item in candidates
        if isinstance(item, Mapping) and str(item.get("timeframe") or "").strip()
    }
    if len(base_timeframes) > 1:
        raise TemporalDiscoveryContractError(
            "QD predeclared evidence requires one base decision timeframe"
        )
    base_decision_timeframe = next(iter(base_timeframes), None)
    input_map: dict[str, Mapping[str, Any]] = {}
    if candidates:
        for item in candidates[0].get("windowInputs") or []:
            if isinstance(item, Mapping) and isinstance(item.get("evidencePlan"), Mapping):
                input_map[str(item.get("windowId"))] = item["evidencePlan"]
    ordered_windows = []
    for window in windows:
        if not isinstance(window, Mapping):
            raise TemporalDiscoveryContractError("QD development window must be an object")
        window_id = str(window.get("windowId") or "")
        plan = _clone(input_map.get(window_id) or {}, name="QD evidence-plan template")
        # These rotate for each source profile and therefore cannot identify the
        # predeclared semantic/cost contract by themselves.
        for key in (
            "plan_id",
            "planId",
            "profile_snapshot_sha256",
            "profileSnapshotSha256",
            "execution_cell_sha256",
            "executionCellSha256",
            "lake_manifest_sha256",
            "lakeManifestSha256",
        ):
            plan.pop(key, None)
        ordered_windows.append(
            {
                "windowId": window_id,
                "window": _clone(window, name="QD development window"),
                "evidencePlanSemantic": plan,
            }
        )
    if construction_catalog is not None and construction_catalog_path is None:
        raise TemporalDiscoveryContractError(
            "frozen construction catalog identity requires its source path"
        )
    construction_catalog_identity = (
        {
            "path": str(Path(construction_catalog_path).resolve()),
            "catalogSha256": canonical_sha256(construction_catalog),
        }
        if construction_catalog is not None
        else None
    )
    context = {
        "schemaVersion": "temporal_qd_predeclared_evidence_context_v3",
        "baseDecisionTimeframe": base_decision_timeframe,
        "orderedWindowPlanSemantic": ordered_windows,
        "workerContractSha256": worker_contract_sha256
        or (template.get("workerContract") or {}).get("workerContractSha256"),
        "constructionCatalog": construction_catalog_identity,
        "costViews": {
            "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
            "research_conservative": {
                "spreadBps": 2.0,
                "slippageBps": 1.0,
                "commissionBps": 0.5,
            },
        },
    }
    context["predeclaredEvidenceContextSha256"] = canonical_sha256(context)
    return context


def qd_canonical_evidence_identity(
    candidate: Mapping[str, Any], evidence_context: Mapping[str, Any]
) -> str:
    """Canonical identity of the actual evaluation material, not a proposal slot."""
    context = _clone(evidence_context, name="QD predeclared evidence context")
    supplied_context_sha = context.pop("predeclaredEvidenceContextSha256", None)
    if supplied_context_sha is not None and _sha(
        supplied_context_sha, name="QD predeclared evidence context identity"
    ) != canonical_sha256(context):
        raise TemporalDiscoveryContractError("QD predeclared evidence context diverged")
    program_sha = _sha(candidate.get("programSha256"), name="QD evidence program SHA-256")
    source_sha = _sha(
        candidate.get("sourceProfileSha256"), name="QD evidence source-profile SHA-256"
    )
    snapshot_sha = _sha(
        candidate.get("profileSnapshotSha256") or source_sha,
        name="QD evidence profile-snapshot SHA-256",
    )
    profile = candidate.get("sourceProfile") or {}
    if not isinstance(profile, Mapping):
        raise TemporalDiscoveryContractError("QD evidence source profile must be an object")
    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_canonical_evidence_identity_v3",
            # Program, rather than proposal/candidate ID, keeps a semantically
            # identical executable program from being evaluated twice.
            "programSha256": program_sha,
            "sourceProfileSha256": source_sha,
            "profileSnapshotSha256": snapshot_sha,
            "orderedWindowPlanSemantic": context.get("orderedWindowPlanSemantic"),
            "costViews": context.get("costViews"),
            "workerContractSha256": context.get("workerContractSha256"),
            "executionConfigSha256": canonical_sha256(
                profile.get("executionConfig") or {}
            ),
        }
    )


def _empty_identity_ledger() -> dict[str, Any]:
    ledger = {
        "schemaVersion": QD_IDENTITY_LEDGER_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "identityPolicy": _clone(QD_POLICY["identity"], name="QD identity policy"),
        "records": [],
        "uniqueCounts": {
            "candidateIdentity": 0,
            "program": 0,
            "sourceProfile": 0,
            "profileSnapshot": 0,
            "canonicalEvidence": 0,
        },
        "duplicateCounters": {
            "candidateIdentity": 0,
            "program": 0,
            "sourceProfile": 0,
            "profileSnapshot": 0,
            "canonicalEvidence": 0,
            "programDifferentEvidenceAllowed": 0,
        },
        "proposalSlotCounters": {
            "proposalsObserved": 0,
            "acceptedUniqueProposalSlots": 0,
            "duplicateRejections": 0,
        },
    }
    ledger["ledgerSha256"] = canonical_sha256(ledger)
    return ledger


def _ledger_identity(ledger: Mapping[str, Any]) -> str:
    material = _clone(ledger, name="QD identity ledger")
    supplied = _sha(material.pop("ledgerSha256", None), name="QD identity ledger SHA-256")
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError("QD identity ledger identity mismatch")
    return supplied


def _save_identity_ledger(path: Path, ledger: dict[str, Any]) -> None:
    ledger.pop("ledgerSha256", None)
    ledger["ledgerSha256"] = canonical_sha256(ledger)
    _replace(path, ledger)


def _load_identity_ledger(path: Path) -> dict[str, Any]:
    if not path.exists():
        ledger = _empty_identity_ledger()
        _write_once(path, ledger)
        return ledger
    ledger = _read(path, name="QD identity ledger")
    _ledger_identity(ledger)
    if (
        ledger.get("schemaVersion") != QD_IDENTITY_LEDGER_SCHEMA
        or ledger.get("qdVersion") != QD_VERSION
        or ledger.get("policyName") != QD_POLICY_NAME
        or ledger.get("policySha256") != QD_POLICY_SHA256
        or ledger.get("identityPolicy") != QD_POLICY["identity"]
    ):
        raise TemporalDiscoveryContractError("QD identity ledger is bound to another policy")
    return ledger


def _ledger_record(candidate: Mapping[str, Any]) -> dict[str, str]:
    return {
        "candidateIdentitySha256": _sha(
            candidate.get("candidateIdentitySha256"), name="QD ledger candidate identity"
        ),
        "programSha256": _sha(candidate.get("programSha256"), name="QD ledger program"),
        "sourceProfileSha256": _sha(
            candidate.get("sourceProfileSha256"), name="QD ledger source profile"
        ),
        "profileSnapshotSha256": _sha(
            candidate.get("profileSnapshotSha256"), name="QD ledger profile snapshot"
        ),
        "canonicalEvidenceIdentitySha256": _sha(
            candidate.get("canonicalEvidenceIdentitySha256"),
            name="QD ledger canonical evidence identity",
        ),
    }


def _ledger_refresh_counts(ledger: dict[str, Any]) -> None:
    records = ledger["records"]
    ledger["uniqueCounts"] = {
        name: len({str(record[key]) for record in records})
        for name, key in _LEDGER_IDENTITY_FIELDS.items()
    }


_LEDGER_IDENTITY_FIELDS = {
    "candidateIdentity": "candidateIdentitySha256",
    "program": "programSha256",
    "sourceProfile": "sourceProfileSha256",
    "profileSnapshot": "profileSnapshotSha256",
    "canonicalEvidence": "canonicalEvidenceIdentitySha256",
}


def _ledger_identity_index(ledger: Mapping[str, Any]) -> dict[str, set[str]]:
    """Build an ephemeral membership index for one generation invocation.

    The ledger remains the sole persisted authority.  This derived index only
    removes repeated O(records) scans while proposals are being made; it is
    deliberately never serialized and is rebuilt on every resume.
    """
    records = ledger.get("records") or []
    return {
        name: {str(record[key]) for record in records}
        for name, key in _LEDGER_IDENTITY_FIELDS.items()
    }


def _ledger_index_add(
    index: dict[str, set[str]], record: Mapping[str, str]
) -> None:
    for name, key in _LEDGER_IDENTITY_FIELDS.items():
        index[name].add(str(record[key]))


def _ledger_bootstrap_archive(
    ledger: dict[str, Any],
    archive: Mapping[str, Any],
    evidence_context: Mapping[str, Any],
) -> None:
    existing = {
        str(record["candidateIdentitySha256"])
        for record in ledger.get("records") or []
    }
    changed = False
    for cell in archive.get("cells") or []:
        for member in cell.get("members") or []:
            candidate = member.get("candidate")
            if not isinstance(candidate, Mapping):
                continue
            candidate = _clone(candidate, name="QD ledger archive candidate")
            if not candidate.get("candidateIdentitySha256"):
                # Imported v3 roots still need a stable identity before becoming
                # parents; this is intentionally not a v2 compatibility path.
                identity = _root_candidate_identity(candidate)
                candidate["candidateIdentitySha256"] = identity
            candidate.setdefault(
                "profileSnapshotSha256", candidate.get("sourceProfileSha256")
            )
            candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(
                candidate, evidence_context
            )
            record = _ledger_record(candidate)
            if record["candidateIdentitySha256"] not in existing:
                ledger["records"].append(record)
                existing.add(record["candidateIdentitySha256"])
                changed = True
    if changed:
        _ledger_refresh_counts(ledger)


def _ledger_duplicate_check(
    ledger: dict[str, Any],
    candidate: Mapping[str, Any],
    *,
    identity_index: Mapping[str, set[str]] | None = None,
) -> tuple[str | None, dict[str, bool]]:
    record = _ledger_record(candidate)
    if identity_index is None:
        records = ledger.get("records") or []
        checks = {
            name: any(existing.get(key) == record[key] for existing in records)
            for name, key in _LEDGER_IDENTITY_FIELDS.items()
        }
    else:
        checks = {
            name: str(record[key]) in identity_index[name]
            for name, key in _LEDGER_IDENTITY_FIELDS.items()
        }
    for name, duplicate in checks.items():
        if duplicate:
            ledger["duplicateCounters"][name] += 1
    if checks["program"] and not checks["canonicalEvidence"]:
        ledger["duplicateCounters"]["programDifferentEvidenceAllowed"] += 1
    if checks["candidateIdentity"]:
        ledger["proposalSlotCounters"]["duplicateRejections"] += 1
        return "duplicate_candidate_identity_global", checks
    if checks["canonicalEvidence"]:
        ledger["proposalSlotCounters"]["duplicateRejections"] += 1
        return "duplicate_canonical_evidence_global", checks
    return None, checks


def _ledger_accept(
    ledger: dict[str, Any],
    candidate: Mapping[str, Any],
    *,
    identity_index: dict[str, set[str]] | None = None,
) -> None:
    record = _ledger_record(candidate)
    ledger["records"].append(record)
    if identity_index is not None:
        _ledger_index_add(identity_index, record)
    ledger["proposalSlotCounters"]["acceptedUniqueProposalSlots"] += 1
    _ledger_refresh_counts(ledger)


def _ledger_public_counts(ledger: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "uniqueIdentityCounts": _clone(
            ledger.get("uniqueCounts") or {}, name="QD ledger unique counts"
        ),
        "duplicateCounters": _clone(
            ledger.get("duplicateCounters") or {}, name="QD ledger duplicate counts"
        ),
        "proposalSlotCounters": _clone(
            ledger.get("proposalSlotCounters") or {}, name="QD proposal slot counts"
        ),
        "identityLedgerSha256": _sha(
            ledger.get("ledgerSha256"), name="QD identity ledger SHA-256"
        ),
    }


def _structural_proposal(
    *,
    rng: random.Random,
    cells: Sequence[Mapping[str, Any]],
    negative_novelty_cells: Sequence[Mapping[str, Any]],
    negative_novelty_slot: bool,
    operators: Mapping[str, Any],
    max_depth: int,
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]],
    selection_state: dict[str, dict[str, int]],
    validator: Any | None,
    evidence_context: Mapping[str, Any] | None = None,
    frozen_construction_catalog: Mapping[str, Any] | None = None,
    replay_steps: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    cell, parent_member, selection_mode, parent_lane, parent_lane_reason = _select_parent(
        rng=rng,
        cells=cells,
        negative_novelty_cells=negative_novelty_cells,
        selection_state=selection_state,
        negative_novelty_slot=negative_novelty_slot,
    )
    parent = parent_member["candidate"]
    parent_id = str(parent["candidateId"])
    history = _clone(
        parent.get("structuralOperatorHistory") or [],
        name="parent structural operator history",
    )
    depth = len(history)
    desired_depth = _mutation_depth(rng)
    base = {
        "originKind": "structural_offspring",
        "parentCellId": cell["cellId"],
        "parentCandidateId": parent_id,
        "parentProgramSha256": parent["programSha256"],
        "parentStructuralDepth": depth,
        "parentSelectionMode": selection_mode,
        "parentLane": parent_lane,
        "parentLaneReason": parent_lane_reason,
        "desiredMutationDepth": desired_depth,
        "parent": parent,
    }
    if depth + desired_depth > max_depth:
        return None, {**base, "proposalIssue": "maximum_structural_depth_reached"}
    current_profile = _clone(parent["sourceProfile"], name="structural parent profile")
    current_program = _sha(parent["programSha256"], name="structural parent program")
    steps: list[dict[str, Any]] = []
    for step_index in range(desired_depth):
        eligible: list[tuple[str, str, list[dict[str, Any]]]] = []
        profile_sha = canonical_sha256(current_profile)
        for operator_id, operator in sorted(operators.items()):
            key = (profile_sha, operator_id)
            if key not in plan_cache:
                plan_cache[key] = operator.enumerate_plans(current_profile)
            plans = plan_cache[key]
            if operator_id == GRAPH_BOUND_TIMEFRAME:
                # Do this before the family/occurrence draw.  Out-of-scope
                # substitutions are not viable candidates; they never reach a
                # validator, evidence-plan rotation, or worker task.
                plans = [
                    plan
                    for plan in plans
                    if _predeclared_lake_scope_report(
                        operator.preview(current_profile, plan),
                        evidence_context,
                        frozen_construction_catalog=frozen_construction_catalog,
                    )["acceptable"]
                ]
            if plans:
                eligible.append((operator_id, "structural", plans))
        for source_family, options in sorted(
            _available_mutations(current_profile).items()
        ):
            if options:
                eligible.append(
                    (PARAMETRIC_FAMILY_IDS[source_family], "parametric", options)
                )
        if not eligible:
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "no_eligible_operator_family",
            }
        operator_id, operator_kind, plans = eligible[rng.randrange(len(eligible))]
        occurrence_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for raw_plan in plans:
            occurrence_key = (
                _plan_occurrence_key(raw_plan)
                if operator_kind == "structural"
                else _parametric_occurrence_key(raw_plan)
            )
            occurrence_groups[occurrence_key].append(raw_plan)
        occurrence_ids = sorted(occurrence_groups)
        occurrence_id = occurrence_ids[rng.randrange(len(occurrence_ids))]
        occurrence_plans = sorted(
            occurrence_groups[occurrence_id],
            key=lambda item: canonical_sha256(
                {key: value for key, value in item.items() if key != "_path"}
            ),
        )
        selected_plan = occurrence_plans[rng.randrange(len(occurrence_plans))]
        if operator_kind == "structural":
            operator = operators[operator_id]
            operator_version = operator.operator_version
            operator_spec_sha = operator.specification["operatorSpecSha256"]
            plan = _clone(selected_plan, name="QD structural plan")
            child = operator.preview(current_profile, plan)
        else:
            try:
                old_value = _get(current_profile, selected_plan["_path"])
            except (KeyError, IndexError):
                old_value = {"__absent__": True}
            public_mutation = _public_mutation(selected_plan, old_value)
            plan = {
                "schemaVersion": "temporal_parametric_operator_plan_v1",
                "operatorId": operator_id,
                "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
                "operatorSpecSha256": PARAMETRIC_OPERATOR_SPECS[operator_id],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "mutation": public_mutation,
            }
            plan["planSha256"] = canonical_sha256(plan)
            operator_version = PARAMETRIC_OPERATOR_VERSION
            operator_spec_sha = PARAMETRIC_OPERATOR_SPECS[operator_id]
            child = _apply_option(current_profile, selected_plan)
        reachability = _static_reachability(child, operator_id=operator_id)
        step: dict[str, Any] = {
            "stepIndex": step_index,
            "operatorId": operator_id,
            "operatorKind": operator_kind,
            "operatorVersion": operator_version,
            "operatorSpecSha256": operator_spec_sha,
            "eligibleOperatorFamilyCount": len(eligible),
            "eligibleOperatorFamilyIds": [item[0] for item in eligible],
            "familyOccurrenceCount": len(occurrence_ids),
            "selectedOccurrenceSha256": occurrence_id,
            "occurrenceParameterPlanCount": len(occurrence_plans),
            "plan": _clone(plan, name="QD mutation plan"),
            "planSha256": plan["planSha256"],
            "parentSourceProfileSha256": canonical_sha256(current_profile),
            "childSourceProfileSha256": canonical_sha256(child),
            "managementReachabilitySha256": reachability["reachabilitySha256"],
            "managementReachabilityIssueCounts": reachability["issueCounts"],
        }
        if reachability["acceptable"] is not True:
            step["disposition"] = "static_reachability_rejected"
            steps.append(step)
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "intermediate_static_reachability_rejected",
            }
        if replay_steps is not None:
            if step_index >= len(replay_steps):
                raise TemporalDiscoveryContractError(
                    "QD replay lacks an intermediate step"
                )
            validation_record = replay_steps[step_index].get("validation")
            if not isinstance(validation_record, Mapping):
                raise TemporalDiscoveryContractError(
                    "QD replay intermediate validation is missing"
                )
            validation_record = _clone(
                validation_record, name="replayed intermediate validation"
            )
        else:
            if validator is None:
                raise TemporalDiscoveryContractError(
                    "QD structural validator is missing"
                )
            validation = validator.validate(
                candidate_id=(
                    "qd_intermediate_"
                    + step["childSourceProfileSha256"].removeprefix("sha256:")[:24]
                ),
                source_profile=child,
                expected_raw_source_profile_sha256=step["childSourceProfileSha256"],
            )
            validation_record = _validation_record(validation)
        step["validation"] = validation_record
        if validation_record.get("candidateAcceptable") is not True:
            step["disposition"] = "native_validator_rejected"
            steps.append(step)
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "intermediate_native_validator_rejected",
            }
        child_program = _sha(
            validation_record.get("programSha256"), name="intermediate program SHA-256"
        )
        if operator_kind == "structural":
            rebound, application = operator.apply(
                current_profile,
                plan,
                parent_validated_program_sha256=current_program,
                child_validated_program_sha256=child_program,
            )
            audit = operator.audit(current_profile, child, application)
            if (
                rebound != child
                or application["staticInvariantReport"]["allChecksPassed"] is not True
                or audit["allChecksPassed"] is not True
            ):
                raise TemporalDiscoveryContractError(
                    "QD intermediate structural audit failed"
                )
            # Construction applications are transactions, not abstract family
            # labels.  Persist their exact trace and evidence invalidation so a
            # resumed proposal can replay and downstream evaluation can rotate
            # profile-bound lake evidence when a graph-bound timeframe changes.
            if operator_id in CONSTRUCTION_OPERATOR_IDS:
                step["mutationTrace"] = _clone(
                    application["mutationTrace"], name="construction mutation trace"
                )
                step["evidenceScope"] = _clone(
                    application["evidenceScope"], name="construction evidence scope"
                )
                step["staticInvariantReportSha256"] = application[
                    "staticInvariantReport"
                ]["auditSha256"]
        else:
            rebound = _apply_option(current_profile, selected_plan)
            application = {
                "schemaVersion": "temporal_parametric_operator_application_v1",
                "operatorId": operator_id,
                "operatorVersion": operator_version,
                "operatorSpecSha256": operator_spec_sha,
                "plan": plan,
                "planSha256": plan["planSha256"],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "childSourceProfileSha256": canonical_sha256(child),
                "parentProgramSha256": current_program,
                "childProgramSha256": child_program,
                **(
                    {
                        "mutationTrace": _clone(
                            application["mutationTrace"],
                            name="construction history mutation trace",
                        ),
                        "evidenceScope": _clone(
                            application["evidenceScope"],
                            name="construction history evidence scope",
                        ),
                        "staticInvariantReportSha256": application[
                            "staticInvariantReport"
                        ]["auditSha256"],
                    }
                    if operator_id in CONSTRUCTION_OPERATOR_IDS
                    else {}
                ),
            }
            application["applicationSha256"] = canonical_sha256(application)
            audit = {
                "schemaVersion": "temporal_parametric_operator_audit_v1",
                "operatorId": operator_id,
                "planSha256": plan["planSha256"],
                "checks": {
                    "single_exact_option_applied": rebound == child,
                    "source_profile_changed": current_profile != child,
                },
                "allChecksPassed": rebound == child and current_profile != child,
            }
            audit["auditSha256"] = canonical_sha256(audit)
            if audit["allChecksPassed"] is not True:
                raise TemporalDiscoveryContractError(
                    "QD parametric mutation audit failed"
                )
        step["applicationSha256"] = application["applicationSha256"]
        step["auditSha256"] = audit["auditSha256"]
        step["disposition"] = "applied"
        steps.append(step)
        history.append(
            {
                "operatorId": operator_id,
                "operatorKind": operator_kind,
                "operatorVersion": operator_version,
                "operatorSpecSha256": operator_spec_sha,
                "plan": _clone(plan, name="mutation history plan"),
                "planSha256": plan["planSha256"],
                "applicationSha256": application["applicationSha256"],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "childSourceProfileSha256": canonical_sha256(child),
                "parentProgramSha256": current_program,
                "childProgramSha256": child_program,
            }
        )
        current_profile = child
        current_program = child_program
    if replay_steps is not None and len(replay_steps) != len(steps):
        raise TemporalDiscoveryContractError("QD replay has extra intermediate steps")
    identity_material, identity_sha, candidate_id = _candidate_identity(
        origin_kind="structural_offspring",
        parent=parent,
        profile=current_profile,
        ordered_lineage=history,
        origin_contract={
            "operatorRegistry": [
                {
                    "operatorId": step["operatorId"],
                    "operatorVersion": step["operatorVersion"],
                    "operatorSpecSha256": step["operatorSpecSha256"],
                }
                for step in steps
            ]
        },
    )
    return current_profile, {
        **base,
        "steps": steps,
        "completeStructuralOperatorHistory": history,
        "evidenceScope": _construction_evidence_scope(steps),
        "finalValidation": steps[-1]["validation"],
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "candidateId": candidate_id,
    }


def _immigrant_proposal(
    *,
    immigrant_source: ExactGeneratorV2Continuation,
) -> tuple[dict[str, Any], dict[str, Any]]:
    proposal = immigrant_source.next_proposal()
    profile = proposal["rawSourceProfile"]
    lineage = [
        {
            "kind": "generator_v2_mutation",
            "payload": item,
        }
        for item in proposal["mutations"]
    ] + [
        {
            "kind": "generator_v2_activation_aware_repair",
            "payload": item,
        }
        for item in proposal["activationAwareRepairs"]
    ]
    identity_material, identity_sha, candidate_id = _candidate_identity(
        origin_kind="random_immigrant",
        parent=None,
        profile=profile,
        ordered_lineage=lineage,
        origin_contract={
            "generatorSourceIdentitySha256": proposal["sourceIdentitySha256"],
            "immigrantProposalSchema": proposal["schemaVersion"],
        },
    )
    return profile, {
        "originKind": "random_immigrant",
        "immigrantProposal": proposal,
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "candidateId": candidate_id,
    }


def _proposal(
    *,
    config_sha: str,
    generation_index: int,
    proposal_ordinal: int,
    cells: Sequence[Mapping[str, Any]],
    negative_novelty_cells: Sequence[Mapping[str, Any]],
    immigrant_source: ExactGeneratorV2Continuation,
    operators: Mapping[str, Any],
    parameters: Mapping[str, Any],
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]],
    selection_state: dict[str, dict[str, int]],
    validator: Any | None,
    evidence_context: Mapping[str, Any] | None = None,
    frozen_construction_catalog: Mapping[str, Any] | None = None,
    replay_steps: Sequence[Mapping[str, Any]] | None = None,
    random_immigrant_only: bool = False,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    seed_sha = _proposal_seed(config_sha, generation_index, proposal_ordinal)
    rng = _proposal_rng(seed_sha)
    origin = _origin_kind(
        proposal_ordinal, random_immigrant_only=random_immigrant_only
    )
    if origin == "random_immigrant":
        profile, metadata = _immigrant_proposal(
            immigrant_source=immigrant_source,
        )
    else:
        profile, metadata = _structural_proposal(
            rng=rng,
            cells=cells,
            negative_novelty_cells=negative_novelty_cells,
            negative_novelty_slot=_negative_novelty_slot(
                proposal_ordinal, random_immigrant_only=random_immigrant_only
            ),
            operators=operators,
            max_depth=int(parameters["maxCumulativeStructuralDepth"]),
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=validator,
            evidence_context=evidence_context,
            frozen_construction_catalog=frozen_construction_catalog,
            replay_steps=replay_steps,
        )
    public_metadata = {
        key: _clone(value, name=f"proposal metadata {key}")
        for key, value in metadata.items()
        if key not in {"parent"}
    }
    material = {
        "proposalSeedSha256": seed_sha,
        "generationIndex": generation_index,
        "proposalOrdinal": proposal_ordinal,
        "originKind": origin,
        **public_metadata,
        "rawSourceProfileSha256": (
            canonical_sha256(profile) if profile is not None else None
        ),
    }
    material["proposalMaterialSha256"] = canonical_sha256(material)
    metadata["proposalMaterial"] = material
    return profile, metadata


def _entry_path(root: Path, ordinal: int) -> Path:
    return root / "proposal-journal" / f"{ordinal:08d}.json"


def _load_entries(root: Path) -> list[dict[str, Any]]:
    paths = sorted((root / "proposal-journal").glob("*.json"))
    entries = []
    for ordinal, path in enumerate(paths):
        if path.name != f"{ordinal:08d}.json":
            raise TemporalDiscoveryContractError("QD proposal journal has a gap")
        entry = _read(path, name="QD proposal entry")
        supplied = _identity_payload(entry, "entrySha256", name="QD proposal entry")
        entry["entrySha256"] = supplied
        if (
            entry.get("schemaVersion") != QD_ENTRY_SCHEMA
            or int(entry.get("proposalOrdinal", -1)) != ordinal
        ):
            raise TemporalDiscoveryContractError("QD proposal entry routing mismatch")
        entries.append(entry)
    return entries


def _accepted_state(
    entries: Sequence[Mapping[str, Any]], archive: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], Counter[str], set[str], set[str]]:
    accepted = []
    counts: Counter[str] = Counter()
    del archive
    programs: set[str] = set()
    identities: set[str] = set()
    for entry in entries:
        if entry.get("disposition") != "accepted":
            continue
        candidate = entry.get("candidate")
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("accepted QD entry lacks candidate")
        candidate = _clone(candidate, name="accepted QD candidate")
        candidate_id = str(candidate["candidateId"])
        if candidate_id in identities:
            raise TemporalDiscoveryContractError(
                "QD accepted candidate identity is duplicated"
            )
        if (
            canonical_sha256(candidate["sourceProfile"])
            != candidate["sourceProfileSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD accepted profile identity mismatch"
            )
        programs.add(candidate["programSha256"])
        _sha(
            candidate.get("canonicalEvidenceIdentitySha256"),
            name="QD accepted canonical evidence identity",
        )
        identity_material = candidate.get("candidateIdentityMaterial")
        if not isinstance(identity_material, Mapping) or canonical_sha256(
            identity_material
        ) != candidate.get("candidateIdentitySha256"):
            raise TemporalDiscoveryContractError(
                "QD candidate identity material diverged"
            )
        if (
            candidate_id
            != "qd_"
            + str(candidate["candidateIdentitySha256"]).removeprefix("sha256:")[:28]
        ):
            raise TemporalDiscoveryContractError("QD candidate ID is not content-bound")
        if (
            "authoredValidationBinding" in candidate
            or "authoredValidationBindingSha256" in candidate
        ):
            validate_authored_validation_binding(candidate)
        identities.add(candidate_id)
        counts[str(entry["originKind"])] += 1
        accepted.append(candidate)
    return accepted, counts, programs, identities


def _replay_entries(
    *,
    entries: Sequence[Mapping[str, Any]],
    config_sha: str,
    generation_index: int,
    cells: Sequence[Mapping[str, Any]],
    negative_novelty_cells: Sequence[Mapping[str, Any]],
    immigrant_source: ExactGeneratorV2Continuation,
    operators: Mapping[str, Any],
    parameters: Mapping[str, Any],
    selection_state: dict[str, dict[str, int]],
    evidence_context: Mapping[str, Any] | None = None,
    frozen_construction_catalog: Mapping[str, Any] | None = None,
    random_immigrant_only: bool = False,
) -> None:
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for ordinal, entry in enumerate(entries):
        proposal_value = entry.get("proposal")
        if not isinstance(proposal_value, Mapping):
            raise TemporalDiscoveryContractError(
                "QD replay proposal material is missing"
            )
        replay_steps = (
            proposal_value.get("steps")
            if entry.get("originKind") == "structural_offspring"
            else None
        )
        profile, metadata = _proposal(
            config_sha=config_sha,
            generation_index=generation_index,
            proposal_ordinal=ordinal,
            cells=cells,
            negative_novelty_cells=negative_novelty_cells,
            immigrant_source=immigrant_source,
            operators=operators,
            parameters=parameters,
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=None,
            evidence_context=evidence_context,
            frozen_construction_catalog=frozen_construction_catalog,
            replay_steps=replay_steps,
            random_immigrant_only=random_immigrant_only,
        )
        if metadata["proposalMaterial"] != entry.get("proposal"):
            raise TemporalDiscoveryContractError(
                f"QD exact proposal replay diverged at ordinal {ordinal}"
            )
        if str(entry.get("originKind")) != _origin_kind(
            ordinal, random_immigrant_only=random_immigrant_only
        ):
            raise TemporalDiscoveryContractError("QD replay origin schedule diverged")
        if entry.get("disposition") == "accepted":
            candidate = entry.get("candidate")
            if not isinstance(candidate, Mapping) or profile is None:
                raise TemporalDiscoveryContractError("QD accepted replay is incomplete")
            if canonical_sha256(profile) != candidate.get("sourceProfileSha256"):
                raise TemporalDiscoveryContractError(
                    "QD accepted replay profile diverged"
                )


def _accepted_candidate(
    *,
    profile: Mapping[str, Any],
    validation: Mapping[str, Any],
    metadata: Mapping[str, Any],
    generation_index: int,
    birth_ordinal: int,
    proposal_ordinal: int,
    evidence_context: Mapping[str, Any],
    authored_validator_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    source_sha = canonical_sha256(profile)
    program_sha = _sha(validation.get("programSha256"), name="QD program SHA-256")
    candidate_id = str(metadata["candidateId"])
    identity_sha = _sha(
        metadata.get("candidateIdentitySha256"), name="QD candidate identity SHA-256"
    )
    identity_material = _clone(
        metadata["candidateIdentityMaterial"], name="QD candidate identity material"
    )
    if canonical_sha256(identity_material) != identity_sha or candidate_id != (
        "qd_" + identity_sha.removeprefix("sha256:")[:28]
    ):
        raise TemporalDiscoveryContractError("QD accepted candidate identity diverged")
    if metadata["originKind"] == "structural_offspring":
        structural_history = _clone(
            metadata["completeStructuralOperatorHistory"],
            name="complete structural history",
        )
        mutation_trace = []
        repairs = []
        seed_id = str(metadata["parent"].get("seedId") or "qd_parent")
        parent_ids = [metadata["parentCandidateId"]]
        parent_programs = [metadata["parentProgramSha256"]]
        evidence_scope = _clone(
            metadata.get("evidenceScope") or _construction_evidence_scope([]),
            name="structural construction evidence scope",
        )
    else:
        immigrant = metadata["immigrantProposal"]
        structural_history = []
        mutation_trace = _clone(immigrant["mutations"], name="immigrant mutations")
        repairs = _clone(immigrant["activationAwareRepairs"], name="immigrant repairs")
        seed_id = str(immigrant["seedId"])
        parent_ids = []
        parent_programs = []
        evidence_scope = _construction_evidence_scope([])
    lineage = {
        "schemaVersion": "temporal_qd_candidate_lineage_v3",
        "candidateId": candidate_id,
        "candidateIdentitySha256": identity_sha,
        "candidateSourceProfileSha256": source_sha,
        "candidateValidatedProgramSha256": program_sha,
        "originKind": metadata["originKind"],
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "parentCandidateIds": parent_ids,
        "parentProgramSha256s": parent_programs,
    }
    lineage["lineageSha256"] = canonical_sha256(lineage)
    candidate = {
        "candidateId": candidate_id,
        "sourceMode": "qd_" + metadata["originKind"],
        "seedId": seed_id,
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "sourceProfile": _clone(profile, name="QD accepted profile"),
        "sourceProfileSha256": source_sha,
        "profileSnapshotSha256": _sha(
            validation.get("profileSnapshotSha256"), name="QD snapshot SHA-256"
        ),
        "programSha256": program_sha,
        "validationReportSha256": _sha(
            validation.get("validationReportSha256"), name="QD validation SHA-256"
        ),
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "structuralDepth": len(structural_history),
        "structuralOperatorHistory": structural_history,
        "mutationTrace": mutation_trace,
        "activationAwareRepairs": repairs,
        "constructionEvidenceScope": evidence_scope,
        "lineage": lineage,
    }
    candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(
        candidate, evidence_context
    )
    authored_binding = build_authored_validation_binding(
        raw_source_profile_sha256=source_sha,
        validation=validation,
        provenance=authored_validator_provenance,
    )
    candidate["authoredValidationBindingSha256"] = authored_binding.pop(
        "authoredValidationBindingSha256"
    )
    candidate["authoredValidationBinding"] = authored_binding
    return candidate


def _manifest(root: Path, *, population_sha: str) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    manifest = {
        "schemaVersion": QD_MANIFEST_SCHEMA,
        "populationSha256": population_sha,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_once(root / "manifest.json", manifest)
    return manifest


def _proposal_accounting(
    entries: Sequence[Mapping[str, Any]],
    *,
    random_immigrant_only: bool = False,
    allow_empty_quality_bootstrap: bool = False,
    quality_parent_cell_count_at_generation_start: int | None = None,
) -> dict[str, Any]:
    origin_proposals: Counter[str] = Counter()
    origin_accepted: Counter[str] = Counter()
    selection_modes: Counter[str] = Counter()
    parent_lanes: Counter[str] = Counter()
    parent_lane_reasons: Counter[str] = Counter()
    selected_cells: Counter[str] = Counter()
    offspring_attempts: Counter[str] = Counter()
    family_attempts: Counter[str] = Counter()
    family_applications: Counter[str] = Counter()
    construction_family_attempts: Counter[str] = Counter()
    construction_family_applications: Counter[str] = Counter()
    mutation_depths: Counter[str] = Counter()
    dispositions: Counter[str] = Counter()
    for entry in entries:
        origin = str(entry["originKind"])
        origin_proposals[origin] += 1
        disposition = str(entry["disposition"])
        dispositions[disposition] += 1
        if disposition == "accepted":
            origin_accepted[origin] += 1
        if origin != "structural_offspring":
            continue
        proposal = entry.get("proposal") or {}
        selection_modes[str(proposal.get("parentSelectionMode") or "unknown")] += 1
        parent_lanes[str(proposal.get("parentLane") or "unknown")] += 1
        parent_lane_reasons[str(proposal.get("parentLaneReason") or "unknown")] += 1
        cell_id = str(proposal.get("parentCellId") or "unknown")
        selected_cells[cell_id] += 1
        offspring_attempts[cell_id] += 1
        mutation_depths[str(proposal.get("desiredMutationDepth") or 0)] += 1
        for step in proposal.get("steps") or []:
            operator_id = str(step.get("operatorId") or "unknown")
            family_attempts[operator_id] += 1
            if operator_id in CONSTRUCTION_OPERATOR_IDS:
                construction_family_attempts[operator_id] += 1
            if step.get("disposition") == "applied":
                family_applications[operator_id] += 1
                if operator_id in CONSTRUCTION_OPERATOR_IDS:
                    construction_family_applications[operator_id] += 1
    if random_immigrant_only and origin_proposals["structural_offspring"]:
        raise TemporalDiscoveryContractError(
            "QD empty-quality bootstrap emitted a structural offspring proposal"
        )
    structural_parent_selections = sum(parent_lanes.values())
    negative_novelty_selections = parent_lanes["negative_novelty"]
    if negative_novelty_selections > structural_parent_selections // 10:
        raise TemporalDiscoveryContractError(
            "QD negative-novelty parent lane exceeded its ten-percent bound"
        )
    return {
        "originScheduling": {
            "policy": "empty_quality_bootstrap_generator_v2_random_immigrants_only",
            "activeMode": (
                "generator_v2_random_immigrants_only"
                if random_immigrant_only
                else "four_archive_offspring_then_one_generator_v2_immigrant"
            ),
            "allowEmptyQualityBootstrap": allow_empty_quality_bootstrap,
            "emptyQualityBootstrapActive": random_immigrant_only,
            **(
                {
                    "qualityParentCellCountAtGenerationStart": (
                        quality_parent_cell_count_at_generation_start
                    )
                }
                if quality_parent_cell_count_at_generation_start is not None
                else {}
            ),
        },
        "originProposalCounts": dict(sorted(origin_proposals.items())),
        "originAcceptedCounts": dict(sorted(origin_accepted.items())),
        "parentSelectionModeCounts": dict(sorted(selection_modes.items())),
        "parentLaneCounts": dict(sorted(parent_lanes.items())),
        "parentLaneReasonCounts": dict(sorted(parent_lane_reasons.items())),
        "negativeNoveltyParentSelectionCount": negative_novelty_selections,
        "structuralParentSelectionCount": structural_parent_selections,
        "negativeNoveltyParentSelectionFraction": (
            negative_novelty_selections / structural_parent_selections
            if structural_parent_selections
            else 0.0
        ),
        "parentCellSelectionCounts": dict(sorted(selected_cells.items())),
        "parentCellOffspringAttemptCounts": dict(sorted(offspring_attempts.items())),
        "operatorFamilyAttemptCounts": dict(sorted(family_attempts.items())),
        "operatorFamilyApplicationCounts": dict(sorted(family_applications.items())),
        "constructionOperatorFamilyAttemptCounts": dict(
            sorted(construction_family_attempts.items())
        ),
        "constructionOperatorFamilyApplicationCounts": dict(
            sorted(construction_family_applications.items())
        ),
        "mutationDepthAttemptCounts": dict(sorted(mutation_depths.items())),
        "dispositionCounts": dict(sorted(dispositions.items())),
    }


def generate_qd_generation(
    *,
    parent_archive_path: Path | str,
    source_preparation_path: Path | str | None = None,
    base_generator_root: Path | str | None = None,
    confirmed_entry_admission_root: Path | str | None = None,
    validator_command: Sequence[str] | None = None,
    output_root: Path | str,
    generation_index: int,
    immigrant_continuation_start: int = 0,
    allow_empty_quality_bootstrap: bool = False,
    parameters: Mapping[str, Any] | None = None,
    evidence_identity_context: Mapping[str, Any] | None = None,
    identity_ledger_path: Path | str | None = None,
    validator_timeout_seconds: float = 60.0,
    max_new_proposals: int | None = None,
    construction_catalog_path: Path | str | None = None,
    generation_funnel_enabled: bool = False,
    bidirectional_pair_policy: Mapping[str, Any] | None = None,
    bidirectional_pair_factory: Any | None = None,
    bidirectional_module_authority: Any | None = None,
    bidirectional_native_validator: Any | None = None,
    bidirectional_pair_compiler: Any | None = None,
    bidirectional_operator_implementation_identity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if generation_index < 1:
        raise TemporalDiscoveryContractError("evolved QD generations begin at index 1")
    if immigrant_continuation_start < 0:
        raise TemporalDiscoveryContractError("immigrant continuation start is negative")
    root = Path(output_root)
    archive, archive_sha = _load_archive(Path(parent_archive_path))
    archive_pair_policy = _bidirectional_pair_policy(archive)
    supplied_pair_policy = (
        _bidirectional_pair_policy({"bidirectionalPairPolicy": bidirectional_pair_policy})
        if bidirectional_pair_policy is not None
        else None
    )
    if archive_pair_policy is not None and supplied_pair_policy is not None and archive_pair_policy != supplied_pair_policy:
        raise TemporalDiscoveryContractError("bidirectional QD generation policy differs from parent archive")
    pair_policy = archive_pair_policy or supplied_pair_policy
    if pair_policy is not None:
        if any(value is None for value in (bidirectional_pair_factory, bidirectional_module_authority, bidirectional_native_validator, bidirectional_pair_compiler, bidirectional_operator_implementation_identity)):
            raise TemporalDiscoveryContractError("bidirectional QD generation requires explicit frozen pair factory and native authorities")
        from .temporal_qd_pair_generation import generate_pair_population

        parents = []
        for cell in archive.get("cells") or []:
            for member in cell.get("members") or []:
                candidate = member.get("candidate") if isinstance(member, Mapping) else None
                if isinstance(candidate, Mapping):
                    parents.append(FrozenPair.from_payload(candidate["bidirectionalGenome"]))
        pair_ledger_file = (
            Path(identity_ledger_path)
            if identity_ledger_path is not None
            else root / "identity-ledger.json"
        )
        pair_evidence_context = _clone(
            evidence_identity_context or qd_predeclared_evidence_context({}),
            name="pair QD evidence identity context",
        )
        return generate_pair_population(
            output_root=root,
            generation_index=generation_index,
            target_unique_candidates=int(_normalize_parameters(parameters)["targetUniqueCandidates"]),
            run_config={"parentArchiveSha256": archive_sha, "parameters": _normalize_parameters(parameters), "evidenceIdentityContext": pair_evidence_context},
            pair_policy={key: value for key, value in pair_policy.items() if key != "policySha256"},
            parent_pairs=parents,
            pair_factory=bidirectional_pair_factory,
            module_authority=bidirectional_module_authority,
            native_validator=bidirectional_native_validator,
            pair_compiler=bidirectional_pair_compiler,
            evidence_identity_context=pair_evidence_context,
            operator_implementation_identity=bidirectional_operator_implementation_identity,
            parent_archive=archive,
            # Pair mode is a first-class QD generation mode.  It must consume
            # the same campaign-wide identity ledger and frozen proposal
            # budget as the legacy proposal loop, rather than silently owning
            # an unbounded side journal.
            identity_ledger_path=pair_ledger_file,
            max_proposal_attempts=int(
                _normalize_parameters(parameters)["maxProposalAttempts"]
            ),
            max_new_proposals=max_new_proposals,
        )
    if any(
        value is None
        for value in (
            source_preparation_path,
            base_generator_root,
            confirmed_entry_admission_root,
            validator_command,
        )
    ):
        raise TemporalDiscoveryContractError(
            "legacy QD generation requires v2 source paths and validator command"
        )
    cells = _reproduction_cells(
        archive, allow_empty_quality_bootstrap=allow_empty_quality_bootstrap
    )
    random_immigrant_only = bool(allow_empty_quality_bootstrap and not cells)
    negative_novelty_cells = _negative_novelty_cells(archive)
    config_parameters = _normalize_parameters(parameters)
    context = _clone(
        evidence_identity_context
        or {
            "schemaVersion": "temporal_qd_predeclared_evidence_context_v3",
            "orderedWindowPlanSemantic": [],
            "workerContractSha256": None,
            "costViews": qd_predeclared_evidence_context({})["costViews"],
        },
        name="QD evidence identity context",
    )
    context.setdefault("predeclaredEvidenceContextSha256", canonical_sha256(context))
    context_sha = _sha(
        context["predeclaredEvidenceContextSha256"],
        name="QD predeclared evidence context identity",
    )
    if canonical_sha256(
        {key: value for key, value in context.items() if key != "predeclaredEvidenceContextSha256"}
    ) != context_sha:
        raise TemporalDiscoveryContractError("QD evidence identity context mismatch")
    ledger_file = Path(identity_ledger_path) if identity_ledger_path is not None else root / "identity-ledger.json"
    ledger = _load_identity_ledger(ledger_file)
    _ledger_bootstrap_archive(ledger, archive, context)
    _save_identity_ledger(ledger_file, ledger)
    construction_policy, construction_registry = qd_construction_operator_policy(
        construction_catalog_path
    )
    frozen_construction_catalog = (
        construction_registry.catalog.payload
        if construction_registry is not None
        else None
    )
    if frozen_construction_catalog is not None:
        expected_catalog_identity = construction_policy["catalog"]
        if context.get("constructionCatalog") != expected_catalog_identity:
            raise TemporalDiscoveryContractError(
                "QD predeclared evidence does not bind the frozen construction catalog"
            )
    operators = _operators(
        construction_registry,
        construction_operator_ids=construction_policy["enabledOperatorIds"],
    )
    operator_specs = [
        {
            "operatorId": operator_id,
            "operatorVersion": operator.operator_version,
            "operatorSpecSha256": operator.specification["operatorSpecSha256"],
        }
        for operator_id, operator in sorted(operators.items())
    ] + [
        {
            "operatorId": operator_id,
            "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
            "operatorSpecSha256": spec_sha,
        }
        for operator_id, spec_sha in sorted(PARAMETRIC_OPERATOR_SPECS.items())
    ]
    immigrant_source = ExactGeneratorV2Continuation(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        confirmed_entry_admission_root=confirmed_entry_admission_root,
        start_continuation_ordinal=immigrant_continuation_start,
    )
    config = {
        "schemaVersion": QD_CONFIG_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="frozen QD policy"),
        "generationIndex": generation_index,
        "parentArchiveSha256": archive_sha,
        "immigrantSourceIdentity": immigrant_source.source_identity,
        "immigrantContinuationStart": immigrant_continuation_start,
        "operatorRegistry": operator_specs,
        **(
            {"constructionOperatorPolicy": construction_policy}
            if construction_policy["enabled"]
            else {}
        ),
        "selectionPolicy": {
            "originSchedule": (
                "generator_v2_random_immigrants_only"
                if random_immigrant_only
                else "four_archive_offspring_then_one_generator_v2_immigrant"
            ),
            "originScheduling": {
                "emptyQualityBootstrapPolicy": (
                    "allow_only_when_explicitly_enabled_at_generation_start"
                ),
                "allowEmptyQualityBootstrap": bool(allow_empty_quality_bootstrap),
                "emptyQualityBootstrapActive": random_immigrant_only,
                "qualityParentCellCountAtGenerationStart": len(cells),
                "bootstrapOriginSchedule": "generator_v2_random_immigrants_only",
                "normalOriginSchedule": (
                    "four_archive_offspring_then_one_generator_v2_immigrant"
                ),
            },
            "parentCellMixture": {
                "uniformOccupiedCell": 0.50,
                "lowVisitCell": 0.30,
                "sparseDescriptorBoundary": 0.20,
            },
            "parentWithinCell": "seeded_rank_weighted_front_crowding_robust_return_capped_support_complexity",
            "negativeNoveltyLane": {
                "maxMembersPerCell": 1,
                "maxParentSelectionFraction": 0.10,
                "schedule": "every_tenth_structural_parent_selection",
            },
            "operator": "uniform_eligible_family_then_uniform_occurrence_then_uniform_parameter_plan",
            "mutationDepth": {"one": 0.70, "two": 0.25, "three": 0.05},
        },
        "parameters": config_parameters,
        "predeclaredEvidenceContext": context,
        "predeclaredEvidenceContextSha256": context_sha,
        "globalIdentityLedger": {
            "schemaVersion": QD_IDENTITY_LEDGER_SCHEMA,
            "policySha256": QD_POLICY_SHA256,
            "locationPolicy": "caller_supplied_generation_global_ledger",
        },
        "marketEvidenceReadDuringGeneration": False,
        "gatewayContactedDuringGeneration": False,
        **(
            {
                "generationFunnel": {
                    "enabled": True,
                    "stageSchemaVersion": "temporal_qd_proposal_funnel_stage_v1",
                }
            }
            if generation_funnel_enabled
            else {}
        ),
    }
    config["configSha256"] = canonical_sha256(config)
    _write_once(root / "config.json", config)

    target = int(config_parameters["targetUniqueCandidates"])
    selection_state = _initial_selection_state([*cells, *negative_novelty_cells])
    entries = _load_entries(root)
    _replay_entries(
        entries=entries,
        config_sha=config["configSha256"],
        generation_index=generation_index,
        cells=cells,
        negative_novelty_cells=negative_novelty_cells,
        immigrant_source=immigrant_source,
        operators=operators,
        parameters=config_parameters,
        selection_state=selection_state,
        evidence_context=context,
        frozen_construction_catalog=frozen_construction_catalog,
        random_immigrant_only=random_immigrant_only,
    )
    accepted, accepted_counts, _seen_programs, _seen_identities = _accepted_state(
        entries, archive
    )
    ledger_identities = {
        str(record["candidateIdentitySha256"])
        for record in ledger.get("records") or []
    }
    recovered_proposal_slots = 0
    for candidate in accepted:
        record = _ledger_record(candidate)
        if record["candidateIdentitySha256"] not in ledger_identities:
            ledger["records"].append(record)
            ledger_identities.add(record["candidateIdentitySha256"])
            recovered_proposal_slots += 1
    ledger["proposalSlotCounters"]["acceptedUniqueProposalSlots"] += (
        recovered_proposal_slots
    )
    _ledger_refresh_counts(ledger)
    ledger["proposalSlotCounters"]["proposalsObserved"] = max(
        int(ledger["proposalSlotCounters"].get("proposalsObserved") or 0),
        len(entries),
    )
    _save_identity_ledger(ledger_file, ledger)
    identity_index = _ledger_identity_index(ledger)
    validator = SubprocessCandidateValidator(
        validator_command, timeout_seconds=validator_timeout_seconds
    )
    authored_validator_provenance = validator_provenance(
        validator,
        validation_contract={
            "schemaVersion": "temporal_qd_validator_contract_v1",
            "validatorSchema": "temporal_search_candidate_validation_v1",
        },
    )
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    new_proposals = 0
    while (
        len(accepted) < target
        and len(entries) < int(config_parameters["maxProposalAttempts"])
        and (max_new_proposals is None or new_proposals < max_new_proposals)
    ):
        ordinal = len(entries)
        profile, metadata = _proposal(
            config_sha=config["configSha256"],
            generation_index=generation_index,
            proposal_ordinal=ordinal,
            cells=cells,
            negative_novelty_cells=negative_novelty_cells,
            immigrant_source=immigrant_source,
            operators=operators,
            parameters=config_parameters,
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=validator,
            evidence_context=context,
            frozen_construction_catalog=frozen_construction_catalog,
            random_immigrant_only=random_immigrant_only,
        )
        proposal = metadata["proposalMaterial"]
        entry: dict[str, Any] = {
            "schemaVersion": QD_ENTRY_SCHEMA,
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "proposalOrdinal": ordinal,
            "originKind": metadata["originKind"],
            "proposal": proposal,
        }
        ledger["proposalSlotCounters"]["proposalsObserved"] += 1
        if profile is None:
            entry["disposition"] = str(metadata["proposalIssue"])
        else:
            structural_steps = metadata.get("steps") or []
            construction_operator_id = next(
                (
                    str(step["operatorId"])
                    for step in reversed(structural_steps)
                    if isinstance(step, Mapping)
                    and step.get("operatorId") in CONSTRUCTION_OPERATOR_IDS
                ),
                None,
            )
            reachability = _static_reachability(
                profile,
                operator_id=construction_operator_id,
            )
            entry["managementReachabilitySha256"] = reachability["reachabilitySha256"]
            entry["managementReachabilityIssueCounts"] = reachability["issueCounts"]
            if reachability["acceptable"] is not True:
                entry["disposition"] = "static_reachability_rejected"
            else:
                scoped = (
                    _predeclared_lake_scope_report(
                        profile,
                        context,
                        frozen_construction_catalog=frozen_construction_catalog,
                    )
                    if context.get("orderedWindowPlanSemantic")
                    else None
                )
                if scoped is not None:
                    entry["predeclaredLakeScope"] = scoped
                if scoped is not None and scoped["acceptable"] is not True:
                    # This covers all origins, including an immigrant whose
                    # generator lineage changed a timeframe.  No out-of-scope
                    # profile may become a QD population member and later
                    # poison an otherwise immutable campaign freeze.
                    entry["disposition"] = "predeclared_lake_scope_rejected"
                else:
                    if metadata["originKind"] == "structural_offspring":
                        validation_record = _clone(
                            metadata["finalValidation"], name="final structural validation"
                        )
                    else:
                        source_sha = canonical_sha256(profile)
                        validation_record = _validation_record(
                            validator.validate(
                                candidate_id=str(metadata["candidateId"]),
                                source_profile=profile,
                                expected_raw_source_profile_sha256=source_sha,
                            )
                        )
                    entry.update(
                        {
                            "validationStatus": validation_record.get("status"),
                            "validationReportSha256": validation_record.get(
                                "validationReportSha256"
                            ),
                            "validatedProgramSha256": validation_record.get(
                                "programSha256"
                            ),
                            **(
                                {
                                    "validatedProfileSnapshotSha256": validation_record.get(
                                        "profileSnapshotSha256"
                                    )
                                }
                                if generation_funnel_enabled
                                else {}
                            ),
                            "issueCodes": validation_record.get("issueCodes") or [],
                        }
                    )
                    if validation_record.get("candidateAcceptable") is not True:
                        entry["disposition"] = "native_validator_rejected"
                    else:
                        candidate = _accepted_candidate(
                            profile=profile,
                            validation=validation_record,
                            metadata=metadata,
                            generation_index=generation_index,
                            birth_ordinal=len(accepted),
                            proposal_ordinal=ordinal,
                            evidence_context=context,
                            authored_validator_provenance=authored_validator_provenance,
                        )
                        duplicate_reason, identity_checks = _ledger_duplicate_check(
                            ledger, candidate, identity_index=identity_index
                        )
                        entry["identityChecks"] = identity_checks
                        entry["canonicalEvidenceIdentitySha256"] = candidate[
                            "canonicalEvidenceIdentitySha256"
                        ]
                        if duplicate_reason is not None:
                            entry["disposition"] = duplicate_reason
                        else:
                            entry["candidate"] = candidate
                            entry["disposition"] = "accepted"
                            accepted.append(candidate)
                            accepted_counts[metadata["originKind"]] += 1
                            _ledger_accept(
                                ledger, candidate, identity_index=identity_index
                            )
        # This is deliberately opt-in: historical QD generation identities stay
        # byte-for-byte stable unless the immutable funnel was frozen at run
        # creation.  The journal is still the sole attempt authority; this
        # compact stage projection makes each materialized proposal's native
        # boundary explicit instead of requiring a later success to imply it.
        if generation_funnel_enabled and profile is not None:
            funnel_candidate = {
                "schemaVersion": "temporal_qd_proposal_funnel_stage_v1",
                "candidateId": str(metadata["candidateId"]),
                "rawSourceProfileSha256": canonical_sha256(profile),
            }
            disposition = str(entry["disposition"])
            static_rejected = disposition in {
                "static_reachability_rejected",
                "predeclared_lake_scope_rejected",
            }
            funnel_candidate["staticReachability"] = {
                "outcome": "rejected" if static_rejected else "reachable",
                "reasons": [disposition] if static_rejected else [],
            }
            if not static_rejected:
                native_rejected = disposition == "native_validator_rejected"
                native = {
                    "outcome": "rejected" if native_rejected else "valid",
                    "reasons": [disposition] if native_rejected else [],
                    "resolvedProfileSha256": entry.get(
                        "validatedProfileSnapshotSha256"
                    ),
                    "programSha256": entry.get("validatedProgramSha256"),
                    "validationReportSha256": entry.get("validationReportSha256"),
                }
                funnel_candidate["nativeValidation"] = native
                if not native_rejected:
                    duplicate = disposition != "accepted"
                    funnel_candidate["admission"] = {
                        "outcome": "rejected_duplicate" if duplicate else "admitted",
                        "reasons": [disposition] if duplicate else [],
                        **(
                            {
                                "canonicalEvidenceIdentitySha256": entry.get(
                                    "canonicalEvidenceIdentitySha256"
                                )
                            }
                            if not duplicate
                            else {}
                        ),
                    }
            entry["funnelCandidate"] = funnel_candidate
        entry["entrySha256"] = canonical_sha256(entry)
        _write_once(_entry_path(root, ordinal), entry)
        entries.append(entry)
        new_proposals += 1
        _save_identity_ledger(ledger_file, ledger)
        accounting = _proposal_accounting(
            entries,
            random_immigrant_only=random_immigrant_only,
            allow_empty_quality_bootstrap=allow_empty_quality_bootstrap,
            quality_parent_cell_count_at_generation_start=len(cells),
        )
        checkpoint = {
            "schemaVersion": QD_CHECKPOINT_SCHEMA,
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "nextProposalOrdinal": len(entries),
            "rngCheckpoint": {
                "policy": "per_proposal_content_seed_v1",
                "rootSeed": config_parameters["seed"],
                "nextProposalOrdinal": len(entries),
            },
            "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
            "selectionState": selection_state,
            "acceptedCount": len(accepted),
            "acceptedOriginCounts": dict(sorted(accepted_counts.items())),
            "proposalAccounting": accounting,
            **_ledger_public_counts(ledger),
            "completed": False,
        }
        checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
        _replace(root / "checkpoint.json", checkpoint)

    if len(accepted) < target:
        if max_new_proposals is not None and new_proposals >= max_new_proposals:
            return {
                "schemaVersion": "temporal_qd_generation_progress_v3",
                "configSha256": config["configSha256"],
                "generationIndex": generation_index,
                "proposalCount": len(entries),
                "acceptedCount": len(accepted),
                "targetUniqueCandidates": target,
                "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
                **_ledger_public_counts(ledger),
                "completed": False,
            }
        raise TemporalDiscoveryGenerationExhausted(
            f"QD generation accepted {len(accepted)} of {target} unique candidates"
        )

    proposal_order = [
        str(entry["candidate"]["candidateId"])
        for entry in entries
        if entry.get("disposition") == "accepted"
    ]
    accepted.sort(key=lambda item: str(item["candidateId"]))
    accounting = _proposal_accounting(
        entries,
        random_immigrant_only=random_immigrant_only,
        allow_empty_quality_bootstrap=allow_empty_quality_bootstrap,
        quality_parent_cell_count_at_generation_start=len(cells),
    )
    population = {
        "schemaVersion": QD_POPULATION_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "targetUniqueCandidates": target,
        "originCounts": accounting["originAcceptedCounts"],
        "proposalOrderCandidateIds": proposal_order,
        "candidateCount": len(accepted),
        "candidates": accepted,
        "authoredValidationBindingRequired": all(
            "authoredValidationBinding" in candidate
            and "authoredValidationBindingSha256" in candidate
            for candidate in accepted
        ),
        "predeclaredEvidenceContextSha256": context_sha,
        **(
            {"constructionOperatorPolicySha256": construction_policy["policySha256"]}
            if construction_policy["enabled"]
            else {}
        ),
        "proposalSlots": {
            "targetUniqueCandidates": target,
            "acceptedUniqueCandidates": len(accepted),
            "proposalAttempts": len(entries),
            "remainingUniqueCandidateSlots": max(0, target - len(accepted)),
        },
        **_ledger_public_counts(ledger),
    }
    population["populationSha256"] = canonical_sha256(population)
    journal = {
        "schemaVersion": QD_JOURNAL_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "proposalCount": len(entries),
        "acceptedCount": len(accepted),
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        **(
            {"constructionOperatorPolicySha256": construction_policy["policySha256"]}
            if construction_policy["enabled"]
            else {}
        ),
        **accounting,
        "proposalSlots": {
            "targetUniqueCandidates": target,
            "acceptedUniqueCandidates": len(accepted),
            "proposalAttempts": len(entries),
            "remainingUniqueCandidateSlots": max(0, target - len(accepted)),
        },
        **_ledger_public_counts(ledger),
        "entrySha256s": [item["entrySha256"] for item in entries],
    }
    journal["journalSha256"] = canonical_sha256(journal)
    _write_once(root / "population.json", population)
    _write_once(root / "generation-journal.json", journal)
    checkpoint = {
        "schemaVersion": QD_CHECKPOINT_SCHEMA,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "nextProposalOrdinal": len(entries),
        "rngCheckpoint": {
            "policy": "per_proposal_content_seed_v1",
            "rootSeed": config_parameters["seed"],
            "nextProposalOrdinal": len(entries),
        },
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        "selectionState": selection_state,
        "acceptedCount": len(accepted),
        "acceptedOriginCounts": accounting["originAcceptedCounts"],
        "proposalAccounting": accounting,
        "proposalSlots": {
            "targetUniqueCandidates": target,
            "acceptedUniqueCandidates": len(accepted),
            "proposalAttempts": len(entries),
            "remainingUniqueCandidateSlots": max(0, target - len(accepted)),
        },
        **_ledger_public_counts(ledger),
        "completed": True,
        "populationSha256": population["populationSha256"],
        "journalSha256": journal["journalSha256"],
    }
    checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
    _replace(root / "checkpoint.json", checkpoint)
    manifest = _manifest(root, population_sha=population["populationSha256"])
    return {
        "schemaVersion": "temporal_qd_generation_result_v3",
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "populationSha256": population["populationSha256"],
        "journalSha256": journal["journalSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "proposalCount": len(entries),
        "candidateCount": len(accepted),
        "originProposalCounts": accounting["originProposalCounts"],
        "originAcceptedCounts": accounting["originAcceptedCounts"],
        "proposalSlots": population["proposalSlots"],
        **_ledger_public_counts(ledger),
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        "completed": True,
        "marketEvidenceReadDuringGeneration": False,
        "gatewayContactedDuringGeneration": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    archive = subparsers.add_parser("archive")
    archive.add_argument("--population", type=Path, required=True)
    archive.add_argument("--result-root", type=Path, required=True)
    archive.add_argument("--output", type=Path, required=True)
    archive.add_argument("--generation-index", type=int, required=True)
    archive.add_argument("--previous-archive", type=Path)
    archive.add_argument("--generation-journal", type=Path)
    archive.add_argument("--cell-capacity", type=int, default=4)
    archive.add_argument("--minimum-total-trades", type=int, default=8)
    archive.add_argument("--minimum-trades-per-window", type=int, default=4)
    archive.add_argument("--cap-trades", type=int, default=20)

    generate = subparsers.add_parser("generate")
    generate.add_argument("--parent-archive", type=Path, required=True)
    generate.add_argument("--source-preparation", type=Path)
    generate.add_argument("--base-generator-root", type=Path)
    generate.add_argument("--confirmed-entry-admission-root", type=Path)
    generate.add_argument("--immigrant-continuation-start", type=int, default=0)
    generate.add_argument("--allow-empty-quality-bootstrap", action="store_true")
    generate.add_argument("--validator-command-file", type=Path)
    generate.add_argument("--output-root", type=Path, required=True)
    generate.add_argument("--generation-index", type=int, required=True)
    generate.add_argument("--parameters", type=Path)
    generate.add_argument("--construction-catalog", type=Path)
    generate.add_argument("--validator-timeout-seconds", type=float, default=60.0)
    generate.add_argument("--max-new-proposals", type=int)
    generate.add_argument("--bidirectional-pair-config", type=Path, help="closed pair authority JSON; disables the v2 continuation path")
    args = parser.parse_args()

    if args.command == "archive":
        result = build_qd_archive(
            population_path=args.population,
            result_root=args.result_root,
            output_path=args.output,
            generation_index=args.generation_index,
            previous_archive_path=args.previous_archive,
            generation_journal_path=args.generation_journal,
            cell_capacity=args.cell_capacity,
            minimum_total_trades=args.minimum_total_trades,
            minimum_trades_per_window=args.minimum_trades_per_window,
            cap_trades=args.cap_trades,
        )
    else:
        if args.bidirectional_pair_config is None and any(value is None for value in (args.source_preparation, args.base_generator_root, args.confirmed_entry_admission_root, args.validator_command_file)):
            parser.error("legacy generation requires v2 source paths and --validator-command-file")
        command = json.loads(args.validator_command_file.read_text(encoding="utf-8")) if args.validator_command_file is not None else []
        if not isinstance(command, list) or not all(isinstance(value, str) for value in command):
            raise TemporalDiscoveryContractError("validator command file must contain a string array")
        parameters = (
            _read(args.parameters, name="QD parameter file")
            if args.parameters is not None
            else None
        )
        generation_kwargs = dict(
            parent_archive_path=args.parent_archive,
            immigrant_continuation_start=args.immigrant_continuation_start,
            allow_empty_quality_bootstrap=args.allow_empty_quality_bootstrap,
            output_root=args.output_root,
            generation_index=args.generation_index,
            parameters=parameters,
            validator_timeout_seconds=args.validator_timeout_seconds,
            max_new_proposals=args.max_new_proposals,
            construction_catalog_path=args.construction_catalog,
        )
        if args.bidirectional_pair_config is None:
            result = generate_qd_generation(
                **generation_kwargs,
                source_preparation_path=args.source_preparation,
                base_generator_root=args.base_generator_root,
                confirmed_entry_admission_root=args.confirmed_entry_admission_root,
                validator_command=command,
            )
        else:
            from .temporal_qd_pair_factory import PairAuthorityBundle, load_pair_run_config, pair_policy_from_config
            frozen = load_pair_run_config(_read(args.bidirectional_pair_config, name="bidirectional pair run config"))
            with PairAuthorityBundle(frozen) as authority:
                result = generate_qd_generation(
                    **generation_kwargs,
                    bidirectional_pair_policy=pair_policy_from_config(frozen),
                    bidirectional_pair_factory=authority.factory,
                    bidirectional_module_authority=authority.operator,
                    bidirectional_native_validator=authority.validator,
                    bidirectional_pair_compiler=authority.compiler,
                    bidirectional_operator_implementation_identity=frozen["operatorImplementation"],
                )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_QD_PARAMETERS",
    "QD_POLICY",
    "QD_POLICY_NAME",
    "QD_POLICY_SHA256",
    "QD_VERSION",
    "build_qd_archive",
    "generate_qd_generation",
    "qd_behavior_descriptor",
    "qd_canonical_evidence_identity",
    "qd_construction_operator_policy",
    "qd_predeclared_evidence_context",
    "select_qd_archive",
]
