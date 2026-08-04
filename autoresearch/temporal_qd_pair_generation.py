"""Deterministic, immutable construction of v3/both QD pair proposals.

This module is intentionally separate from the frozen legacy v2 proposal loop.
It accepts only :class:`FrozenPair` parents / factory output and records the
complete material needed to replay a proposal without reading a mutable catalog.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import os
import random
from typing import Any, Iterator, Protocol
import uuid

from .temporal_bidirectional_genome import (
    BidirectionalGenomeError,
    CanonicalPairCompiler,
    FrozenModule,
    FrozenPair,
    HoldMutationPlan,
    IdentitySnapshot,
    NativeModuleValidator,
    apply_pair_hold_mutation,
    canonical_hold,
    canonical_json,
    canonical_sha256,
    deterministic_same_side_crossover,
    proposal_side,
)
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_observability import (
    PerformanceTrace,
    activate_performance_trace,
    assert_performance_resource_guard,
    flush_performance_events,
    record_performance_interval,
    start_performance_interval,
    timed_span,
    timing_scope,
)


PAIR_GENERATION_SCHEMA = "temporal_qd_pair_generation_v1"
PAIR_PROPOSAL_SCHEMA = "temporal_qd_pair_proposal_v1"
CROSSOVER_PROPOSAL_KIND = "temporal_qd_same_side_crossover_v1"
PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA = "temporal_qd_pair_executable_semantic_record_v1"
DEFAULT_MAX_PROPOSAL_ATTEMPTS = 20_000
PAIR_GENERATION_IMPLEMENTATION_LEGACY = "legacy"
PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED = "optimized"
_PAIR_GENERATION_IMPLEMENTATIONS = frozenset(
    (
        PAIR_GENERATION_IMPLEMENTATION_LEGACY,
        PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED,
    )
)


def _unbiased_choice(seed: str, *, size: int) -> int:
    """Hash/rejection-sample a uniform finite bucket without modulo bias."""

    if size < 1:
        raise TemporalDiscoveryContractError("pair selection bucket size must be positive")
    limit = (1 << 256) - ((1 << 256) % size)
    attempt = 0
    while True:
        value = int(canonical_sha256({"seed": seed, "attempt": attempt})[7:], 16)
        if value < limit:
            return value % size
        attempt += 1


def _mutation_depth_from_bucket(bucket: int) -> int:
    if not 0 <= bucket < 20:
        raise TemporalDiscoveryContractError("pair mutation depth bucket is outside 0..19")
    return 1 if bucket < 14 else 2 if bucket < 19 else 3


def _mutation_depth_for_seed(seed: str) -> int:
    # Twenty equiprobable buckets encode exactly 14/5/1 = 70/25/5.
    return _mutation_depth_from_bucket(_unbiased_choice(seed, size=20))


def _pair_genome_semantic_sha256(pair: FrozenPair) -> str:
    """Identity of executable authored modules, excluding proposal lineage.

    ``FrozenPair.identity_sha256`` intentionally includes lineage, while the
    compiled v3 raw profile includes its pair compilation candidate ID.  Both
    therefore differ when two proposal seeds select the same two modules.  A
    unique QD proposal slot is semantic: bind the exact long/short v2 module
    profiles, which include topology, resources, indicators, and management
    but not proposal provenance.
    """

    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_pair_genome_semantics_v1",
            "longProfileSha256": pair.long.profile_sha256,
            "shortProfileSha256": pair.short.profile_sha256,
        }
    )


class TypedPairFactory(Protocol):
    """Finite deterministic factory; it must return both frozen sides."""

    def create_pair(self, *, proposal_seed: str) -> FrozenPair: ...


class PairModuleOperator(Protocol):
    """Side-local grammar / indicator operations over immutable modules."""

    def grammar_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]: ...
    def apply_grammar(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]: ...
    def indicator_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]: ...
    def apply_indicator(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]: ...
    def hold_policy_choices(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]: ...
    def crossover(self, left_program: Mapping[str, Any], right_program: Mapping[str, Any], *, direction: str, proposal_seed: str) -> Mapping[str, Any]: ...
    def compile_program(self, template: FrozenModule, program: Mapping[str, Any], *, candidate_id: str) -> FrozenModule: ...


class TypedGrammarPairOperator:
    """Concrete bridge to typed fragments and ``IndicatorLearningRegistry``.

    ``grammar_factory`` is deliberately supplied by the frozen run authority;
    it receives the module snapshot rather than a catalog alias.  The registry
    is likewise constructed from the already frozen catalog payload.
    """

    def __init__(self, *, grammar_factory: Callable[[FrozenModule], Any], native_validator: NativeModuleValidator, indicator_registry: Any | None = None, hold_operator_policy: Mapping[str, Any]) -> None:
        self._grammar_factory = grammar_factory
        self._native_validator = native_validator
        self._indicator_registry = indicator_registry
        policy = _clone(hold_operator_policy)
        choices = policy.get("choices") if isinstance(policy, Mapping) else None
        if not isinstance(choices, list):
            raise TemporalDiscoveryContractError("typed pair hold operator requires a frozen choices policy")
        self._hold_operator_choices = tuple(_clone(choice) for choice in choices)

    @staticmethod
    def _program(module: FrozenModule, program: Mapping[str, Any] | None = None) -> Any:
        from .temporal_typed_motif_grammar import Fragment, ModuleProgram

        # FrozenModule intentionally stores recursive MappingProxy/tuple
        # values.  Grammar construction needs ordinary JSON containers.
        raw = _mutable(program) if program is not None else module.canonical_payload()["program"]
        if raw.get("direction") != module.direction or not isinstance(raw.get("fragments"), list):
            raise TemporalDiscoveryContractError("typed pair module program is not exact")
        fragments = tuple(Fragment(f"replay_{index}", str(item["productionId"]), dict(item["resources"]), dict(item["choices"])) for index, item in enumerate(raw["fragments"]))
        return ModuleProgram(module.direction, fragments, tuple(module.lineage))

    def _grammar(self, module: FrozenModule) -> Any:
        grammar = self._grammar_factory(module)
        if grammar is None:
            raise TemporalDiscoveryContractError("frozen typed grammar authority is unavailable")
        if getattr(grammar, "context_sha256", None) != module.grammar_context.sha256:
            raise TemporalDiscoveryContractError("typed grammar authority does not match the frozen module context")
        return grammar

    @staticmethod
    def _freeze(template: FrozenModule, *, program: Mapping[str, Any], profile: Mapping[str, Any], report: Mapping[str, Any], lineage: Sequence[Mapping[str, Any]]) -> FrozenModule:
        # Side-local indicator operations preserve the parent's typed program.
        # That program is stored frozen, while ``FrozenModule.freeze`` accepts
        # canonical JSON containers only.  Thaw all boundary values so a
        # frozen snapshot can be safely rebound after native validation.
        return FrozenModule.freeze(
            program=_mutable(program),
            profile=_mutable(profile),
            grammar_context=template.grammar_context,
            catalog=template.catalog,
            policy=template.policy,
            native_authority=template.native_authority,
            native_report=_mutable(report),
            lineage=_mutable(lineage),
        )

    def grammar_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        return self._grammar(module).enumerate_operations(self._program(module))

    def apply_grammar(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        grammar = self._grammar(module)
        child = grammar.apply(self._program(module), plan)
        compiled = grammar.compile_module(child, candidate_id=candidate_id)
        frozen = self._freeze(module, program=compiled.program, profile=compiled.profile, report=compiled.native_report, lineage=[*[_clone(item) for item in module.lineage], {"operation": "typed_grammar", "side": module.direction, "plan": _clone(plan), "planSha256": canonical_sha256(plan)}])
        audit = {"schemaVersion": "temporal_qd_typed_grammar_operation_audit_v1", "side": module.direction, "plan": _clone(plan), "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "nativeValidationReportSha256": frozen.native_validation_report_sha256}
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def _registry(self, module: FrozenModule) -> Any | None:
        return self._indicator_registry(module) if callable(self._indicator_registry) else self._indicator_registry

    def indicator_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        registry = self._registry(module)
        return [] if registry is None else registry.enumerate_plans(module.canonical_payload()["profile"])

    def apply_indicator(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        registry = self._registry(module)
        if registry is None:
            raise TemporalDiscoveryContractError("indicator learning registry is not frozen for this pair run")
        operator = registry.get(str(plan.get("operatorId") or ""))
        source_profile = module.canonical_payload()["profile"]
        preview = operator.preview(source_profile, plan)
        report = self._native_validator.validate_v2(profile=preview, candidate_id=candidate_id)
        child_program = report.get("programSha256")
        child, application = operator.apply(source_profile, plan, parent_validated_program_sha256=module.native_program_sha256, child_validated_program_sha256=child_program)
        if child != preview:
            raise TemporalDiscoveryContractError("indicator-learning preview/application diverged")
        frozen = self._freeze(module, program=module.program, profile=child, report=report, lineage=[*[_clone(item) for item in module.lineage], {"operation": "indicator_learning", "side": module.direction, "plan": _clone(plan), "planSha256": plan.get("planSha256"), "application": _clone(application)}])
        audit = {"schemaVersion": "temporal_qd_indicator_operation_audit_v1", "side": module.direction, "operatorId": plan.get("operatorId"), "planSha256": plan.get("planSha256"), "applicationSha256": application.get("applicationSha256"), "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "nativeValidationReportSha256": frozen.native_validation_report_sha256}
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def hold_policy_choices(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        """Return the run-frozen native values, never a generated range."""

        del module
        return tuple(_clone(choice) for choice in self._hold_operator_choices)

    def crossover(self, left_program: Mapping[str, Any], right_program: Mapping[str, Any], *, direction: str, proposal_seed: str) -> Mapping[str, Any]:
        raise TemporalDiscoveryContractError("typed crossover must be bound to frozen side modules")

    def crossover_for_modules(self, left: FrozenModule, right: FrozenModule) -> Mapping[str, Any]:
        return self.crossover_for_programs(left, left.program, right.program)

    def crossover_for_programs(self, template: FrozenModule, left_program: Mapping[str, Any], right_program: Mapping[str, Any]) -> Mapping[str, Any]:
        grammar = self._grammar(template)
        child = grammar.crossover(self._program(template, left_program), self._program(template, right_program), direction=template.direction)
        return child.canonical()

    def compile_program(self, template: FrozenModule, program: Mapping[str, Any], *, candidate_id: str) -> FrozenModule:
        grammar = self._grammar(template)
        compiled = grammar.compile_module(self._program(template, program), candidate_id=candidate_id)
        return self._freeze(template, program=compiled.program, profile=compiled.profile, report=compiled.native_report, lineage=[*[_clone(item) for item in template.lineage], {"operation": "same_side_crossover", "side": template.direction, "childProgramSha256": canonical_sha256(program)}])


def _clone(value: Any) -> Any:
    # ``FrozenModule`` deliberately stores its persisted material as recursive
    # MappingProxyType/tuple values.  Proposal operators extend that immutable
    # lineage, so cloning must first thaw the snapshot representation without
    # changing its canonical JSON identity.
    return __import__("json").loads(canonical_json(_mutable(value)))


def _mutable(value: Any) -> Any:
    """Recursively thaw frozen snapshot containers without changing identity."""

    if isinstance(value, Mapping):
        return {str(key): _mutable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable(item) for item in value]
    return value


def _sorted(plans: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((_clone(dict(plan)) for plan in plans), key=canonical_sha256)


def _hold_plans(module: FrozenModule, authority: PairModuleOperator) -> list[dict[str, Any]]:
    plans = (((module.profile.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
    output = []
    choices = _sorted(authority.hold_policy_choices(module))
    for plan in plans:
        if not isinstance(plan, Mapping) or not isinstance(plan.get("id"), str):
            continue
        old = canonical_hold(plan.get("holdPolicy"))
        for choice in choices:
            # ``none`` is represented in mutation identity but applies by
            # removing the optional native field.  Do not schedule a no-op.
            if canonical_hold(choice) != old:
                output.append({"kind": "hold", "planId": plan["id"], "newHold": choice})
    return _sorted(output)


def _operation_choices(module: FrozenModule, authority: PairModuleOperator) -> list[dict[str, Any]]:
    rows = []
    rows.extend({"kind": "typed_grammar", "plan": plan} for plan in _sorted(authority.grammar_plans(module)))
    rows.extend({"kind": "indicator_learning", "plan": plan} for plan in _sorted(authority.indicator_plans(module)))
    rows.extend(_hold_plans(module, authority))
    return _sorted(rows)


def _compile_pair(long: FrozenModule, short: FrozenModule, *, parent: FrozenPair, pair_compiler: CanonicalPairCompiler, candidate_id: str, lineage: Sequence[Mapping[str, Any]]) -> FrozenPair:
    return FrozenPair.compile(
        long=long,
        short=short,
        pair_compiler_identity=parent.pair_compiler,
        pair_compiler=pair_compiler,
        candidate_id=candidate_id,
        side_targeted_lineage=lineage,
    )


def propose_pair(
    *,
    proposal_seed: str,
    parent: FrozenPair | None,
    pair_factory: TypedPairFactory | None,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    crossover_parent: FrozenPair | None = None,
    replay_operation: Mapping[str, Any] | None = None,
) -> tuple[FrozenPair | None, dict[str, Any]]:
    """Make exactly one auditable pair attempt; no v2 profile is ever returned."""
    seed = str(proposal_seed)
    side = proposal_side(seed)
    if parent is None:
        if pair_factory is None:
            raise TemporalDiscoveryContractError("pair immigrants require an explicit typed pair factory")
        with timed_span("proposal.immigrant.factory_create_pair"):
            pair = pair_factory.create_pair(proposal_seed=seed)
        if not isinstance(pair, FrozenPair):
            raise TemporalDiscoveryContractError("pair immigrant factory must return FrozenPair")
        with timed_span("proposal.immigrant.factory_audit"):
            factory_audit = None
            audit_pair = getattr(pair_factory, "audit_pair", None)
            if callable(audit_pair):
                factory_audit = audit_pair(pair)
                if not isinstance(factory_audit, Mapping):
                    raise TemporalDiscoveryContractError(
                        "pair immigrant factory audit must be an object"
                    )
        with timed_span("proposal.immigrant.build_payload"):
            payload = {
                "schemaVersion": PAIR_PROPOSAL_SCHEMA,
                "proposalSeed": seed,
                "originKind": "random_immigrant",
                "side": side,
                "factoryPair": pair.canonical_payload(),
                "pairIdentitySha256": pair.identity_sha256,
                "disposition": "materialized",
                **(
                    {"factoryConstructionAudit": _clone(factory_audit)}
                    if factory_audit is not None
                    else {}
                ),
            }
        with timed_span("proposal.immigrant.hash_payload"):
            payload["proposalSha256"] = canonical_sha256(payload)
        return pair, payload

    # Reconstruct first so tampering/missing opposite modules never reach an operator.
    try:
        parent = FrozenPair.from_payload(parent.canonical_payload())
    except BidirectionalGenomeError as exc:
        raise TemporalDiscoveryContractError("pair parent material is not restart-safe") from exc
    target = parent.long if side == "long" else parent.short
    other = parent.short if side == "long" else parent.long
    choices = _operation_choices(target, module_authority)
    base: dict[str, Any] = {
        "schemaVersion": PAIR_PROPOSAL_SCHEMA,
        "proposalSeed": seed,
        "originKind": "structural_offspring",
        "side": side,
        "parentPair": parent.canonical_payload(),
        "parentPairIdentitySha256": parent.identity_sha256,
        "untouchedOppositeModuleIdentitySha256": other.identity_sha256,
    }
    if not choices:
        payload = {**base, "disposition": "no_eligible_side_operation", "eligibleOperationCount": 0, "rejection": {"schemaVersion": "temporal_qd_pair_rejection_audit_v1", "reasonCode": "no_eligible_side_operation", "side": side, "eligibleOperationCount": 0}}
        payload["proposalSha256"] = canonical_sha256(payload)
        return None, payload
    selected = _clone(replay_operation) if replay_operation is not None else choices[int(canonical_sha256({"seed": seed, "parent": parent.identity_sha256})[-2:], 16) % len(choices)]
    if selected not in choices:
        raise TemporalDiscoveryContractError("stored pair proposal operation is no longer exact/canonical")
    candidate_id = "qd_pair_" + canonical_sha256({"seed": seed, "parent": parent.identity_sha256, "operation": selected})[7:35]
    try:
        if selected["kind"] == "typed_grammar":
            changed, audit = module_authority.apply_grammar(target, selected["plan"], candidate_id=candidate_id + "_" + side)
        elif selected["kind"] == "indicator_learning":
            changed, audit = module_authority.apply_indicator(target, selected["plan"], candidate_id=candidate_id + "_" + side)
        elif selected["kind"] == "hold":
            hold = HoldMutationPlan.create(target, plan_id=selected["planId"], new_hold=selected["newHold"])
            changed_pair = apply_pair_hold_mutation(parent, hold, native_validator=native_validator, pair_compiler=pair_compiler, candidate_id=candidate_id)
            payload = {**base, "disposition": "materialized", "operation": selected, "holdMutationPlan": hold.canonical_payload(), "operationAudit": {"schemaVersion": "temporal_qd_pair_hold_audit_v1", "side": side, "holdMutationPlanSha256": hold.plan_sha256}, "pair": changed_pair.canonical_payload(), "pairIdentitySha256": changed_pair.identity_sha256}
            payload["proposalSha256"] = canonical_sha256(payload)
            return changed_pair, payload
        else:
            raise TemporalDiscoveryContractError("pair operation kind is unknown")
        if changed.direction != side:
            raise TemporalDiscoveryContractError("side-local operation emitted an opposite-side module")
        lineage = [*_clone(parent.canonical_payload())["sideTargetedLineage"], {"operation": selected["kind"], "side": side, "proposalSeed": seed, "operationSha256": canonical_sha256(selected), "audit": _clone(audit)}]
        long, short = (changed, other) if side == "long" else (other, changed)
        pair = _compile_pair(long, short, parent=parent, pair_compiler=pair_compiler, candidate_id=candidate_id, lineage=lineage)
        payload = {**base, "disposition": "materialized", "operation": selected, "operationAudit": _clone(audit), "changedModule": changed.canonical_payload(), "pair": pair.canonical_payload(), "pairIdentitySha256": pair.identity_sha256}
        payload["proposalSha256"] = canonical_sha256(payload)
        return pair, payload
    except (BidirectionalGenomeError, TemporalDiscoveryContractError) as exc:
        # Exception messages commonly include subprocess text.  Persist a
        # stable typed reason/audit instead of making replay depend on it.
        payload = {**base, "disposition": "operation_rejected", "operation": selected, "rejection": {"schemaVersion": "temporal_qd_pair_rejection_audit_v1", "reasonCode": "operator_rejected", "exceptionType": type(exc).__name__, "side": side, "operationSha256": canonical_sha256(selected)}}
        payload["proposalSha256"] = canonical_sha256(payload)
        return None, payload


def _propose_pair_sequence(
    *,
    proposal_seed: str,
    parent: FrozenPair,
    mutation_depth: int,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    parent_selection: Mapping[str, Any] | None = None,
    replay_steps: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[FrozenPair | None, dict[str, Any]]:
    """Apply 1..3 side-local operations as one replayable offspring."""

    if mutation_depth not in (1, 2, 3):
        raise TemporalDiscoveryContractError("pair mutation depth must be 1, 2, or 3")
    if replay_steps is not None:
        if not replay_steps:
            raise TemporalDiscoveryContractError("pair multi-operation replay has no stored steps")
        if len(replay_steps) > mutation_depth:
            raise TemporalDiscoveryContractError("pair multi-operation replay has more steps than its planned mutation depth")
    root = FrozenPair.from_payload(parent.canonical_payload())
    current = root
    steps: list[dict[str, Any]] = []
    step_count = len(replay_steps) if replay_steps is not None else mutation_depth
    for index in range(step_count):
        step_seed = canonical_sha256({"proposalSeed": proposal_seed, "mutationStep": index, "parentPairIdentitySha256": current.identity_sha256})
        if replay_steps is not None and not isinstance(replay_steps[index], Mapping):
            raise TemporalDiscoveryContractError("pair multi-operation replay step is not an object")
        operation = None if replay_steps is None else replay_steps[index].get("operation")
        child, step = propose_pair(
            proposal_seed=step_seed,
            parent=current,
            pair_factory=None,
            module_authority=module_authority,
            native_validator=native_validator,
            pair_compiler=pair_compiler,
            replay_operation=operation,
        )
        steps.append(step)
        if child is None:
            if replay_steps is not None and index + 1 != step_count:
                raise TemporalDiscoveryContractError("pair multi-operation replay has stored steps after a terminal disposition")
            payload = {"schemaVersion": PAIR_PROPOSAL_SCHEMA, "proposalSeed": proposal_seed, "originKind": "structural_offspring", "side": step["side"], "parentPair": root.canonical_payload(), "parentPairIdentitySha256": root.identity_sha256, "mutationDepth": mutation_depth, "mutationSteps": steps, "disposition": "operation_rejected", **({"parentSelection": _clone(parent_selection)} if parent_selection is not None else {})}
            payload["proposalSha256"] = canonical_sha256(payload)
            return None, payload
        if _pair_genome_semantic_sha256(child) == _pair_genome_semantic_sha256(current):
            if replay_steps is not None and index + 1 != step_count:
                raise TemporalDiscoveryContractError("pair multi-operation replay has stored steps after a terminal disposition")
            payload = {"schemaVersion": PAIR_PROPOSAL_SCHEMA, "proposalSeed": proposal_seed, "originKind": "structural_offspring", "side": step["side"], "parentPair": root.canonical_payload(), "parentPairIdentitySha256": root.identity_sha256, "mutationDepth": mutation_depth, "mutationSteps": steps, "disposition": "no_op_proposal", **({"parentSelection": _clone(parent_selection)} if parent_selection is not None else {})}
            payload["proposalSha256"] = canonical_sha256(payload)
            return None, payload
        current = child
    if replay_steps is not None and step_count < mutation_depth:
        raise TemporalDiscoveryContractError("pair multi-operation replay is truncated before a terminal disposition")
    if _pair_genome_semantic_sha256(current) == _pair_genome_semantic_sha256(root):
        payload = {"schemaVersion": PAIR_PROPOSAL_SCHEMA, "proposalSeed": proposal_seed, "originKind": "structural_offspring", "side": steps[-1]["side"], "parentPair": root.canonical_payload(), "parentPairIdentitySha256": root.identity_sha256, "mutationDepth": mutation_depth, "mutationSteps": steps, "disposition": "no_op_proposal", **({"parentSelection": _clone(parent_selection)} if parent_selection is not None else {})}
        payload["proposalSha256"] = canonical_sha256(payload)
        return None, payload
    payload = {"schemaVersion": PAIR_PROPOSAL_SCHEMA, "proposalSeed": proposal_seed, "originKind": "structural_offspring", "side": steps[-1]["side"], "parentPair": root.canonical_payload(), "parentPairIdentitySha256": root.identity_sha256, "mutationDepth": mutation_depth, "mutationSteps": steps, "disposition": "materialized", "pair": current.canonical_payload(), "pairIdentitySha256": current.identity_sha256, **({"parentSelection": _clone(parent_selection)} if parent_selection is not None else {})}
    payload["proposalSha256"] = canonical_sha256(payload)
    return current, payload


def propose_same_side_crossover(*, proposal_seed: str, parent: FrozenPair, mate: FrozenPair, module_authority: PairModuleOperator, pair_compiler: CanonicalPairCompiler) -> tuple[FrozenPair, dict[str, Any]]:
    """Cross only matching long↔long or short↔short modules, deterministically."""
    seed, side = str(proposal_seed), proposal_side(proposal_seed)
    left = parent.long if side == "long" else parent.short
    right = mate.long if side == "long" else mate.short
    crossover: Any = module_authority
    if isinstance(module_authority, TypedGrammarPairOperator):
        class _BoundCrossover:
            def crossover(self, left_program: Mapping[str, Any], right_program: Mapping[str, Any], *, direction: str, proposal_seed: str) -> Mapping[str, Any]:
                if direction != left.direction:
                    raise TemporalDiscoveryContractError("typed crossover parent material drifted")
                return module_authority.crossover_for_programs(left, left_program, right_program)
        crossover = _BoundCrossover()
    program, record = deterministic_same_side_crossover(left, right, proposal_seed=seed, crossover=crossover)
    changed = module_authority.compile_program(left, program, candidate_id="qd_pair_cross_" + canonical_sha256({"seed": seed, "side": side})[7:31])
    opposite = parent.short if side == "long" else parent.long
    lineage = [*_clone(parent.canonical_payload())["sideTargetedLineage"], {**record, "side": side}]
    pair = _compile_pair(changed if side == "long" else opposite, opposite if side == "long" else changed, parent=parent, pair_compiler=pair_compiler, candidate_id="qd_pair_cross_" + canonical_sha256({"seed": seed, "parent": parent.identity_sha256, "mate": mate.identity_sha256})[7:31], lineage=lineage)
    audit = {"schemaVersion": "temporal_qd_pair_crossover_audit_v1", "side": side, "sameSide": True, "operation": record, "pairIdentitySha256": pair.identity_sha256}
    audit["auditSha256"] = canonical_sha256(audit)
    return pair, audit


def _propose_crossover(
    *,
    proposal_seed: str,
    parent: FrozenPair,
    mate: FrozenPair,
    module_authority: PairModuleOperator,
    pair_compiler: CanonicalPairCompiler,
    parent_selection: Mapping[str, Any] | None,
    mate_selection: Mapping[str, Any] | None,
    mate_selection_attempts: Sequence[Mapping[str, Any] | None],
) -> tuple[FrozenPair | None, dict[str, Any]]:
    """Build every crossover disposition from the same replayable operator kind."""

    base = {
        "schemaVersion": PAIR_PROPOSAL_SCHEMA,
        "proposalKind": CROSSOVER_PROPOSAL_KIND,
        "proposalSeed": proposal_seed,
        "originKind": "structural_offspring",
        "side": proposal_side(proposal_seed),
        "parentPair": parent.canonical_payload(),
        "parentPairIdentitySha256": parent.identity_sha256,
        "matePair": mate.canonical_payload(),
        "matePairIdentitySha256": mate.identity_sha256,
        "parentSelection": _clone(parent_selection) if parent_selection is not None else None,
        "mateSelection": _clone(mate_selection) if mate_selection is not None else None,
        "mateSelectionAttempts": [_clone(item) if item is not None else None for item in mate_selection_attempts],
    }
    try:
        pair, audit = propose_same_side_crossover(
            proposal_seed=proposal_seed,
            parent=parent,
            mate=mate,
            module_authority=module_authority,
            pair_compiler=pair_compiler,
        )
    except (BidirectionalGenomeError, TemporalDiscoveryContractError) as exc:
        payload = {
            **base,
            "disposition": "operation_rejected",
            "rejection": {
                "schemaVersion": "temporal_qd_pair_rejection_audit_v1",
                "reasonCode": "crossover_rejected",
                "exceptionType": type(exc).__name__,
            },
        }
        payload["proposalSha256"] = canonical_sha256(payload)
        return None, payload

    disposition = (
        "no_op_proposal"
        if _pair_genome_semantic_sha256(pair) == _pair_genome_semantic_sha256(parent)
        else "materialized"
    )
    payload = {
        **base,
        "side": audit["side"],
        "disposition": disposition,
        "crossoverAudit": audit,
        **(
            {"pair": pair.canonical_payload(), "pairIdentitySha256": pair.identity_sha256}
            if disposition == "materialized"
            else {}
        ),
    }
    payload["proposalSha256"] = canonical_sha256(payload)
    return (pair if disposition == "materialized" else None), payload


def replay_pair_proposal(*, payload: Mapping[str, Any], module_authority: PairModuleOperator, native_validator: NativeModuleValidator, pair_compiler: CanonicalPairCompiler) -> FrozenPair | None:
    """Replay only persisted material; factories/catalog aliases are never read."""
    data = _clone(payload)
    supplied = data.pop("proposalSha256", None)
    if supplied != canonical_sha256(data) or data.get("schemaVersion") != PAIR_PROPOSAL_SCHEMA:
        raise TemporalDiscoveryContractError("pair proposal journal identity mismatch")
    if data.get("originKind") == "random_immigrant":
        pair = FrozenPair.from_payload(data["factoryPair"])
        if pair.identity_sha256 != data.get("pairIdentitySha256"):
            raise TemporalDiscoveryContractError("pair immigrant journal identity mismatch")
        return pair
    if "mutationSteps" in data:
        parent = FrozenPair.from_payload(data["parentPair"])
        pair, replayed = _propose_pair_sequence(
            proposal_seed=data["proposalSeed"], parent=parent,
            mutation_depth=int(data["mutationDepth"]), module_authority=module_authority,
            native_validator=native_validator, pair_compiler=pair_compiler,
            parent_selection=data.get("parentSelection"), replay_steps=data["mutationSteps"],
        )
        if replayed != {**data, "proposalSha256": supplied}:
            raise TemporalDiscoveryContractError("pair multi-operation proposal replay diverged")
        return pair
    if data.get("proposalKind") == CROSSOVER_PROPOSAL_KIND:
        parent = FrozenPair.from_payload(data["parentPair"])
        mate = FrozenPair.from_payload(data["matePair"])
        pair, replayed = _propose_crossover(
            proposal_seed=data["proposalSeed"],
            parent=parent,
            mate=mate,
            module_authority=module_authority,
            pair_compiler=pair_compiler,
            parent_selection=data.get("parentSelection"),
            mate_selection=data.get("mateSelection"),
            mate_selection_attempts=data.get("mateSelectionAttempts") or [],
        )
        if replayed != {**data, "proposalSha256": supplied}:
            raise TemporalDiscoveryContractError("pair crossover proposal replay diverged")
        return pair
    if data.get("originKind") == "structural_offspring" and "crossoverAudit" in data:
        if data.get("disposition") != "materialized":
            return None
        parent = FrozenPair.from_payload(data["parentPair"])
        mate = FrozenPair.from_payload(data["matePair"])
        pair, audit = propose_same_side_crossover(proposal_seed=data["proposalSeed"], parent=parent, mate=mate, module_authority=module_authority, pair_compiler=pair_compiler)
        if audit != data.get("crossoverAudit") or pair.canonical_payload() != data.get("pair") or pair.identity_sha256 != data.get("pairIdentitySha256"):
            raise TemporalDiscoveryContractError("pair crossover replay diverged")
        return pair
    parent = FrozenPair.from_payload(data["parentPair"])
    if data.get("disposition") == "no_eligible_side_operation":
        return None
    pair, replayed = propose_pair(proposal_seed=data["proposalSeed"], parent=parent, pair_factory=None, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler, replay_operation=data.get("operation"))
    if replayed != {**data, "proposalSha256": supplied}:
        raise TemporalDiscoveryContractError("pair proposal replay diverged")
    return pair


def materialize_pair_candidate(*, pair: FrozenPair, proposal: Mapping[str, Any], pair_policy: Mapping[str, Any], generation_index: int, birth_ordinal: int, proposal_ordinal: int) -> dict[str, Any]:
    """Attach an immutable materialized proposal to one economic QD candidate."""
    from .temporal_qd_evolution import materialize_bidirectional_qd_candidate

    record = _clone(proposal)
    supplied = record.get("proposalSha256")
    check = dict(record); check.pop("proposalSha256", None)
    if supplied != canonical_sha256(check) or record.get("disposition") != "materialized":
        raise TemporalDiscoveryContractError("only an exact materialized pair proposal can enter QD")
    candidate = materialize_bidirectional_qd_candidate(pair=pair, pair_policy=pair_policy, origin_kind=str(record["originKind"]), generation_index=generation_index, birth_ordinal=birth_ordinal, proposal_ordinal=proposal_ordinal)
    candidate["pairProposal"] = record
    candidate["pairProposalSha256"] = supplied
    candidate["candidateIdentityMaterial"] = {
        **candidate["candidateIdentityMaterial"],
        "materializedPairProposalSha256": supplied,
    }
    candidate["candidateIdentitySha256"] = canonical_sha256(candidate["candidateIdentityMaterial"])
    candidate["candidateId"] = "qd_" + candidate["candidateIdentitySha256"][7:35]
    candidate["lineage"]["candidateId"] = candidate["candidateId"]
    candidate["lineage"]["candidateIdentitySha256"] = candidate["candidateIdentitySha256"]
    return candidate


def _write_once(path: Path, value: Mapping[str, Any]) -> int:
    artifact_kind = (
        "proposal_journal_entry"
        if path.parent.name == "proposal-journal"
        else path.name
    )
    with timed_span(
        "artifact.serialize_canonical_json", artifactKind=artifact_kind
    ) as span:
        encoded = canonical_json(value) + "\n"
        encoded_bytes = len(encoded.encode("utf-8"))
        span.annotate(encodedBytes=encoded_bytes)
    with timed_span("artifact.ensure_parent", artifactKind=artifact_kind):
        path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with timed_span(
            "artifact.verify_existing", artifactKind=artifact_kind
        ) as span:
            existing = path.read_text(encoding="utf-8")
            span.annotate(encodedBytes=encoded_bytes)
            if existing != encoded:
                raise TemporalDiscoveryContractError(
                    f"refusing to overwrite divergent pair-generation artifact: {path}"
                )
        return encoded_bytes
    with timed_span("artifact.write_new", artifactKind=artifact_kind) as span:
        path.write_text(encoded, encoding="utf-8")
        span.annotate(encodedBytes=encoded_bytes)
    return encoded_bytes


def _counter_increment(target: dict[str, int], value: Any) -> None:
    key = canonical_json(value) if isinstance(value, (Mapping, list, tuple)) else str(value)
    target[key] = target.get(key, 0) + 1


def _rich_immigrant_distribution(
    entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Summarize actual constructor breadth from persisted proposal audits."""

    def empty_side() -> dict[str, Any]:
        return {
            "moduleCount": 0,
            "seedNameCounts": {},
            "evidenceGroupCounts": {},
            "eventBindingCounts": {},
            "holdKindCounts": {},
            "plannedGrammarDepthCounts": {},
            "appliedGrammarDepthCounts": {},
            "grammarOperationFamilyCounts": {},
            "plannedIndicatorDepthCounts": {},
            "appliedIndicatorDepthCounts": {},
            "indicatorOperatorCounts": {},
            "indicatorConstructionKindCounts": {},
            "indicatorCountCounts": {},
            "evidenceGroupMemberShapeCounts": {},
        }

    def summarize(selected: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        sides = {"long": empty_side(), "short": empty_side()}
        proposal_count = 0
        for entry in selected:
            proposal = entry.get("proposal")
            audit = proposal.get("factoryConstructionAudit") if isinstance(proposal, Mapping) else None
            modules = audit.get("sides") if isinstance(audit, Mapping) else None
            if not isinstance(modules, Mapping):
                continue
            proposal_count += 1
            for direction in ("long", "short"):
                module = modules.get(direction)
                if not isinstance(module, Mapping):
                    continue
                side = sides[direction]
                side["moduleCount"] += 1
                selector = module.get("selector") if isinstance(module.get("selector"), Mapping) else {}
                _counter_increment(side["seedNameCounts"], selector.get("seedName"))
                _counter_increment(side["evidenceGroupCounts"], selector.get("groupId"))
                _counter_increment(side["eventBindingCounts"], selector.get("eventId"))
                grammar = module.get("grammar") if isinstance(module.get("grammar"), Mapping) else {}
                indicator = module.get("indicator") if isinstance(module.get("indicator"), Mapping) else {}
                shape = module.get("profileShape") if isinstance(module.get("profileShape"), Mapping) else {}
                _counter_increment(side["holdKindCounts"], shape.get("holdKind"))
                _counter_increment(side["plannedGrammarDepthCounts"], grammar.get("plannedDepth"))
                _counter_increment(side["appliedGrammarDepthCounts"], grammar.get("appliedDepth"))
                for step in grammar.get("steps") or []:
                    if isinstance(step, Mapping):
                        _counter_increment(side["grammarOperationFamilyCounts"], step.get("operationFamily"))
                _counter_increment(side["plannedIndicatorDepthCounts"], indicator.get("plannedDepth"))
                _counter_increment(side["appliedIndicatorDepthCounts"], indicator.get("appliedDepth"))
                for step in indicator.get("steps") or []:
                    if isinstance(step, Mapping):
                        _counter_increment(side["indicatorOperatorCounts"], step.get("operatorId"))
                        _counter_increment(side["indicatorConstructionKindCounts"], step.get("constructionKind"))
                _counter_increment(side["indicatorCountCounts"], shape.get("indicatorCount"))
                _counter_increment(side["evidenceGroupMemberShapeCounts"], shape.get("evidenceGroupMemberCounts") or [])
        for side in sides.values():
            for key, value in list(side.items()):
                if isinstance(value, dict):
                    side[key] = dict(sorted(value.items()))
        return {"proposalCount": proposal_count, "sides": sides}

    rich_entries = [
        entry
        for entry in entries
        if isinstance(entry.get("proposal"), Mapping)
        and isinstance(entry["proposal"].get("factoryConstructionAudit"), Mapping)
    ]
    if not rich_entries:
        return None
    result = {
        "schemaVersion": "temporal_qd_rich_immigrant_distribution_v1",
        "attempted": summarize(rich_entries),
        "accepted": summarize(
            [entry for entry in rich_entries if entry.get("disposition") == "accepted"]
        ),
    }
    result["distributionSha256"] = canonical_sha256(result)
    return result


def _explicit_parent_draw_count(entries: Sequence[Mapping[str, Any]]) -> int:
    """Rebuild the selection cursor for the non-archive parent ring.

    The ring has no archive-cell audit.  Its persisted proposal shape is
    therefore closed: a side-local mutation consumes one draw and a
    same-side crossover consumes its parent, mate, and any recorded mate
    retries.  Reject an archive-shaped or incomplete entry rather than
    silently deriving a different restart cursor.
    """

    draws = 0
    for entry in entries:
        if entry.get("originKind") != "structural_offspring":
            continue
        proposal = entry.get("proposal")
        if not isinstance(proposal, Mapping):
            raise TemporalDiscoveryContractError(
                "explicit-parent structural proposal is not an object"
            )
        selection_keys = {
            "parentSelection",
            "mateSelection",
            "mateSelectionAttempts",
        }
        if proposal.get("proposalKind") != CROSSOVER_PROPOSAL_KIND:
            if selection_keys.intersection(proposal):
                raise TemporalDiscoveryContractError(
                    "explicit-parent mutation proposal has unexpected selection audit"
                )
            draws += 1
            continue
        if not selection_keys.issubset(proposal):
            raise TemporalDiscoveryContractError(
                "explicit-parent crossover proposal lacks closed selection fields"
            )
        if proposal["parentSelection"] is not None or proposal["mateSelection"] is not None:
            raise TemporalDiscoveryContractError(
                "explicit-parent crossover proposal contains archive selection audit"
            )
        retries = proposal["mateSelectionAttempts"]
        if not isinstance(retries, list) or any(item is not None for item in retries):
            raise TemporalDiscoveryContractError(
                "explicit-parent crossover mate retries are not closed null records"
            )
        draws += 2 + len(retries)
    return draws


def _pair_semantic_ledger_records(ledger: dict[str, Any]) -> list[dict[str, str]]:
    """Return the persisted pair-semantic extension of the QD identity ledger.

    The base QD ledger deliberately knows only generic candidate/program/evidence
    identities.  A pair candidate's provenance can change its compiled wrapper
    while leaving its two executable module profiles unchanged, so pair mode
    additionally records that executable semantic identity in the *same*
    campaign ledger.  Keeping this as an additive, versioned record lets old
    empty/general ledgers remain readable while making pair runs fail closed on
    malformed historical material.
    """

    raw = ledger.setdefault("pairExecutableSemantics", [])
    duplicate_counter = ledger.setdefault(
        "pairExecutableSemanticDuplicateRejections", 0
    )
    if not isinstance(duplicate_counter, int) or duplicate_counter < 0:
        raise TemporalDiscoveryContractError(
            "QD identity ledger pair executable semantic duplicate counter is invalid"
        )
    if not isinstance(raw, list):
        raise TemporalDiscoveryContractError(
            "QD identity ledger pair executable semantics are not a list"
        )
    seen_semantics: dict[str, str] = {}
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != {
            "schemaVersion",
            "pairGenomeSemanticSha256",
            "candidateIdentitySha256",
        }:
            raise TemporalDiscoveryContractError(
                "QD identity ledger pair executable semantic record is malformed"
            )
        if item.get("schemaVersion") != PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA:
            raise TemporalDiscoveryContractError(
                "QD identity ledger pair executable semantic record has an unknown schema"
            )
        semantic = str(item.get("pairGenomeSemanticSha256") or "")
        candidate_identity = str(item.get("candidateIdentitySha256") or "")
        if not semantic.startswith("sha256:") or len(semantic) != 71 or not candidate_identity.startswith("sha256:") or len(candidate_identity) != 71:
            raise TemporalDiscoveryContractError(
                "QD identity ledger pair executable semantic identity is invalid"
            )
        prior = seen_semantics.get(semantic)
        if prior is not None and prior != candidate_identity:
            raise TemporalDiscoveryContractError(
                "QD identity ledger has duplicate executable pair semantics"
            )
        if prior is not None:
            raise TemporalDiscoveryContractError(
                "QD identity ledger repeats an executable pair semantic record"
            )
        seen_semantics[semantic] = candidate_identity
    # Return the actual persisted list, not a normalized copy: callers append
    # the next immutable semantic record before the ledger hash is checkpointed.
    return raw


def _pair_ledger_semantic_index(ledger: dict[str, Any]) -> dict[str, str]:
    return {
        record["pairGenomeSemanticSha256"]: record["candidateIdentitySha256"]
        for record in _pair_semantic_ledger_records(ledger)
    }


def _pair_ledger_accept_semantic(
    ledger: dict[str, Any],
    *,
    semantic_sha256: str,
    candidate_identity_sha256: str,
    semantic_index: dict[str, str],
) -> None:
    existing = semantic_index.get(semantic_sha256)
    if existing is not None:
        if existing != candidate_identity_sha256:
            raise TemporalDiscoveryContractError(
                "QD identity ledger has duplicate executable pair semantics"
            )
        return
    _pair_semantic_ledger_records(ledger).append(
        {
            "schemaVersion": PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA,
            "pairGenomeSemanticSha256": semantic_sha256,
            "candidateIdentitySha256": candidate_identity_sha256,
        }
    )
    semantic_index[semantic_sha256] = candidate_identity_sha256


def _pair_ledger_bootstrap_archive(
    ledger: dict[str, Any],
    archive: Mapping[str, Any] | None,
    *,
    evidence_identity_context: Mapping[str, Any],
) -> None:
    """Recover parent-archive pair semantics into the global identity ledger."""

    if archive is None:
        return
    from .temporal_qd_evolution import _ledger_accept, _ledger_identity_index, _ledger_record, qd_canonical_evidence_identity

    identity_index = _ledger_identity_index(ledger)
    semantic_index = _pair_ledger_semantic_index(ledger)
    for cell in archive.get("cells") or []:
        if not isinstance(cell, Mapping):
            raise TemporalDiscoveryContractError("pair parent archive cell is invalid")
        for member in cell.get("members") or []:
            candidate = member.get("candidate") if isinstance(member, Mapping) else None
            if not isinstance(candidate, Mapping):
                raise TemporalDiscoveryContractError(
                    "pair parent archive member lacks candidate"
                )
            restored = _clone(candidate)
            pair = FrozenPair.from_payload(restored.get("bidirectionalGenome"))
            restored["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(
                restored, evidence_identity_context
            )
            record = _ledger_record(restored)
            if record["candidateIdentitySha256"] not in identity_index["candidateIdentity"]:
                _ledger_accept(ledger, restored, identity_index=identity_index)
            _pair_ledger_accept_semantic(
                ledger,
                semantic_sha256=_pair_genome_semantic_sha256(pair),
                candidate_identity_sha256=record["candidateIdentitySha256"],
                semantic_index=semantic_index,
            )


def _pair_ledger_recover_accepted_entries(
    ledger: dict[str, Any],
    entries: Sequence[Mapping[str, Any]],
    *,
    evidence_identity_context: Mapping[str, Any],
) -> None:
    """Make an interrupted pair journal and its ledger mutually complete.

    A proposal is written before the ledger checkpoint can be flushed.  On
    restart, recover that exact accepted record.  Conversely, a semantic owned
    by a different candidate is a cross-generation contradiction and must not
    be reinterpreted as a local duplicate.
    """

    from .temporal_qd_evolution import _ledger_identity_index

    identity_index = _ledger_identity_index(ledger)
    semantic_index = _pair_ledger_semantic_index(ledger)
    for entry in entries:
        _pair_ledger_recover_accepted_entry(
            ledger,
            entry,
            evidence_identity_context=evidence_identity_context,
            identity_index=identity_index,
            semantic_index=semantic_index,
        )


def _pair_ledger_recover_accepted_entry(
    ledger: dict[str, Any],
    entry: Mapping[str, Any],
    *,
    evidence_identity_context: Mapping[str, Any],
    identity_index: dict[str, set[str]],
    semantic_index: dict[str, str],
) -> None:
    """Recover one accepted entry without retaining the full journal.

    The optimized generator invokes this as it streams immutable journal files
    during resume.  The legacy bulk helper above deliberately delegates to the
    same implementation so recovery checks remain byte-for-byte equivalent.
    """

    if entry.get("disposition") != "accepted":
        return
    from .temporal_qd_evolution import (
        _ledger_accept,
        _ledger_record,
        qd_canonical_evidence_identity,
    )

    candidate = entry.get("candidate")
    if not isinstance(candidate, Mapping):
        raise TemporalDiscoveryContractError("pair accepted proposal lacks candidate")
    restored = _clone(candidate)
    pair = FrozenPair.from_payload(restored.get("bidirectionalGenome"))
    canonical_evidence = qd_canonical_evidence_identity(
        restored, evidence_identity_context
    )
    supplied = restored.get("canonicalEvidenceIdentitySha256")
    if supplied is not None and supplied != canonical_evidence:
        raise TemporalDiscoveryContractError(
            "pair accepted proposal canonical evidence identity diverged"
        )
    restored["canonicalEvidenceIdentitySha256"] = canonical_evidence
    record = _ledger_record(restored)
    if record["candidateIdentitySha256"] not in identity_index["candidateIdentity"]:
        _ledger_accept(ledger, restored, identity_index=identity_index)
    semantic_sha = _pair_genome_semantic_sha256(pair)
    existing = semantic_index.get(semantic_sha)
    if existing is not None and existing != record["candidateIdentitySha256"]:
        raise TemporalDiscoveryContractError(
            "pair accepted proposal duplicates a global executable pair semantic"
        )
    _pair_ledger_accept_semantic(
        ledger,
        semantic_sha256=semantic_sha,
        candidate_identity_sha256=record["candidateIdentitySha256"],
        semantic_index=semantic_index,
    )


@dataclass(frozen=True)
class _AcceptedCandidateReference:
    """The only accepted-candidate state retained by the optimized loop.

    The full candidate remains durably available in its immutable proposal
    entry.  Keeping only a path plus identity fields prevents the generation
    process from retaining the same large pair/proposal material for every
    accepted candidate.
    """

    journal_path: Path
    proposal_ordinal: int
    candidate_id: str
    candidate_identity_sha256: str
    pair_genome_semantic_sha256: str


@dataclass
class _CompactPairGenerationState:
    proposal_count: int
    entry_sha256s: list[str]
    accepted: list[_AcceptedCandidateReference]
    seen_candidate_identities: set[str]
    seen_pair_genomes: set[str]
    disposition_counts: dict[str, int]
    origin_proposal_counts: dict[str, int]
    origin_accepted_counts: dict[str, int]
    archive_parent_selection_cell_ids: list[str]
    immigrant_attempts: int
    immigrant_accepted: int

    @classmethod
    def create(cls) -> "_CompactPairGenerationState":
        return cls(
            proposal_count=0,
            entry_sha256s=[],
            accepted=[],
            seen_candidate_identities=set(),
            seen_pair_genomes=set(),
            disposition_counts={},
            origin_proposal_counts={},
            origin_accepted_counts={},
            archive_parent_selection_cell_ids=[],
            immigrant_attempts=0,
            immigrant_accepted=0,
        )

    def observe_entry(self, entry: Mapping[str, Any], *, journal_path: Path) -> None:
        """Fold one verified journal entry into compact restart state."""

        origin = str(entry["originKind"])
        disposition = str(entry["disposition"])
        ordinal = int(entry["proposalOrdinal"])
        self.proposal_count += 1
        self.entry_sha256s.append(str(entry["entrySha256"]))
        self.disposition_counts[disposition] = (
            self.disposition_counts.get(disposition, 0) + 1
        )
        self.origin_proposal_counts[origin] = (
            self.origin_proposal_counts.get(origin, 0) + 1
        )
        if origin == "random_immigrant":
            self.immigrant_attempts += 1

        proposal = entry.get("proposal")
        if isinstance(proposal, Mapping):
            audits: list[Any] = [
                proposal.get("parentSelection"),
                proposal.get("mateSelection"),
            ]
            audits.extend(proposal.get("mateSelectionAttempts") or [])
            for audit in audits:
                if isinstance(audit, Mapping) and audit.get("parentCellId") is not None:
                    self.archive_parent_selection_cell_ids.append(
                        str(audit["parentCellId"])
                    )

        if disposition != "accepted":
            return
        candidate = entry.get("candidate")
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("pair accepted proposal lacks candidate")
        pair = FrozenPair.from_payload(candidate.get("bidirectionalGenome"))
        pair_semantic_sha = _pair_genome_semantic_sha256(pair)
        if pair_semantic_sha in self.seen_pair_genomes:
            raise TemporalDiscoveryContractError(
                "pair generation journal has duplicate executable pair semantics"
            )
        candidate_identity = str(candidate["candidateIdentitySha256"])
        candidate_id = str(candidate.get("candidateId") or "")
        self.accepted.append(
            _AcceptedCandidateReference(
                journal_path=journal_path,
                proposal_ordinal=ordinal,
                candidate_id=candidate_id,
                candidate_identity_sha256=candidate_identity,
                pair_genome_semantic_sha256=pair_semantic_sha,
            )
        )
        self.seen_candidate_identities.add(candidate_identity)
        self.seen_pair_genomes.add(pair_semantic_sha)
        self.origin_accepted_counts[origin] = (
            self.origin_accepted_counts.get(origin, 0) + 1
        )
        if origin == "random_immigrant":
            self.immigrant_accepted += 1


def _load_pair_proposal_entry(path: Path, *, ordinal: int) -> dict[str, Any]:
    """Read one canonical entry without cloning or retaining its payload."""

    if path.name != f"{ordinal:08d}.json":
        raise TemporalDiscoveryContractError("pair proposal journal has a gap")
    try:
        row = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read pair proposal journal entry: {path}"
        ) from exc
    if not isinstance(row, dict):
        raise TemporalDiscoveryContractError("pair proposal entry must be an object")
    if row.get("entrySha256") != canonical_sha256(
        {key: value for key, value in row.items() if key != "entrySha256"}
    ):
        raise TemporalDiscoveryContractError("pair proposal entry identity mismatch")
    return row


def _candidate_from_reference(reference: _AcceptedCandidateReference) -> dict[str, Any]:
    entry = _load_pair_proposal_entry(
        reference.journal_path, ordinal=reference.proposal_ordinal
    )
    if entry.get("disposition") != "accepted":
        raise TemporalDiscoveryContractError(
            "accepted candidate reference points to a non-accepted proposal"
        )
    candidate = entry.get("candidate")
    if not isinstance(candidate, dict):
        raise TemporalDiscoveryContractError("pair accepted proposal lacks candidate")
    if (
        candidate.get("candidateId") != reference.candidate_id
        or candidate.get("candidateIdentitySha256")
        != reference.candidate_identity_sha256
    ):
        raise TemporalDiscoveryContractError(
            "accepted candidate reference identity diverged from its journal entry"
        )
    return candidate


@dataclass(frozen=True)
class _JournalCandidateSequence:
    """Replay accepted candidates from their journal entries on demand."""

    references: tuple[_AcceptedCandidateReference, ...]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for reference in self.references:
            yield _candidate_from_reference(reference)


_CANONICAL_JSON_ENCODER = json.JSONEncoder(
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=True,
    allow_nan=False,
)


def _iter_canonical_json_chunks(value: Any) -> Iterator[str]:
    """Stream the exact ``canonical_json`` grammar without a whole copy.

    ``json.dumps(..., sort_keys=True, separators=(",", ":"))`` is the
    persisted population contract.  The only lazy value here is the candidate
    sequence; normal maps/lists are emitted with that same sorted recursive
    grammar, so the resulting bytes and SHA-256 remain identity-compatible.
    """

    if isinstance(value, _JournalCandidateSequence):
        yield "["
        for index, item in enumerate(value):
            if index:
                yield ","
            yield from _iter_canonical_json_chunks(item)
        yield "]"
        return
    if isinstance(value, Mapping):
        yield "{"
        for index, key in enumerate(sorted(value)):
            if index:
                yield ","
            yield _CANONICAL_JSON_ENCODER.encode(key)
            yield ":"
            yield from _iter_canonical_json_chunks(value[key])
        yield "}"
        return
    if isinstance(value, (list, tuple)):
        yield "["
        for index, item in enumerate(value):
            if index:
                yield ","
            yield from _iter_canonical_json_chunks(item)
        yield "]"
        return
    yield _CANONICAL_JSON_ENCODER.encode(value)


def _stream_canonical_sha256(value: Any) -> str:
    digest = hashlib.sha256()
    for chunk in _iter_canonical_json_chunks(value):
        digest.update(chunk.encode("utf-8"))
    return "sha256:" + digest.hexdigest()


def _iter_canonical_file_chunks(value: Any) -> Iterator[bytes]:
    for chunk in _iter_canonical_json_chunks(value):
        yield chunk.encode("utf-8")
    # _write_once historically uses Path.write_text without a newline override,
    # so preserve that platform-native final newline byte contract exactly.
    yield os.linesep.encode("ascii")


def _stream_matches_existing(path: Path, value: Any) -> tuple[bool, int]:
    total = 0
    with path.open("rb") as existing:
        for chunk in _iter_canonical_file_chunks(value):
            total += len(chunk)
            if existing.read(len(chunk)) != chunk:
                return False, total
        return existing.read(1) == b"", total


def _write_canonical_stream_once(path: Path, value: Any) -> int:
    """Atomically write a canonical JSON document without a giant string."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        matches, total = _stream_matches_existing(path, value)
        if not matches:
            raise TemporalDiscoveryContractError(
                f"refusing to overwrite divergent pair-generation artifact: {path}"
            )
        return total
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    total = 0
    try:
        with temporary.open("xb") as handle:
            for chunk in _iter_canonical_file_chunks(value):
                handle.write(chunk)
                total += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            matches, _ = _stream_matches_existing(path, value)
            if not matches:
                raise TemporalDiscoveryContractError(
                    f"refusing to overwrite divergent pair-generation artifact: {path}"
                )
            temporary.unlink(missing_ok=True)
            return total
        os.replace(temporary, path)
        return total
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _materialize_pair_candidate_optimized(
    *,
    pair: FrozenPair,
    proposal: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    generation_index: int,
    birth_ordinal: int,
    proposal_ordinal: int,
) -> dict[str, Any]:
    """Build the legacy candidate bytes while sharing owned pair/proposal data.

    This is intentionally private to the optimized generation path.  It sees a
    freshly constructed immutable ``FrozenPair`` and its just-built proposal,
    so it can avoid the legacy boundary's defensive thaw/clone/reparse cycle.
    The durable entry still contains the exact same independent JSON values.
    """

    from .temporal_qd_evolution import (
        QD_VERSION,
        _bidirectional_pair_policy,
        _construction_evidence_scope,
    )

    supplied = proposal.get("proposalSha256")
    material = dict(proposal)
    material.pop("proposalSha256", None)
    if supplied != canonical_sha256(material) or proposal.get("disposition") != "materialized":
        raise TemporalDiscoveryContractError(
            "only an exact materialized pair proposal can enter QD"
        )
    pair_payload = proposal.get("pair") or proposal.get("factoryPair")
    if not isinstance(pair_payload, dict):
        raise TemporalDiscoveryContractError("materialized pair proposal lacks frozen pair")
    pair_identity = ((pair_payload.get("identities") or {}).get("pairIdentitySha256"))
    if pair_identity != pair.identity_sha256:
        raise TemporalDiscoveryContractError(
            "materialized pair proposal identity diverged from its frozen pair"
        )
    policy = _bidirectional_pair_policy({"bidirectionalPairPolicy": pair_policy})
    assert policy is not None
    if pair.pair_compiler.canonical_payload() != policy["compilerAuthority"]:
        raise TemporalDiscoveryContractError(
            "bidirectional pair compiler authority does not match policy"
        )
    origin_kind = str(proposal["originKind"])
    if origin_kind not in {"random_immigrant", "structural_offspring"}:
        raise TemporalDiscoveryContractError("bidirectional QD origin kind is unknown")
    lineage = pair_payload.get("sideTargetedLineage")
    profile = pair_payload.get("profile")
    if not isinstance(lineage, list) or not isinstance(profile, dict):
        raise TemporalDiscoveryContractError("materialized pair proposal payload is malformed")
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
        "materializedPairProposalSha256": supplied,
    }
    candidate_identity = canonical_sha256(identity_material)
    candidate_id = "qd_" + candidate_identity[7:35]
    return {
        "candidateId": candidate_id,
        "sourceMode": "qd_" + origin_kind + "_bidirectional_pair",
        "seedId": "bidirectional_pair",
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "sourceProfile": profile,
        "sourceProfileSha256": pair.raw_pair_sha256,
        "profileSnapshotSha256": pair.profile_sha256,
        "programSha256": pair.native_program_sha256,
        "validationReportSha256": pair.native_validation_report_sha256,
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": candidate_identity,
        "structuralDepth": len(lineage),
        "structuralOperatorHistory": lineage,
        "mutationTrace": [],
        "activationAwareRepairs": [],
        "constructionEvidenceScope": _construction_evidence_scope([]),
        "bidirectionalGenome": pair_payload,
        "lineage": {
            "schemaVersion": "temporal_qd_bidirectional_candidate_lineage_v1",
            "candidateId": candidate_id,
            "candidateIdentitySha256": candidate_identity,
            "pairIdentitySha256": pair.identity_sha256,
            "orderedSideLineage": lineage,
        },
        "pairProposal": proposal,
        "pairProposalSha256": supplied,
    }


def _optimized_funnel_audit(
    *, pair: FrozenPair, candidate: Mapping[str, Any]
) -> dict[str, Any]:
    """Emit the legacy funnel payload without reparsing a large pair copy."""

    return {
        "schemaVersion": "temporal_qd_proposal_funnel_stage_v1",
        "candidateId": candidate["candidateId"],
        "rawSourceProfileSha256": pair.raw_pair_sha256,
        "staticReachability": {"outcome": "reachable", "reasons": []},
        "nativeValidation": {
            "outcome": "valid",
            "reasons": [],
            "resolvedProfileSha256": pair.profile_sha256,
            "programSha256": pair.native_program_sha256,
            "validationReportSha256": pair.native_validation_report_sha256,
        },
        "admission": {
            "outcome": "admitted",
            "reasons": [],
            "canonicalEvidenceIdentitySha256": candidate.get(
                "canonicalEvidenceIdentitySha256"
            ),
        },
    }


def _rich_immigrant_distribution_from_journal(
    journal_paths: Sequence[Path],
) -> dict[str, Any] | None:
    """Stream the legacy immigrant-distribution reduction from journal files."""

    def empty_side() -> dict[str, Any]:
        return {
            "moduleCount": 0,
            "seedNameCounts": {},
            "evidenceGroupCounts": {},
            "eventBindingCounts": {},
            "holdKindCounts": {},
            "plannedGrammarDepthCounts": {},
            "appliedGrammarDepthCounts": {},
            "grammarOperationFamilyCounts": {},
            "plannedIndicatorDepthCounts": {},
            "appliedIndicatorDepthCounts": {},
            "indicatorOperatorCounts": {},
            "indicatorConstructionKindCounts": {},
            "indicatorCountCounts": {},
            "evidenceGroupMemberShapeCounts": {},
        }

    def empty_distribution() -> dict[str, Any]:
        return {"proposalCount": 0, "sides": {"long": empty_side(), "short": empty_side()}}

    def add(target: dict[str, Any], entry: Mapping[str, Any]) -> bool:
        proposal = entry.get("proposal")
        audit = (
            proposal.get("factoryConstructionAudit")
            if isinstance(proposal, Mapping)
            else None
        )
        modules = audit.get("sides") if isinstance(audit, Mapping) else None
        if not isinstance(modules, Mapping):
            return False
        target["proposalCount"] += 1
        for direction in ("long", "short"):
            module = modules.get(direction)
            if not isinstance(module, Mapping):
                continue
            side = target["sides"][direction]
            side["moduleCount"] += 1
            selector = module.get("selector") if isinstance(module.get("selector"), Mapping) else {}
            _counter_increment(side["seedNameCounts"], selector.get("seedName"))
            _counter_increment(side["evidenceGroupCounts"], selector.get("groupId"))
            _counter_increment(side["eventBindingCounts"], selector.get("eventId"))
            grammar = module.get("grammar") if isinstance(module.get("grammar"), Mapping) else {}
            indicator = module.get("indicator") if isinstance(module.get("indicator"), Mapping) else {}
            shape = module.get("profileShape") if isinstance(module.get("profileShape"), Mapping) else {}
            _counter_increment(side["holdKindCounts"], shape.get("holdKind"))
            _counter_increment(side["plannedGrammarDepthCounts"], grammar.get("plannedDepth"))
            _counter_increment(side["appliedGrammarDepthCounts"], grammar.get("appliedDepth"))
            for step in grammar.get("steps") or []:
                if isinstance(step, Mapping):
                    _counter_increment(
                        side["grammarOperationFamilyCounts"],
                        step.get("operationFamily"),
                    )
            _counter_increment(side["plannedIndicatorDepthCounts"], indicator.get("plannedDepth"))
            _counter_increment(side["appliedIndicatorDepthCounts"], indicator.get("appliedDepth"))
            for step in indicator.get("steps") or []:
                if isinstance(step, Mapping):
                    _counter_increment(side["indicatorOperatorCounts"], step.get("operatorId"))
                    _counter_increment(
                        side["indicatorConstructionKindCounts"],
                        step.get("constructionKind"),
                    )
            _counter_increment(side["indicatorCountCounts"], shape.get("indicatorCount"))
            _counter_increment(
                side["evidenceGroupMemberShapeCounts"],
                shape.get("evidenceGroupMemberCounts") or [],
            )
        return True

    attempted = empty_distribution()
    accepted = empty_distribution()
    for ordinal, path in enumerate(journal_paths):
        entry = _load_pair_proposal_entry(path, ordinal=ordinal)
        if add(attempted, entry) and entry.get("disposition") == "accepted":
            add(accepted, entry)
    if not attempted["proposalCount"]:
        return None
    for distribution in (attempted, accepted):
        for side in distribution["sides"].values():
            for key, value in list(side.items()):
                if isinstance(value, dict):
                    side[key] = dict(sorted(value.items()))
    result = {
        "schemaVersion": "temporal_qd_rich_immigrant_distribution_v1",
        "attempted": attempted,
        "accepted": accepted,
    }
    result["distributionSha256"] = canonical_sha256(result)
    return result


def _generate_pair_population_legacy_impl(
    *,
    output_root: Path | str,
    generation_index: int,
    target_unique_candidates: int,
    run_config: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    parent_pairs: Sequence[FrozenPair] = (),
    parent_archive: Mapping[str, Any] | None = None,
    pair_factory: TypedPairFactory | None = None,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    evidence_identity_context: Mapping[str, Any] | None = None,
    operator_implementation_identity: Mapping[str, Any] | None = None,
    identity_ledger_path: Path | str | None = None,
    max_proposal_attempts: int = DEFAULT_MAX_PROPOSAL_ATTEMPTS,
    max_new_proposals: int | None = None,
) -> dict[str, Any]:
    """Small operational pair-QD journal/population path with exact restart.

    It is intentionally a separate versioned policy rather than a mutation of
    the historical QD generation format.  Population consumers use the normal
    QD loader/archive/campaign after its pair-material gate has admitted it.
    """
    with timed_span("generation.validate_arguments"):
        if generation_index < 0 or target_unique_candidates < 1:
            raise TemporalDiscoveryContractError("pair generation index/target is invalid")
        max_proposal_attempts = int(max_proposal_attempts)
        if max_proposal_attempts < target_unique_candidates:
            raise TemporalDiscoveryContractError(
                "pair generation proposal ceiling is below its target"
            )
        if max_new_proposals is not None and int(max_new_proposals) < 0:
            raise TemporalDiscoveryContractError("pair generation new proposal limit is negative")
        if identity_ledger_path is not None and evidence_identity_context is None:
            raise TemporalDiscoveryContractError(
                "pair generation global identity ledger requires a predeclared evidence context"
            )
        if operator_implementation_identity is None:
            raise TemporalDiscoveryContractError("pair generation requires a frozen operator implementation identity")
    with timed_span("generation.resolve_runtime_inputs"):
        root = Path(output_root)
        policy = _clone(pair_policy)
        factory_construction_policy = (
            getattr(pair_factory, "construction_policy", None)
            if pair_factory is not None
            else None
        )
    if factory_construction_policy is not None:
        if not isinstance(factory_construction_policy, Mapping):
            raise TemporalDiscoveryContractError(
                "pair immigrant factory construction policy must be an object"
            )
        factory_construction_policy = _clone(factory_construction_policy)
        tripwire = factory_construction_policy.get("collisionTripwire")
        if (
            not isinstance(tripwire, Mapping)
            or isinstance(tripwire.get("minimumImmigrantAttempts"), bool)
            or int(tripwire.get("minimumImmigrantAttempts") or 0) < 1
            or isinstance(tripwire.get("minimumAcceptedRatio"), bool)
            or not 0.0 < float(tripwire.get("minimumAcceptedRatio") or 0.0) <= 1.0
        ):
            raise TemporalDiscoveryContractError(
                "pair immigrant collision tripwire policy is invalid"
            )
    with timed_span("generation.build_config"):
        config = {
            "schemaVersion": PAIR_GENERATION_SCHEMA,
            "generationIndex": generation_index,
            "targetUniqueCandidates": target_unique_candidates,
            "maxProposalAttempts": max_proposal_attempts,
            "runConfig": _clone(run_config),
            "pairPolicy": policy,
            "operatorImplementation": _clone(operator_implementation_identity),
            "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
            **(
                {"immigrantConstructionPolicy": factory_construction_policy}
                if factory_construction_policy is not None
                else {}
            ),
            **(
                {
                    "globalIdentityLedger": {
                        "schemaVersion": "temporal_qd_identity_ledger_v3",
                        "locationPolicy": "caller_supplied_generation_global_ledger",
                    }
                }
                if identity_ledger_path is not None
                else {}
            ),
        }
        config["configSha256"] = canonical_sha256(config)
    with timed_span("generation.persist_config"):
        _write_once(root / "pair-config.json", config)
    with timed_span("generation.resume.scan_journal") as span:
        journal_paths = sorted((root / "proposal-journal").glob("*.json"))
        span.annotate(existingProposalCount=len(journal_paths))
    entries: list[dict[str, Any]] = []
    for ordinal, path in enumerate(journal_paths):
        with timing_scope(workClass="resume_replay", proposalOrdinal=ordinal):
            with timed_span("generation.resume.replay_entry"):
                if path.name != f"{ordinal:08d}.json":
                    raise TemporalDiscoveryContractError("pair proposal journal has a gap")
                row = _clone(__import__("json").loads(path.read_text(encoding="utf-8")))
                if row.get("entrySha256") != canonical_sha256({key: value for key, value in row.items() if key != "entrySha256"}):
                    raise TemporalDiscoveryContractError("pair proposal entry identity mismatch")
                proposal = row.get("proposal")
                if not isinstance(proposal, Mapping):
                    raise TemporalDiscoveryContractError("pair proposal entry lacks immutable proposal material")
                replayed = replay_pair_proposal(payload=proposal, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler)
                if row.get("disposition") == "accepted":
                    candidate = row.get("candidate")
                    if not isinstance(candidate, Mapping) or replayed is None or candidate.get("bidirectionalGenome") != replayed.canonical_payload():
                        raise TemporalDiscoveryContractError("pair accepted proposal resume material diverged")
                entries.append(row)
    with timed_span("generation.resume.restore_accepted") as span:
        accepted = [_clone(entry["candidate"]) for entry in entries if entry.get("disposition") == "accepted"]
        span.annotate(acceptedCount=len(accepted))
    # Pair identity intentionally includes lineage/provenance.  Unique QD slots
    # must additionally be protected against a pre-patch journal containing
    # distinct provenance wrappers for the exact same executable long/short
    # module profiles.
    with timed_span("generation.resume.rebuild_semantic_indexes") as span:
        semantic_pairs: dict[str, str] = {}
        for entry in entries:
            if entry.get("disposition") != "accepted":
                continue
            candidate = entry.get("candidate")
            if not isinstance(candidate, Mapping):
                raise TemporalDiscoveryContractError("pair accepted proposal lacks candidate")
            pair = FrozenPair.from_payload(candidate.get("bidirectionalGenome"))
            semantic_sha = canonical_sha256({"longModuleProfileSha256": pair.long.profile_sha256, "shortModuleProfileSha256": pair.short.profile_sha256})
            existing = semantic_pairs.get(semantic_sha)
            candidate_id = str(candidate.get("candidateId") or "")
            if existing is not None:
                raise TemporalDiscoveryContractError("pair generation journal has duplicate executable pair semantics")
            semantic_pairs[semantic_sha] = candidate_id
        seen = {str(row["candidateIdentitySha256"]) for row in accepted}
        seen_pair_genomes = {
            _pair_genome_semantic_sha256(FrozenPair.from_payload(row["bidirectionalGenome"]))
            for row in accepted
        }
        span.annotate(
            candidateIdentityCount=len(seen),
            executablePairSemanticCount=len(seen_pair_genomes),
        )
    ledger = None
    identity_index = None
    global_pair_semantics: dict[str, str] = {}
    if identity_ledger_path is not None:
        from .temporal_qd_evolution import (
            _ledger_bootstrap_archive,
            _ledger_identity_index,
            _load_identity_ledger,
            _save_identity_ledger,
            qd_canonical_evidence_identity,
        )

        ledger = _load_identity_ledger(Path(identity_ledger_path))
        # Retain the base ledger's archive recovery for all ordinary QD
        # identities, then add pair semantics absent from the generic schema.
        _ledger_bootstrap_archive(ledger, parent_archive or {"cells": []}, evidence_identity_context)
        _pair_ledger_bootstrap_archive(
            ledger,
            parent_archive,
            evidence_identity_context=evidence_identity_context,
        )
        _pair_ledger_recover_accepted_entries(
            ledger,
            entries,
            evidence_identity_context=evidence_identity_context,
        )
        ledger["proposalSlotCounters"]["proposalsObserved"] = max(
            int(ledger["proposalSlotCounters"].get("proposalsObserved") or 0),
            len(entries),
        )
        _save_identity_ledger(Path(identity_ledger_path), ledger)
        identity_index = _ledger_identity_index(ledger)
        global_pair_semantics = _pair_ledger_semantic_index(ledger)
    parents = sorted((FrozenPair.from_payload(item.canonical_payload()) for item in parent_pairs), key=lambda item: item.identity_sha256)
    archive_cells: list[dict[str, Any]] = []
    negative_cells: list[dict[str, Any]] = []
    selection_state: dict[str, dict[str, int]] = {}
    if parent_archive is not None:
        from .temporal_qd_evolution import _initial_selection_state, _negative_novelty_cells, _reproduction_cells
        archive_cells = _reproduction_cells(parent_archive, allow_empty_quality_bootstrap=True)
        negative_cells = _negative_novelty_cells(parent_archive)
        selection_state = _initial_selection_state(archive_cells + negative_cells)
        parents = []
        # Rebuild the mutable visit/offspring counters from immutable audits so
        # an interrupted generation makes exactly the same next draw.
        for entry in entries:
            proposal = entry.get("proposal")
            if not isinstance(proposal, Mapping):
                continue
            audits = [proposal.get("parentSelection"), proposal.get("mateSelection")]
            audits.extend(proposal.get("mateSelectionAttempts") or [])
            for audit in audits:
                if not isinstance(audit, Mapping) or audit.get("parentCellId") is None:
                    continue
                cell_id = str(audit["parentCellId"])
                if cell_id not in selection_state:
                    raise TemporalDiscoveryContractError("persisted pair parent selection references an unavailable archive cell")
                selection_state[cell_id]["selectionVisitCount"] += 1
                selection_state[cell_id]["offspringAttemptCount"] += 1
    immigrant_only_bootstrap = not archive_cells and not parents
    if archive_cells:
        structural_parent_selections = sum(
            1
            for entry in entries
            for audit in (
                [entry.get("proposal", {}).get("parentSelection"), entry.get("proposal", {}).get("mateSelection"), *(entry.get("proposal", {}).get("mateSelectionAttempts") or [])]
                if isinstance(entry.get("proposal"), Mapping) else []
            )
            if isinstance(audit, Mapping) and audit.get("parentCellId") is not None
        )
    else:
        structural_parent_selections = _explicit_parent_draw_count(entries)
    def select_parent(label: str) -> tuple[FrozenPair, dict[str, Any] | None]:
        nonlocal structural_parent_selections
        if archive_cells:
            from .temporal_qd_evolution import _select_parent
            rng = random.Random(int(canonical_sha256({"generationSeed": config["configSha256"], "selectionOrdinal": structural_parent_selections, "label": label})[7:23], 16))
            cell, member, mode, lane, reason = _select_parent(rng=rng, cells=archive_cells, negative_novelty_cells=negative_cells, selection_state=selection_state, negative_novelty_slot=(structural_parent_selections + 1) % 10 == 0)
            structural_parent_selections += 1
            return FrozenPair.from_payload(member["candidate"]["bidirectionalGenome"]), {"schemaVersion": "temporal_qd_pair_parent_selection_v1", "parentCellId": cell["cellId"], "parentCandidateId": member["candidateId"], "selectionMode": mode, "parentLane": lane, "parentLaneReason": reason, "paretoFront": member.get("paretoFront"), "crowdingDistance": member.get("crowdingDistance")}
        parent = parents[structural_parent_selections % len(parents)]
        structural_parent_selections += 1
        return parent, None
    made = 0
    while (
        len(accepted) < target_unique_candidates
        and len(entries) < max_proposal_attempts
        and (max_new_proposals is None or made < max_new_proposals)
    ):
        proposal_started = start_performance_interval()
        ordinal = len(entries)
        with timed_span("proposal.select_origin", proposalOrdinal=ordinal) as span:
            seed = canonical_sha256({"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": config["configSha256"], "proposalOrdinal": ordinal})
            use_immigrant = not (archive_cells or parents) or ordinal % 5 == 4
            use_crossover = (
                not use_immigrant
                and ordinal % 7 == 6
                and (len(archive_cells) > 1 or len(parents) > 1)
            )
            scheduled_origin = (
                "random_immigrant"
                if use_immigrant
                else "same_side_crossover"
                if use_crossover
                else "structural_offspring"
            )
            span.annotate(scheduledOrigin=scheduled_origin)
        with timing_scope(
            workClass="new_proposal",
            proposalOrdinal=ordinal,
            scheduledOrigin=scheduled_origin,
        ):
            with timed_span("proposal.construct"):
                if use_immigrant:
                    pair, proposal = propose_pair(proposal_seed=seed, parent=None, pair_factory=pair_factory, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler)
                elif use_crossover:
                    parent, parent_selection = select_parent("crossover_parent")
                    mate, mate_selection = select_parent("crossover_mate")
                    mate_attempts: list[dict[str, Any] | None] = []
                    eligible_count = sum(len(cell.get("members") or []) for cell in archive_cells) if archive_cells else len(parents)
                    while mate.identity_sha256 == parent.identity_sha256 and eligible_count > 1:
                        mate_attempts.append(mate_selection)
                        mate, mate_selection = select_parent(f"crossover_mate_retry_{len(mate_attempts)}")
                    pair, proposal = _propose_crossover(
                        proposal_seed=seed,
                        parent=parent,
                        mate=mate,
                        module_authority=module_authority,
                        pair_compiler=pair_compiler,
                        parent_selection=parent_selection,
                        mate_selection=mate_selection,
                        mate_selection_attempts=mate_attempts,
                    )
                else:
                    parent, selection = select_parent("mutation")
                    depth = _mutation_depth_for_seed(seed)
                    pair, proposal = _propose_pair_sequence(proposal_seed=seed, parent=parent, mutation_depth=depth, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler, parent_selection=selection)
        with timed_span(
            "proposal.build_entry",
            proposalOrdinal=ordinal,
            originKind=proposal["originKind"],
        ):
            entry: dict[str, Any] = {"schemaVersion": "temporal_qd_proposal_entry_v3", "configSha256": config["configSha256"], "generationIndex": generation_index, "proposalOrdinal": ordinal, "originKind": proposal["originKind"], "proposal": proposal, "operatorImplementationSha256": canonical_sha256(operator_implementation_identity)}
        if ledger is not None:
            ledger["proposalSlotCounters"]["proposalsObserved"] += 1
        if pair is None:
            entry["disposition"] = proposal["disposition"]
        else:
            with timed_span(
                "proposal.materialize_candidate",
                proposalOrdinal=ordinal,
                originKind=proposal["originKind"],
            ):
                candidate = materialize_pair_candidate(pair=pair, proposal=proposal, pair_policy=policy, generation_index=generation_index, birth_ordinal=len(accepted), proposal_ordinal=ordinal)
            if evidence_identity_context is not None:
                from .temporal_qd_evolution import qd_canonical_evidence_identity
                with timed_span(
                    "proposal.compute_evidence_identity",
                    proposalOrdinal=ordinal,
                ):
                    candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(candidate, evidence_identity_context)
            with timed_span(
                "proposal.compute_executable_semantic_identity",
                proposalOrdinal=ordinal,
            ):
                pair_genome_sha256 = _pair_genome_semantic_sha256(pair)
            with timed_span(
                "proposal.check_local_duplicates",
                proposalOrdinal=ordinal,
            ) as span:
                duplicate_pair = pair_genome_sha256 in seen_pair_genomes
                duplicate_candidate = candidate["candidateIdentitySha256"] in seen
                span.annotate(
                    duplicatePairGenome=duplicate_pair,
                    duplicateCandidateIdentity=duplicate_candidate,
                )
            if duplicate_pair:
                entry["disposition"] = "duplicate_pair_genome"
            elif duplicate_candidate:
                entry["disposition"] = "duplicate_candidate_identity"
            else:
                if ledger is not None:
                    from .temporal_qd_evolution import _ledger_accept, _ledger_duplicate_check

                    candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(
                        candidate, evidence_identity_context
                    )
                    duplicate_reason, identity_checks = _ledger_duplicate_check(
                        ledger, candidate, identity_index=identity_index
                    )
                    entry["identityChecks"] = identity_checks
                    if pair_genome_sha256 in global_pair_semantics:
                        entry["disposition"] = "duplicate_pair_genome_global"
                        ledger["pairExecutableSemanticDuplicateRejections"] += 1
                    elif duplicate_reason is not None:
                        entry["disposition"] = duplicate_reason
                    else:
                        entry["disposition"] = "accepted"
                        entry["candidate"] = candidate
                        accepted.append(candidate)
                        seen.add(candidate["candidateIdentitySha256"])
                        seen_pair_genomes.add(pair_genome_sha256)
                        _ledger_accept(ledger, candidate, identity_index=identity_index)
                        _pair_ledger_accept_semantic(
                            ledger,
                            semantic_sha256=pair_genome_sha256,
                            candidate_identity_sha256=str(candidate["candidateIdentitySha256"]),
                            semantic_index=global_pair_semantics,
                        )
                else:
                    with timed_span(
                        "proposal.accept_local_candidate",
                        proposalOrdinal=ordinal,
                    ):
                        entry["disposition"] = "accepted"; entry["candidate"] = candidate; accepted.append(candidate); seen.add(candidate["candidateIdentitySha256"]); seen_pair_genomes.add(pair_genome_sha256)
        if proposal.get("disposition") == "materialized" and entry["disposition"] == "accepted":
            with timed_span(
                "proposal.build_funnel_audit",
                proposalOrdinal=ordinal,
            ):
                pair_payload = proposal.get("pair") or proposal.get("factoryPair")
                if not isinstance(pair_payload, Mapping):
                    raise TemporalDiscoveryContractError("materialized pair proposal lacks frozen pair")
                frozen = FrozenPair.from_payload(pair_payload)
                funnel = {"schemaVersion": "temporal_qd_proposal_funnel_stage_v1", "candidateId": entry["candidate"]["candidateId"], "rawSourceProfileSha256": frozen.raw_pair_sha256, "staticReachability": {"outcome": "reachable", "reasons": []}, "nativeValidation": {"outcome": "valid", "reasons": [], "resolvedProfileSha256": frozen.profile_sha256, "programSha256": frozen.native_program_sha256, "validationReportSha256": frozen.native_validation_report_sha256}, "admission": {"outcome": "admitted", "reasons": [], "canonicalEvidenceIdentitySha256": entry["candidate"].get("canonicalEvidenceIdentitySha256")}}
                entry["funnelCandidate"] = funnel
        with timed_span("proposal.hash_entry", proposalOrdinal=ordinal):
            entry["entrySha256"] = canonical_sha256(entry)
        with timed_span("proposal.persist_entry", proposalOrdinal=ordinal):
            _write_once(root / "proposal-journal" / f"{ordinal:08d}.json", entry)
        entries.append(entry); made += 1
        if ledger is not None:
            from .temporal_qd_evolution import _save_identity_ledger

            with timed_span(
                "proposal.persist_identity_ledger", proposalOrdinal=ordinal
            ):
                _save_identity_ledger(Path(identity_ledger_path), ledger)
        if factory_construction_policy is not None and immigrant_only_bootstrap:
            with timed_span(
                "proposal.evaluate_collision_tripwire",
                proposalOrdinal=ordinal,
            ):
                tripwire = factory_construction_policy["collisionTripwire"]
                minimum_attempts = int(tripwire["minimumImmigrantAttempts"])
                immigrant_attempts = sum(
                    1 for item in entries if item.get("originKind") == "random_immigrant"
                )
                immigrant_accepted = sum(
                    1
                    for item in entries
                    if item.get("originKind") == "random_immigrant"
                    and item.get("disposition") == "accepted"
                )
                accepted_ratio = (
                    immigrant_accepted / immigrant_attempts if immigrant_attempts else 0.0
                )
            if (
                immigrant_attempts >= minimum_attempts
                and accepted_ratio < float(tripwire["minimumAcceptedRatio"])
            ):
                dispositions: dict[str, int] = {}
                for item in entries:
                    key = str(item.get("disposition") or "unknown")
                    dispositions[key] = dispositions.get(key, 0) + 1
                failure = {
                    "schemaVersion": "temporal_qd_immigrant_collision_tripwire_v1",
                    "configSha256": config["configSha256"],
                    "generationIndex": generation_index,
                    "immigrantAttempts": immigrant_attempts,
                    "immigrantAccepted": immigrant_accepted,
                    "acceptedRatio": accepted_ratio,
                    "minimumAcceptedRatio": float(tripwire["minimumAcceptedRatio"]),
                    "minimumImmigrantAttempts": minimum_attempts,
                    "dispositionCounts": dict(sorted(dispositions.items())),
                    "globalPairSemanticCount": len(global_pair_semantics),
                    "reason": "rich_immigrant_semantic_acceptance_collapsed",
                }
                failure["tripwireSha256"] = canonical_sha256(failure)
                _write_once(root / "immigrant-collision-tripwire.json", failure)
                raise TemporalDiscoveryContractError(
                    "rich immigrant semantic acceptance collapsed below the frozen collision tripwire"
                )
        record_performance_interval(
            "proposal.total",
            proposal_started,
            proposalOrdinal=ordinal,
            originKind=proposal["originKind"],
            disposition=entry["disposition"],
            acceptedCount=len(accepted),
        )
        flush_performance_events()
        assert_performance_resource_guard()
    if len(accepted) < target_unique_candidates:
        with timed_span("generation.build_progress_result"):
            reason = (
                "max_proposal_attempts_reached"
                if len(entries) >= max_proposal_attempts
                else "max_new_proposals_reached"
            )
            return {"schemaVersion": "temporal_qd_pair_generation_progress_v1", "configSha256": config["configSha256"], "proposalCount": len(entries), "acceptedCount": len(accepted), "maxProposalAttempts": max_proposal_attempts, "terminationReason": reason, "completed": False}
    with timed_span("generation.finalize.sort_candidates"):
        accepted.sort(key=lambda item: item["candidateId"])
    with timed_span("generation.finalize.summarize_immigrant_distribution"):
        immigrant_distribution = _rich_immigrant_distribution(entries)
    assert_performance_resource_guard()
    with timed_span("generation.finalize.build_population"):
        population = {"schemaVersion": "temporal_qd_generation_population_v3", "qdVersion": "temporal_qd_evolution_v3", "policyName": "stage5e7_v3_robust_quality_archive", "policySha256": "__resolved_by_qd_hook__", "configSha256": config["configSha256"], "generationIndex": generation_index, "targetUniqueCandidates": target_unique_candidates, "maxProposalAttempts": max_proposal_attempts, "originCounts": {}, "proposalOrderCandidateIds": [row["candidateId"] for row in accepted], "candidateCount": len(accepted), "candidates": accepted, "authoredValidationBindingRequired": False, "bidirectionalPairPolicy": policy, "pairGenerationConfigSha256": config["configSha256"], "proposalAttempts": len(entries), "proposalSlots": {"targetUniqueCandidates": target_unique_candidates, "acceptedUniqueCandidates": len(accepted), "proposalAttempts": len(entries), "maxProposalAttempts": max_proposal_attempts, "remainingUniqueCandidateSlots": max(0, target_unique_candidates-len(accepted))}, **({"predeclaredEvidenceContextSha256": evidence_identity_context.get("predeclaredEvidenceContextSha256")} if evidence_identity_context is not None else {})}
    assert_performance_resource_guard()
    if immigrant_distribution is not None:
        population["immigrantConstructionDistribution"] = immigrant_distribution
    # Reuse the exact frozen legacy policy token only at the boundary, without
    # importing its generator or changing opt-in-disabled payload identities.
    from .temporal_qd_evolution import QD_POLICY_SHA256
    population["policySha256"] = QD_POLICY_SHA256
    with timed_span("generation.finalize.reduce_counts"):
        disposition_counts: dict[str, int] = {}
        origin_counts: dict[str, int] = {}
        for entry in entries:
            disposition_counts[str(entry["disposition"])] = disposition_counts.get(str(entry["disposition"]), 0) + 1
            origin = str(entry["originKind"])
            if entry["disposition"] == "accepted": origin_counts[origin] = origin_counts.get(origin, 0) + 1
        population["originCounts"] = dict(sorted(origin_counts.items()))
    # Origin counts participate in the population identity.
    with timed_span("generation.finalize.hash_population"):
        population["populationSha256"] = canonical_sha256(population)
    assert_performance_resource_guard()
    with timed_span("generation.finalize.persist_population"):
        _write_once(root / "population.json", population)
    assert_performance_resource_guard()
    with timed_span("generation.finalize.build_journal"):
        journal = {"schemaVersion": "temporal_qd_generation_journal_v3", "qdVersion": "temporal_qd_evolution_v3", "policyName": "stage5e7_v3_robust_quality_archive", "policySha256": population["policySha256"], "configSha256": config["configSha256"], "generationIndex": generation_index, "proposalCount": len(entries), "acceptedCount": len(accepted), "maxProposalAttempts": max_proposal_attempts, "nextImmigrantContinuationOrdinal": 0, "originProposalCounts": {origin: sum(1 for entry in entries if entry["originKind"] == origin) for origin in sorted({str(entry["originKind"]) for entry in entries})}, "originAcceptedCounts": dict(sorted(origin_counts.items())), "dispositionCounts": dict(sorted(disposition_counts.items())), "proposalSlots": population["proposalSlots"], "uniqueIdentityCounts": {"candidateIdentity": len(accepted), "pairGenome": len(seen_pair_genomes)}, "duplicateCounters": {"candidateIdentity": disposition_counts.get("duplicate_candidate_identity", 0), "pairGenome": disposition_counts.get("duplicate_pair_genome", 0), "pairGenomeGlobal": disposition_counts.get("duplicate_pair_genome_global", 0)}, "proposalSlotCounters": {"proposalsObserved": len(entries), "maxProposalAttempts": max_proposal_attempts}, "entrySha256s": [entry["entrySha256"] for entry in entries], "operatorImplementation": _clone(operator_implementation_identity), "populationSha256": population["populationSha256"], **({"globalIdentityLedger": {"pairExecutableSemanticCount": len(global_pair_semantics), "pairExecutableSemanticDuplicateRejections": int(ledger["pairExecutableSemanticDuplicateRejections"]), "identityLedgerSha256": ledger["ledgerSha256"]}} if ledger is not None else {})}
    if immigrant_distribution is not None:
        journal["immigrantConstructionDistribution"] = immigrant_distribution
    with timed_span("generation.finalize.hash_journal"):
        journal["journalSha256"] = canonical_sha256(journal)
    with timed_span("generation.finalize.persist_journal"):
        _write_once(root / "generation-journal.json", journal)
    with timed_span("generation.finalize.build_result"):
        return {"schemaVersion": "temporal_qd_pair_generation_result_v1", "configSha256": config["configSha256"], "populationSha256": population["populationSha256"], "journalSha256": journal["journalSha256"], "proposalCount": len(entries), "candidateCount": len(accepted), "originProposalCounts": journal["originProposalCounts"], "originAcceptedCounts": journal["originAcceptedCounts"], "proposalSlots": journal["proposalSlots"], "uniqueIdentityCounts": journal["uniqueIdentityCounts"], "duplicateCounters": journal["duplicateCounters"], "proposalSlotCounters": journal["proposalSlotCounters"], **({"immigrantConstructionDistribution": immigrant_distribution} if immigrant_distribution is not None else {}), "nextImmigrantContinuationOrdinal": 0, "completed": True}


def _generate_pair_population_optimized_impl(
    *,
    output_root: Path | str,
    generation_index: int,
    target_unique_candidates: int,
    run_config: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    parent_pairs: Sequence[FrozenPair] = (),
    parent_archive: Mapping[str, Any] | None = None,
    pair_factory: TypedPairFactory | None = None,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    evidence_identity_context: Mapping[str, Any] | None = None,
    operator_implementation_identity: Mapping[str, Any] | None = None,
    identity_ledger_path: Path | str | None = None,
    max_proposal_attempts: int = DEFAULT_MAX_PROPOSAL_ATTEMPTS,
    max_new_proposals: int | None = None,
) -> dict[str, Any]:
    """Memory-bounded equivalent of the preserved legacy implementation.

    Proposal entries remain the authoritative restart/replay artifact.  This
    path therefore retains compact identity/path state only and derives the
    final population in deterministic candidate-ID order directly from those
    immutable entries.
    """

    with timed_span("generation.validate_arguments"):
        if generation_index < 0 or target_unique_candidates < 1:
            raise TemporalDiscoveryContractError("pair generation index/target is invalid")
        max_proposal_attempts = int(max_proposal_attempts)
        if max_proposal_attempts < target_unique_candidates:
            raise TemporalDiscoveryContractError(
                "pair generation proposal ceiling is below its target"
            )
        if max_new_proposals is not None and int(max_new_proposals) < 0:
            raise TemporalDiscoveryContractError(
                "pair generation new proposal limit is negative"
            )
        if identity_ledger_path is not None and evidence_identity_context is None:
            raise TemporalDiscoveryContractError(
                "pair generation global identity ledger requires a predeclared evidence context"
            )
        if operator_implementation_identity is None:
            raise TemporalDiscoveryContractError(
                "pair generation requires a frozen operator implementation identity"
            )
    with timed_span("generation.resolve_runtime_inputs"):
        root = Path(output_root)
        policy = _clone(pair_policy)
        factory_construction_policy = (
            getattr(pair_factory, "construction_policy", None)
            if pair_factory is not None
            else None
        )
        optimized_factory = pair_factory
        view = getattr(pair_factory, "optimized_runtime_view", None)
        if callable(view):
            optimized_factory = view()
    if factory_construction_policy is not None:
        if not isinstance(factory_construction_policy, Mapping):
            raise TemporalDiscoveryContractError(
                "pair immigrant factory construction policy must be an object"
            )
        factory_construction_policy = _clone(factory_construction_policy)
        tripwire = factory_construction_policy.get("collisionTripwire")
        if (
            not isinstance(tripwire, Mapping)
            or isinstance(tripwire.get("minimumImmigrantAttempts"), bool)
            or int(tripwire.get("minimumImmigrantAttempts") or 0) < 1
            or isinstance(tripwire.get("minimumAcceptedRatio"), bool)
            or not 0.0
            < float(tripwire.get("minimumAcceptedRatio") or 0.0)
            <= 1.0
        ):
            raise TemporalDiscoveryContractError(
                "pair immigrant collision tripwire policy is invalid"
            )
    with timed_span("generation.build_config"):
        config = {
            "schemaVersion": PAIR_GENERATION_SCHEMA,
            "generationIndex": generation_index,
            "targetUniqueCandidates": target_unique_candidates,
            "maxProposalAttempts": max_proposal_attempts,
            "runConfig": _clone(run_config),
            "pairPolicy": policy,
            "operatorImplementation": _clone(operator_implementation_identity),
            "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
            **(
                {"immigrantConstructionPolicy": factory_construction_policy}
                if factory_construction_policy is not None
                else {}
            ),
            **(
                {
                    "globalIdentityLedger": {
                        "schemaVersion": "temporal_qd_identity_ledger_v3",
                        "locationPolicy": "caller_supplied_generation_global_ledger",
                    }
                }
                if identity_ledger_path is not None
                else {}
            ),
        }
        config["configSha256"] = canonical_sha256(config)
    with timed_span("generation.persist_config"):
        _write_once(root / "pair-config.json", config)
    with timed_span("generation.resume.scan_journal") as span:
        journal_paths = sorted((root / "proposal-journal").glob("*.json"))
        span.annotate(existingProposalCount=len(journal_paths))
    state = _CompactPairGenerationState.create()
    for ordinal, path in enumerate(journal_paths):
        with timing_scope(workClass="resume_replay", proposalOrdinal=ordinal):
            with timed_span("generation.resume.replay_entry"):
                row = _load_pair_proposal_entry(path, ordinal=ordinal)
                proposal = row.get("proposal")
                if not isinstance(proposal, Mapping):
                    raise TemporalDiscoveryContractError(
                        "pair proposal entry lacks immutable proposal material"
                    )
                replayed = replay_pair_proposal(
                    payload=proposal,
                    module_authority=module_authority,
                    native_validator=native_validator,
                    pair_compiler=pair_compiler,
                )
                if row.get("disposition") == "accepted":
                    candidate = row.get("candidate")
                    if (
                        not isinstance(candidate, Mapping)
                        or replayed is None
                        or candidate.get("bidirectionalGenome")
                        != replayed.canonical_payload()
                    ):
                        raise TemporalDiscoveryContractError(
                            "pair accepted proposal resume material diverged"
                        )
                state.observe_entry(row, journal_path=path)
    with timed_span("generation.resume.restore_accepted") as span:
        span.annotate(acceptedCount=len(state.accepted))
    with timed_span("generation.resume.rebuild_semantic_indexes") as span:
        span.annotate(
            candidateIdentityCount=len(state.seen_candidate_identities),
            executablePairSemanticCount=len(state.seen_pair_genomes),
        )
    ledger = None
    identity_index = None
    global_pair_semantics: dict[str, str] = {}
    if identity_ledger_path is not None:
        from .temporal_qd_evolution import (
            _ledger_bootstrap_archive,
            _ledger_identity_index,
            _load_identity_ledger,
            _save_identity_ledger,
        )

        ledger = _load_identity_ledger(Path(identity_ledger_path))
        _ledger_bootstrap_archive(
            ledger,
            parent_archive or {"cells": []},
            evidence_identity_context,
        )
        _pair_ledger_bootstrap_archive(
            ledger,
            parent_archive,
            evidence_identity_context=evidence_identity_context,
        )
        recovery_identity_index = _ledger_identity_index(ledger)
        recovery_semantic_index = _pair_ledger_semantic_index(ledger)
        for reference in state.accepted:
            _pair_ledger_recover_accepted_entry(
                ledger,
                _load_pair_proposal_entry(
                    reference.journal_path,
                    ordinal=reference.proposal_ordinal,
                ),
                evidence_identity_context=evidence_identity_context,
                identity_index=recovery_identity_index,
                semantic_index=recovery_semantic_index,
            )
        ledger["proposalSlotCounters"]["proposalsObserved"] = max(
            int(ledger["proposalSlotCounters"].get("proposalsObserved") or 0),
            state.proposal_count,
        )
        _save_identity_ledger(Path(identity_ledger_path), ledger)
        identity_index = _ledger_identity_index(ledger)
        global_pair_semantics = _pair_ledger_semantic_index(ledger)
    parents = sorted(
        (
            FrozenPair.from_payload(item.canonical_payload())
            for item in parent_pairs
        ),
        key=lambda item: item.identity_sha256,
    )
    archive_cells: list[dict[str, Any]] = []
    negative_cells: list[dict[str, Any]] = []
    selection_state: dict[str, dict[str, int]] = {}
    if parent_archive is not None:
        from .temporal_qd_evolution import (
            _initial_selection_state,
            _negative_novelty_cells,
            _reproduction_cells,
        )

        archive_cells = _reproduction_cells(
            parent_archive, allow_empty_quality_bootstrap=True
        )
        negative_cells = _negative_novelty_cells(parent_archive)
        selection_state = _initial_selection_state(archive_cells + negative_cells)
        parents = []
        for cell_id in state.archive_parent_selection_cell_ids:
            if cell_id not in selection_state:
                raise TemporalDiscoveryContractError(
                    "persisted pair parent selection references an unavailable archive cell"
                )
            selection_state[cell_id]["selectionVisitCount"] += 1
            selection_state[cell_id]["offspringAttemptCount"] += 1
    immigrant_only_bootstrap = not archive_cells and not parents
    if archive_cells:
        structural_parent_selections = len(state.archive_parent_selection_cell_ids)
    else:
        structural_parent_selections = sum(
            _explicit_parent_draw_count(
                [_load_pair_proposal_entry(path, ordinal=ordinal)]
            )
            for ordinal, path in enumerate(journal_paths)
        )

    def select_parent(label: str) -> tuple[FrozenPair, dict[str, Any] | None]:
        nonlocal structural_parent_selections
        if archive_cells:
            from .temporal_qd_evolution import _select_parent

            rng = random.Random(
                int(
                    canonical_sha256(
                        {
                            "generationSeed": config["configSha256"],
                            "selectionOrdinal": structural_parent_selections,
                            "label": label,
                        }
                    )[7:23],
                    16,
                )
            )
            cell, member, mode, lane, reason = _select_parent(
                rng=rng,
                cells=archive_cells,
                negative_novelty_cells=negative_cells,
                selection_state=selection_state,
                negative_novelty_slot=(structural_parent_selections + 1) % 10 == 0,
            )
            structural_parent_selections += 1
            return FrozenPair.from_payload(member["candidate"]["bidirectionalGenome"]), {
                "schemaVersion": "temporal_qd_pair_parent_selection_v1",
                "parentCellId": cell["cellId"],
                "parentCandidateId": member["candidateId"],
                "selectionMode": mode,
                "parentLane": lane,
                "parentLaneReason": reason,
                "paretoFront": member.get("paretoFront"),
                "crowdingDistance": member.get("crowdingDistance"),
            }
        parent = parents[structural_parent_selections % len(parents)]
        structural_parent_selections += 1
        return parent, None

    made = 0
    while (
        len(state.accepted) < target_unique_candidates
        and state.proposal_count < max_proposal_attempts
        and (max_new_proposals is None or made < max_new_proposals)
    ):
        proposal_started = start_performance_interval()
        ordinal = state.proposal_count
        with timed_span("proposal.select_origin", proposalOrdinal=ordinal) as span:
            seed = canonical_sha256(
                {
                    "schemaVersion": PAIR_GENERATION_SCHEMA,
                    "configSha256": config["configSha256"],
                    "proposalOrdinal": ordinal,
                }
            )
            use_immigrant = not (archive_cells or parents) or ordinal % 5 == 4
            use_crossover = (
                not use_immigrant
                and ordinal % 7 == 6
                and (len(archive_cells) > 1 or len(parents) > 1)
            )
            scheduled_origin = (
                "random_immigrant"
                if use_immigrant
                else "same_side_crossover"
                if use_crossover
                else "structural_offspring"
            )
            span.annotate(scheduledOrigin=scheduled_origin)
        with timing_scope(
            workClass="new_proposal",
            proposalOrdinal=ordinal,
            scheduledOrigin=scheduled_origin,
        ):
            with timed_span("proposal.construct"):
                if use_immigrant:
                    pair, proposal = propose_pair(
                        proposal_seed=seed,
                        parent=None,
                        pair_factory=optimized_factory,
                        module_authority=module_authority,
                        native_validator=native_validator,
                        pair_compiler=pair_compiler,
                    )
                elif use_crossover:
                    parent, parent_selection = select_parent("crossover_parent")
                    mate, mate_selection = select_parent("crossover_mate")
                    mate_attempts: list[dict[str, Any] | None] = []
                    eligible_count = (
                        sum(len(cell.get("members") or []) for cell in archive_cells)
                        if archive_cells
                        else len(parents)
                    )
                    while (
                        mate.identity_sha256 == parent.identity_sha256
                        and eligible_count > 1
                    ):
                        mate_attempts.append(mate_selection)
                        mate, mate_selection = select_parent(
                            f"crossover_mate_retry_{len(mate_attempts)}"
                        )
                    pair, proposal = _propose_crossover(
                        proposal_seed=seed,
                        parent=parent,
                        mate=mate,
                        module_authority=module_authority,
                        pair_compiler=pair_compiler,
                        parent_selection=parent_selection,
                        mate_selection=mate_selection,
                        mate_selection_attempts=mate_attempts,
                    )
                else:
                    parent, selection = select_parent("mutation")
                    depth = _mutation_depth_for_seed(seed)
                    pair, proposal = _propose_pair_sequence(
                        proposal_seed=seed,
                        parent=parent,
                        mutation_depth=depth,
                        module_authority=module_authority,
                        native_validator=native_validator,
                        pair_compiler=pair_compiler,
                        parent_selection=selection,
                    )
        with timed_span(
            "proposal.build_entry",
            proposalOrdinal=ordinal,
            originKind=proposal["originKind"],
        ):
            entry: dict[str, Any] = {
                "schemaVersion": "temporal_qd_proposal_entry_v3",
                "configSha256": config["configSha256"],
                "generationIndex": generation_index,
                "proposalOrdinal": ordinal,
                "originKind": proposal["originKind"],
                "proposal": proposal,
                "operatorImplementationSha256": canonical_sha256(
                    operator_implementation_identity
                ),
            }
        if ledger is not None:
            ledger["proposalSlotCounters"]["proposalsObserved"] += 1
        if pair is None:
            entry["disposition"] = proposal["disposition"]
        else:
            with timed_span(
                "proposal.materialize_candidate",
                proposalOrdinal=ordinal,
                originKind=proposal["originKind"],
            ):
                candidate = _materialize_pair_candidate_optimized(
                    pair=pair,
                    proposal=proposal,
                    pair_policy=policy,
                    generation_index=generation_index,
                    birth_ordinal=len(state.accepted),
                    proposal_ordinal=ordinal,
                )
            if evidence_identity_context is not None:
                from .temporal_qd_evolution import qd_canonical_evidence_identity

                with timed_span(
                    "proposal.compute_evidence_identity",
                    proposalOrdinal=ordinal,
                ):
                    candidate["canonicalEvidenceIdentitySha256"] = (
                        qd_canonical_evidence_identity(
                            candidate, evidence_identity_context
                        )
                    )
            with timed_span(
                "proposal.compute_executable_semantic_identity",
                proposalOrdinal=ordinal,
            ):
                pair_genome_sha256 = _pair_genome_semantic_sha256(pair)
            with timed_span(
                "proposal.check_local_duplicates",
                proposalOrdinal=ordinal,
            ) as span:
                duplicate_pair = pair_genome_sha256 in state.seen_pair_genomes
                duplicate_candidate = (
                    candidate["candidateIdentitySha256"]
                    in state.seen_candidate_identities
                )
                span.annotate(
                    duplicatePairGenome=duplicate_pair,
                    duplicateCandidateIdentity=duplicate_candidate,
                )
            if duplicate_pair:
                entry["disposition"] = "duplicate_pair_genome"
            elif duplicate_candidate:
                entry["disposition"] = "duplicate_candidate_identity"
            elif ledger is not None:
                from .temporal_qd_evolution import _ledger_accept, _ledger_duplicate_check

                candidate["canonicalEvidenceIdentitySha256"] = (
                    qd_canonical_evidence_identity(
                        candidate, evidence_identity_context
                    )
                )
                duplicate_reason, identity_checks = _ledger_duplicate_check(
                    ledger, candidate, identity_index=identity_index
                )
                entry["identityChecks"] = identity_checks
                if pair_genome_sha256 in global_pair_semantics:
                    entry["disposition"] = "duplicate_pair_genome_global"
                    ledger["pairExecutableSemanticDuplicateRejections"] += 1
                elif duplicate_reason is not None:
                    entry["disposition"] = duplicate_reason
                else:
                    entry["disposition"] = "accepted"
                    entry["candidate"] = candidate
                    _ledger_accept(ledger, candidate, identity_index=identity_index)
                    _pair_ledger_accept_semantic(
                        ledger,
                        semantic_sha256=pair_genome_sha256,
                        candidate_identity_sha256=str(
                            candidate["candidateIdentitySha256"]
                        ),
                        semantic_index=global_pair_semantics,
                    )
            else:
                with timed_span(
                    "proposal.accept_local_candidate", proposalOrdinal=ordinal
                ):
                    entry["disposition"] = "accepted"
                    entry["candidate"] = candidate
        if (
            proposal.get("disposition") == "materialized"
            and entry["disposition"] == "accepted"
        ):
            with timed_span("proposal.build_funnel_audit", proposalOrdinal=ordinal):
                entry["funnelCandidate"] = _optimized_funnel_audit(
                    pair=pair,
                    candidate=entry["candidate"],
                )
        with timed_span("proposal.hash_entry", proposalOrdinal=ordinal):
            entry["entrySha256"] = canonical_sha256(entry)
        journal_path = root / "proposal-journal" / f"{ordinal:08d}.json"
        with timed_span("proposal.persist_entry", proposalOrdinal=ordinal):
            _write_once(journal_path, entry)
        state.observe_entry(entry, journal_path=journal_path)
        journal_paths.append(journal_path)
        made += 1
        if ledger is not None:
            from .temporal_qd_evolution import _save_identity_ledger

            with timed_span(
                "proposal.persist_identity_ledger", proposalOrdinal=ordinal
            ):
                _save_identity_ledger(Path(identity_ledger_path), ledger)
        if factory_construction_policy is not None and immigrant_only_bootstrap:
            with timed_span(
                "proposal.evaluate_collision_tripwire", proposalOrdinal=ordinal
            ):
                tripwire = factory_construction_policy["collisionTripwire"]
                accepted_ratio = (
                    state.immigrant_accepted / state.immigrant_attempts
                    if state.immigrant_attempts
                    else 0.0
                )
            if (
                state.immigrant_attempts
                >= int(tripwire["minimumImmigrantAttempts"])
                and accepted_ratio < float(tripwire["minimumAcceptedRatio"])
            ):
                failure = {
                    "schemaVersion": "temporal_qd_immigrant_collision_tripwire_v1",
                    "configSha256": config["configSha256"],
                    "generationIndex": generation_index,
                    "immigrantAttempts": state.immigrant_attempts,
                    "immigrantAccepted": state.immigrant_accepted,
                    "acceptedRatio": accepted_ratio,
                    "minimumAcceptedRatio": float(
                        tripwire["minimumAcceptedRatio"]
                    ),
                    "minimumImmigrantAttempts": int(
                        tripwire["minimumImmigrantAttempts"]
                    ),
                    "dispositionCounts": dict(
                        sorted(state.disposition_counts.items())
                    ),
                    "globalPairSemanticCount": len(global_pair_semantics),
                    "reason": "rich_immigrant_semantic_acceptance_collapsed",
                }
                failure["tripwireSha256"] = canonical_sha256(failure)
                _write_once(root / "immigrant-collision-tripwire.json", failure)
                raise TemporalDiscoveryContractError(
                    "rich immigrant semantic acceptance collapsed below the frozen collision tripwire"
                )
        record_performance_interval(
            "proposal.total",
            proposal_started,
            proposalOrdinal=ordinal,
            originKind=proposal["originKind"],
            disposition=entry["disposition"],
            acceptedCount=len(state.accepted),
        )
        flush_performance_events()
        assert_performance_resource_guard()

    if len(state.accepted) < target_unique_candidates:
        with timed_span("generation.build_progress_result"):
            reason = (
                "max_proposal_attempts_reached"
                if state.proposal_count >= max_proposal_attempts
                else "max_new_proposals_reached"
            )
            return {
                "schemaVersion": "temporal_qd_pair_generation_progress_v1",
                "configSha256": config["configSha256"],
                "proposalCount": state.proposal_count,
                "acceptedCount": len(state.accepted),
                "maxProposalAttempts": max_proposal_attempts,
                "terminationReason": reason,
                "completed": False,
            }
    with timed_span("generation.finalize.sort_candidates"):
        accepted_references = tuple(
            sorted(state.accepted, key=lambda item: item.candidate_id)
        )
    with timed_span("generation.finalize.summarize_immigrant_distribution"):
        immigrant_distribution = _rich_immigrant_distribution_from_journal(
            journal_paths
        )
    assert_performance_resource_guard()
    proposal_slots = {
        "targetUniqueCandidates": target_unique_candidates,
        "acceptedUniqueCandidates": len(accepted_references),
        "proposalAttempts": state.proposal_count,
        "maxProposalAttempts": max_proposal_attempts,
        "remainingUniqueCandidateSlots": max(
            0, target_unique_candidates - len(accepted_references)
        ),
    }
    with timed_span("generation.finalize.build_population"):
        population: dict[str, Any] = {
            "schemaVersion": "temporal_qd_generation_population_v3",
            "qdVersion": "temporal_qd_evolution_v3",
            "policyName": "stage5e7_v3_robust_quality_archive",
            "policySha256": "__resolved_by_qd_hook__",
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "targetUniqueCandidates": target_unique_candidates,
            "maxProposalAttempts": max_proposal_attempts,
            "originCounts": {},
            "proposalOrderCandidateIds": [
                item.candidate_id for item in accepted_references
            ],
            "candidateCount": len(accepted_references),
            "candidates": _JournalCandidateSequence(accepted_references),
            "authoredValidationBindingRequired": False,
            "bidirectionalPairPolicy": policy,
            "pairGenerationConfigSha256": config["configSha256"],
            "proposalAttempts": state.proposal_count,
            "proposalSlots": proposal_slots,
            **(
                {
                    "predeclaredEvidenceContextSha256": evidence_identity_context.get(
                        "predeclaredEvidenceContextSha256"
                    )
                }
                if evidence_identity_context is not None
                else {}
            ),
        }
    assert_performance_resource_guard()
    if immigrant_distribution is not None:
        population["immigrantConstructionDistribution"] = immigrant_distribution
    from .temporal_qd_evolution import QD_POLICY_SHA256

    population["policySha256"] = QD_POLICY_SHA256
    with timed_span("generation.finalize.reduce_counts"):
        population["originCounts"] = dict(
            sorted(state.origin_accepted_counts.items())
        )
    # This hashes the exact legacy canonical document *without* its derived
    # populationSha256 field.  Candidate bytes are replayed one at a time from
    # durable entries instead of constructing a second population-sized string.
    with timed_span("generation.finalize.hash_population"):
        population_sha256 = _stream_canonical_sha256(population)
    population["populationSha256"] = population_sha256
    assert_performance_resource_guard()
    with timed_span("generation.finalize.persist_population") as span:
        span.annotate(
            encodedBytes=_write_canonical_stream_once(
                root / "population.json", population
            )
        )
    assert_performance_resource_guard()
    with timed_span("generation.finalize.build_journal"):
        journal = {
            "schemaVersion": "temporal_qd_generation_journal_v3",
            "qdVersion": "temporal_qd_evolution_v3",
            "policyName": "stage5e7_v3_robust_quality_archive",
            "policySha256": population["policySha256"],
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "proposalCount": state.proposal_count,
            "acceptedCount": len(accepted_references),
            "maxProposalAttempts": max_proposal_attempts,
            "nextImmigrantContinuationOrdinal": 0,
            "originProposalCounts": dict(
                sorted(state.origin_proposal_counts.items())
            ),
            "originAcceptedCounts": dict(
                sorted(state.origin_accepted_counts.items())
            ),
            "dispositionCounts": dict(sorted(state.disposition_counts.items())),
            "proposalSlots": proposal_slots,
            "uniqueIdentityCounts": {
                "candidateIdentity": len(accepted_references),
                "pairGenome": len(state.seen_pair_genomes),
            },
            "duplicateCounters": {
                "candidateIdentity": state.disposition_counts.get(
                    "duplicate_candidate_identity", 0
                ),
                "pairGenome": state.disposition_counts.get(
                    "duplicate_pair_genome", 0
                ),
                "pairGenomeGlobal": state.disposition_counts.get(
                    "duplicate_pair_genome_global", 0
                ),
            },
            "proposalSlotCounters": {
                "proposalsObserved": state.proposal_count,
                "maxProposalAttempts": max_proposal_attempts,
            },
            "entrySha256s": state.entry_sha256s,
            "operatorImplementation": _clone(operator_implementation_identity),
            "populationSha256": population_sha256,
            **(
                {
                    "globalIdentityLedger": {
                        "pairExecutableSemanticCount": len(global_pair_semantics),
                        "pairExecutableSemanticDuplicateRejections": int(
                            ledger["pairExecutableSemanticDuplicateRejections"]
                        ),
                        "identityLedgerSha256": ledger["ledgerSha256"],
                    }
                }
                if ledger is not None
                else {}
            ),
        }
    if immigrant_distribution is not None:
        journal["immigrantConstructionDistribution"] = immigrant_distribution
    with timed_span("generation.finalize.hash_journal"):
        journal["journalSha256"] = canonical_sha256(journal)
    with timed_span("generation.finalize.persist_journal"):
        _write_once(root / "generation-journal.json", journal)
    with timed_span("generation.finalize.build_result"):
        return {
            "schemaVersion": "temporal_qd_pair_generation_result_v1",
            "configSha256": config["configSha256"],
            "populationSha256": population_sha256,
            "journalSha256": journal["journalSha256"],
            "proposalCount": state.proposal_count,
            "candidateCount": len(accepted_references),
            "originProposalCounts": journal["originProposalCounts"],
            "originAcceptedCounts": journal["originAcceptedCounts"],
            "proposalSlots": journal["proposalSlots"],
            "uniqueIdentityCounts": journal["uniqueIdentityCounts"],
            "duplicateCounters": journal["duplicateCounters"],
            "proposalSlotCounters": journal["proposalSlotCounters"],
            **(
                {"immigrantConstructionDistribution": immigrant_distribution}
                if immigrant_distribution is not None
                else {}
            ),
            "nextImmigrantContinuationOrdinal": 0,
            "completed": True,
        }


def generate_pair_population(
    *,
    output_root: Path | str,
    generation_index: int,
    target_unique_candidates: int,
    run_config: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    parent_pairs: Sequence[FrozenPair] = (),
    parent_archive: Mapping[str, Any] | None = None,
    pair_factory: TypedPairFactory | None = None,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    evidence_identity_context: Mapping[str, Any] | None = None,
    operator_implementation_identity: Mapping[str, Any] | None = None,
    identity_ledger_path: Path | str | None = None,
    max_proposal_attempts: int = DEFAULT_MAX_PROPOSAL_ATTEMPTS,
    max_new_proposals: int | None = None,
    implementation: str = PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED,
) -> dict[str, Any]:
    """Generate a pair population with identity-excluded performance evidence.

    ``optimized`` is the production path after byte-exact oracle admission. It
    retains only compact journal state and streams the final population.
    ``legacy`` remains an explicit reference oracle during the bounded
    deprecation window.
    """

    implementation = str(implementation).strip().lower()
    if implementation not in _PAIR_GENERATION_IMPLEMENTATIONS:
        raise TemporalDiscoveryContractError(
            "pair generation implementation must be legacy or optimized"
        )
    implementation_function = (
        _generate_pair_population_legacy_impl
        if implementation == PAIR_GENERATION_IMPLEMENTATION_LEGACY
        else _generate_pair_population_optimized_impl
    )

    trace = PerformanceTrace(
        output_root=output_root,
        generation_index=generation_index,
    )
    result: dict[str, Any] | None = None
    outcome = "error"
    error_type: str | None = None
    try:
        with activate_performance_trace(trace):
            trace.assert_resource_guard()
            with trace.span(
                "generation.total",
                targetUniqueCandidates=target_unique_candidates,
                maxProposalAttempts=max_proposal_attempts,
                maxNewProposals=max_new_proposals,
                parentPairCount=len(parent_pairs),
                hasParentArchive=parent_archive is not None,
                hasIdentityLedger=identity_ledger_path is not None,
                generationImplementation=implementation,
            ):
                result = implementation_function(
                    output_root=output_root,
                    generation_index=generation_index,
                    target_unique_candidates=target_unique_candidates,
                    run_config=run_config,
                    pair_policy=pair_policy,
                    parent_pairs=parent_pairs,
                    parent_archive=parent_archive,
                    pair_factory=pair_factory,
                    module_authority=module_authority,
                    native_validator=native_validator,
                    pair_compiler=pair_compiler,
                    evidence_identity_context=evidence_identity_context,
                    operator_implementation_identity=operator_implementation_identity,
                    identity_ledger_path=identity_ledger_path,
                    max_proposal_attempts=max_proposal_attempts,
                    max_new_proposals=max_new_proposals,
                )
                trace.set_result(result)
            flush_performance_events()
        outcome = "completed" if result.get("completed") is True else "progress"
        return result
    except BaseException as exc:
        error_type = type(exc).__name__
        raise
    finally:
        trace.close(outcome=outcome, error_type=error_type)


__all__ = ["PAIR_GENERATION_IMPLEMENTATION_LEGACY", "PAIR_GENERATION_IMPLEMENTATION_OPTIMIZED", "PAIR_GENERATION_SCHEMA", "PAIR_PROPOSAL_SCHEMA", "PairModuleOperator", "TypedGrammarPairOperator", "TypedPairFactory", "generate_pair_population", "materialize_pair_candidate", "propose_pair", "propose_same_side_crossover", "replay_pair_proposal"]
