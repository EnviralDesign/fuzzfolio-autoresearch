"""Deterministic, immutable construction of v3/both QD pair proposals.

This module is intentionally separate from the frozen legacy v2 proposal loop.
It accepts only :class:`FrozenPair` parents / factory output and records the
complete material needed to replay a proposal without reading a mutable catalog.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .temporal_bidirectional_genome import (
    BidirectionalGenomeError,
    CanonicalPairCompiler,
    FrozenModule,
    FrozenPair,
    HoldMutationPlan,
    IdentitySnapshot,
    NativeModuleValidator,
    apply_pair_hold_mutation,
    canonical_json,
    canonical_sha256,
    deterministic_same_side_crossover,
    proposal_side,
)
from .temporal_discovery_base import TemporalDiscoveryContractError


PAIR_GENERATION_SCHEMA = "temporal_qd_pair_generation_v1"
PAIR_PROPOSAL_SCHEMA = "temporal_qd_pair_proposal_v1"


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
    def crossover(self, left_program: Mapping[str, Any], right_program: Mapping[str, Any], *, direction: str, proposal_seed: str) -> Mapping[str, Any]: ...
    def compile_program(self, template: FrozenModule, program: Mapping[str, Any], *, candidate_id: str) -> FrozenModule: ...


class TypedGrammarPairOperator:
    """Concrete bridge to typed fragments and ``IndicatorLearningRegistry``.

    ``grammar_factory`` is deliberately supplied by the frozen run authority;
    it receives the module snapshot rather than a catalog alias.  The registry
    is likewise constructed from the already frozen catalog payload.
    """

    def __init__(self, *, grammar_factory: Callable[[FrozenModule], Any], native_validator: NativeModuleValidator, indicator_registry: Any | None = None) -> None:
        self._grammar_factory = grammar_factory
        self._native_validator = native_validator
        self._indicator_registry = indicator_registry

    @staticmethod
    def _program(module: FrozenModule, program: Mapping[str, Any] | None = None) -> Any:
        from .temporal_typed_motif_grammar import Fragment, ModuleProgram

        raw = dict(program or module.program)
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
        return FrozenModule.freeze(program=program, profile=profile, grammar_context=template.grammar_context, catalog=template.catalog, policy=template.policy, native_authority=template.native_authority, native_report=report, lineage=lineage)

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
        return [] if registry is None else registry.enumerate_plans(module.profile)

    def apply_indicator(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        registry = self._registry(module)
        if registry is None:
            raise TemporalDiscoveryContractError("indicator learning registry is not frozen for this pair run")
        operator = registry.get(str(plan.get("operatorId") or ""))
        preview = operator.preview(module.profile, plan)
        report = self._native_validator.validate_v2(profile=preview, candidate_id=candidate_id)
        child_program = report.get("programSha256")
        child, application = operator.apply(module.profile, plan, parent_validated_program_sha256=module.native_program_sha256, child_validated_program_sha256=child_program)
        if child != preview:
            raise TemporalDiscoveryContractError("indicator-learning preview/application diverged")
        frozen = self._freeze(module, program=module.program, profile=child, report=report, lineage=[*[_clone(item) for item in module.lineage], {"operation": "indicator_learning", "side": module.direction, "plan": _clone(plan), "planSha256": plan.get("planSha256"), "application": _clone(application)}])
        audit = {"schemaVersion": "temporal_qd_indicator_operation_audit_v1", "side": module.direction, "operatorId": plan.get("operatorId"), "planSha256": plan.get("planSha256"), "applicationSha256": application.get("applicationSha256"), "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "nativeValidationReportSha256": frozen.native_validation_report_sha256}
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

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
    return __import__("json").loads(canonical_json(value))


def _sorted(plans: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((_clone(dict(plan)) for plan in plans), key=canonical_sha256)


def _hold_plans(module: FrozenModule) -> list[dict[str, Any]]:
    plans = (((module.profile.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
    output = []
    for plan in plans:
        if not isinstance(plan, Mapping) or not isinstance(plan.get("id"), str):
            continue
        # Bounded vocabulary is enforced again by HoldMutationPlan.create.
        output.append({"kind": "hold", "planId": plan["id"], "newHold": {"kind": "market_bars", "bars": 1 + int(canonical_sha256({"module": module.identity_sha256, "plan": plan["id"]})[-2:], 16) % 32}})
    return _sorted(output)


def _operation_choices(module: FrozenModule, authority: PairModuleOperator) -> list[dict[str, Any]]:
    rows = []
    rows.extend({"kind": "typed_grammar", "plan": plan} for plan in _sorted(authority.grammar_plans(module)))
    rows.extend({"kind": "indicator_learning", "plan": plan} for plan in _sorted(authority.indicator_plans(module)))
    rows.extend(_hold_plans(module))
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
        pair = pair_factory.create_pair(proposal_seed=seed)
        if not isinstance(pair, FrozenPair):
            raise TemporalDiscoveryContractError("pair immigrant factory must return FrozenPair")
        payload = {
            "schemaVersion": PAIR_PROPOSAL_SCHEMA,
            "proposalSeed": seed,
            "originKind": "random_immigrant",
            "side": side,
            "factoryPair": pair.canonical_payload(),
            "pairIdentitySha256": pair.identity_sha256,
            "disposition": "materialized",
        }
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
    if data.get("originKind") == "structural_offspring" and "crossoverAudit" in data:
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


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = canonical_json(value) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(f"refusing to overwrite divergent pair-generation artifact: {path}")
        return
    path.write_text(encoded, encoding="utf-8")


def generate_pair_population(
    *,
    output_root: Path | str,
    generation_index: int,
    target_unique_candidates: int,
    run_config: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    parent_pairs: Sequence[FrozenPair] = (),
    pair_factory: TypedPairFactory | None = None,
    module_authority: PairModuleOperator,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    evidence_identity_context: Mapping[str, Any] | None = None,
    operator_implementation_identity: Mapping[str, Any] | None = None,
    max_new_proposals: int | None = None,
) -> dict[str, Any]:
    """Small operational pair-QD journal/population path with exact restart.

    It is intentionally a separate versioned policy rather than a mutation of
    the historical QD generation format.  Population consumers use the normal
    QD loader/archive/campaign after its pair-material gate has admitted it.
    """
    if generation_index < 0 or target_unique_candidates < 1:
        raise TemporalDiscoveryContractError("pair generation index/target is invalid")
    root = Path(output_root)
    policy = _clone(pair_policy)
    if operator_implementation_identity is None:
        raise TemporalDiscoveryContractError("pair generation requires a frozen operator implementation identity")
    config = {"schemaVersion": PAIR_GENERATION_SCHEMA, "generationIndex": generation_index, "targetUniqueCandidates": target_unique_candidates, "runConfig": _clone(run_config), "pairPolicy": policy, "operatorImplementation": _clone(operator_implementation_identity)}
    config["configSha256"] = canonical_sha256(config)
    _write_once(root / "pair-config.json", config)
    entries: list[dict[str, Any]] = []
    for ordinal, path in enumerate(sorted((root / "proposal-journal").glob("*.json"))):
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
    accepted = [_clone(entry["candidate"]) for entry in entries if entry.get("disposition") == "accepted"]
    # Pair identity intentionally includes lineage/provenance.  Unique QD slots
    # must additionally be protected against a pre-patch journal containing
    # distinct provenance wrappers for the exact same executable long/short
    # module profiles.
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
    parents = sorted((FrozenPair.from_payload(item.canonical_payload()) for item in parent_pairs), key=lambda item: item.identity_sha256)
    made = 0
    while len(accepted) < target_unique_candidates and (max_new_proposals is None or made < max_new_proposals):
        ordinal = len(entries)
        seed = canonical_sha256({"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": config["configSha256"], "proposalOrdinal": ordinal})
        use_immigrant = not parents or ordinal % 5 == 4
        if use_immigrant:
            pair, proposal = propose_pair(proposal_seed=seed, parent=None, pair_factory=pair_factory, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler)
        elif len(parents) > 1 and ordinal % 7 == 6:
            parent = parents[ordinal % len(parents)]
            mate = parents[(ordinal + 1) % len(parents)]
            pair, audit = propose_same_side_crossover(proposal_seed=seed, parent=parent, mate=mate, module_authority=module_authority, pair_compiler=pair_compiler)
            proposal = {"schemaVersion": PAIR_PROPOSAL_SCHEMA, "proposalSeed": seed, "originKind": "structural_offspring", "side": audit["side"], "parentPair": parent.canonical_payload(), "parentPairIdentitySha256": parent.identity_sha256, "matePair": mate.canonical_payload(), "matePairIdentitySha256": mate.identity_sha256, "disposition": "materialized", "crossoverAudit": audit, "pair": pair.canonical_payload(), "pairIdentitySha256": pair.identity_sha256}
            proposal["proposalSha256"] = canonical_sha256(proposal)
        else:
            parent = parents[ordinal % len(parents)]
            pair, proposal = propose_pair(proposal_seed=seed, parent=parent, pair_factory=None, module_authority=module_authority, native_validator=native_validator, pair_compiler=pair_compiler)
        entry: dict[str, Any] = {"schemaVersion": "temporal_qd_proposal_entry_v3", "configSha256": config["configSha256"], "generationIndex": generation_index, "proposalOrdinal": ordinal, "originKind": proposal["originKind"], "proposal": proposal, "operatorImplementationSha256": canonical_sha256(operator_implementation_identity)}
        if pair is None:
            entry["disposition"] = proposal["disposition"]
        else:
            candidate = materialize_pair_candidate(pair=pair, proposal=proposal, pair_policy=policy, generation_index=generation_index, birth_ordinal=len(accepted), proposal_ordinal=ordinal)
            if evidence_identity_context is not None:
                from .temporal_qd_evolution import qd_canonical_evidence_identity
                candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(candidate, evidence_identity_context)
            pair_genome_sha256 = _pair_genome_semantic_sha256(pair)
            if pair_genome_sha256 in seen_pair_genomes:
                entry["disposition"] = "duplicate_pair_genome"
            elif candidate["candidateIdentitySha256"] in seen:
                entry["disposition"] = "duplicate_candidate_identity"
            else:
                entry["disposition"] = "accepted"; entry["candidate"] = candidate; accepted.append(candidate); seen.add(candidate["candidateIdentitySha256"]); seen_pair_genomes.add(pair_genome_sha256)
        if proposal.get("disposition") == "materialized" and entry["disposition"] == "accepted":
            pair_payload = proposal.get("pair") or proposal.get("factoryPair")
            if not isinstance(pair_payload, Mapping):
                raise TemporalDiscoveryContractError("materialized pair proposal lacks frozen pair")
            frozen = FrozenPair.from_payload(pair_payload)
            funnel = {"schemaVersion": "temporal_qd_proposal_funnel_stage_v1", "candidateId": entry["candidate"]["candidateId"], "rawSourceProfileSha256": frozen.raw_pair_sha256, "staticReachability": {"outcome": "reachable", "reasons": []}, "nativeValidation": {"outcome": "valid", "reasons": [], "resolvedProfileSha256": frozen.profile_sha256, "programSha256": frozen.native_program_sha256, "validationReportSha256": frozen.native_validation_report_sha256}, "admission": {"outcome": "admitted", "reasons": [], "canonicalEvidenceIdentitySha256": entry["candidate"].get("canonicalEvidenceIdentitySha256")}}
            entry["funnelCandidate"] = funnel
        entry["entrySha256"] = canonical_sha256(entry)
        _write_once(root / "proposal-journal" / f"{ordinal:08d}.json", entry)
        entries.append(entry); made += 1
    if len(accepted) < target_unique_candidates:
        return {"schemaVersion": "temporal_qd_pair_generation_progress_v1", "configSha256": config["configSha256"], "proposalCount": len(entries), "acceptedCount": len(accepted), "completed": False}
    accepted.sort(key=lambda item: item["candidateId"])
    population = {"schemaVersion": "temporal_qd_generation_population_v3", "qdVersion": "temporal_qd_evolution_v3", "policyName": "stage5e7_v3_robust_quality_archive", "policySha256": "__resolved_by_qd_hook__", "configSha256": config["configSha256"], "generationIndex": generation_index, "targetUniqueCandidates": target_unique_candidates, "originCounts": {}, "proposalOrderCandidateIds": [row["candidateId"] for row in accepted], "candidateCount": len(accepted), "candidates": accepted, "authoredValidationBindingRequired": False, "bidirectionalPairPolicy": policy, "pairGenerationConfigSha256": config["configSha256"], "proposalAttempts": len(entries), "proposalSlots": {"targetUniqueCandidates": target_unique_candidates, "acceptedUniqueCandidates": len(accepted), "proposalAttempts": len(entries), "remainingUniqueCandidateSlots": max(0, target_unique_candidates-len(accepted))}, **({"predeclaredEvidenceContextSha256": evidence_identity_context.get("predeclaredEvidenceContextSha256")} if evidence_identity_context is not None else {})}
    # Reuse the exact frozen legacy policy token only at the boundary, without
    # importing its generator or changing opt-in-disabled payload identities.
    from .temporal_qd_evolution import QD_POLICY_SHA256
    population["policySha256"] = QD_POLICY_SHA256
    disposition_counts: dict[str, int] = {}
    origin_counts: dict[str, int] = {}
    for entry in entries:
        disposition_counts[str(entry["disposition"])] = disposition_counts.get(str(entry["disposition"]), 0) + 1
        origin = str(entry["originKind"])
        if entry["disposition"] == "accepted": origin_counts[origin] = origin_counts.get(origin, 0) + 1
    population["originCounts"] = dict(sorted(origin_counts.items()))
    # Origin counts participate in the population identity.
    population["populationSha256"] = canonical_sha256(population)
    _write_once(root / "population.json", population)
    journal = {"schemaVersion": "temporal_qd_generation_journal_v3", "qdVersion": "temporal_qd_evolution_v3", "policyName": "stage5e7_v3_robust_quality_archive", "policySha256": population["policySha256"], "configSha256": config["configSha256"], "generationIndex": generation_index, "proposalCount": len(entries), "acceptedCount": len(accepted), "nextImmigrantContinuationOrdinal": 0, "originProposalCounts": {origin: sum(1 for entry in entries if entry["originKind"] == origin) for origin in sorted({str(entry["originKind"]) for entry in entries})}, "originAcceptedCounts": dict(sorted(origin_counts.items())), "dispositionCounts": dict(sorted(disposition_counts.items())), "proposalSlots": population["proposalSlots"], "uniqueIdentityCounts": {"candidateIdentity": len(accepted), "pairGenome": len(seen_pair_genomes)}, "duplicateCounters": {"candidateIdentity": disposition_counts.get("duplicate_candidate_identity", 0), "pairGenome": disposition_counts.get("duplicate_pair_genome", 0)}, "proposalSlotCounters": {"proposalsObserved": len(entries)}, "entrySha256s": [entry["entrySha256"] for entry in entries], "operatorImplementation": _clone(operator_implementation_identity), "populationSha256": population["populationSha256"]}
    journal["journalSha256"] = canonical_sha256(journal)
    _write_once(root / "generation-journal.json", journal)
    return {"schemaVersion": "temporal_qd_pair_generation_result_v1", "configSha256": config["configSha256"], "populationSha256": population["populationSha256"], "journalSha256": journal["journalSha256"], "proposalCount": len(entries), "candidateCount": len(accepted), "originProposalCounts": journal["originProposalCounts"], "originAcceptedCounts": journal["originAcceptedCounts"], "proposalSlots": journal["proposalSlots"], "uniqueIdentityCounts": journal["uniqueIdentityCounts"], "duplicateCounters": journal["duplicateCounters"], "proposalSlotCounters": journal["proposalSlotCounters"], "nextImmigrantContinuationOrdinal": 0, "completed": True}


__all__ = ["PAIR_GENERATION_SCHEMA", "PAIR_PROPOSAL_SCHEMA", "PairModuleOperator", "TypedGrammarPairOperator", "TypedPairFactory", "generate_pair_population", "materialize_pair_candidate", "propose_pair", "propose_same_side_crossover", "replay_pair_proposal"]
