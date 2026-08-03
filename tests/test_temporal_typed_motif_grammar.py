from __future__ import annotations

import copy
from pathlib import Path

import pytest

from autoresearch.temporal_search import canonical_sha256
from autoresearch.temporal_typed_motif_grammar import (
    DashboardNativeAuthority,
    Fragment,
    GrammarContext,
    GrammarError,
    REGISTRY,
    ModuleProgram,
    TypedFragmentGrammar,
    inspect_module,
    module_signatures,
)


CORE_PYTHON = Path(r"C:\repos\Trading-Dashboard\compute-service\.venv\Scripts\python.exe")


def _context() -> GrammarContext:
    indicators = tuple(
        {"meta": {"id": f"I_{name.upper()}", "instanceId": name}, "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"}}
        for name in ("rsi", "trend", "breakout", "volume")
    )
    groups = tuple({"id": f"g_{name}", "indicatorInstanceIds": [name]} for name in ("rsi", "trend", "breakout", "volume"))
    events = tuple({"id": f"e_{name}", "indicatorInstanceId": name, "longOutput": "bullish", "shortOutput": "bearish"} for name in ("rsi", "trend", "breakout", "volume"))
    return GrammarContext(
        instrument="EURUSD", indicators=indicators, evidence_groups=groups, event_bindings=events,
        execution_config={"managementLibrary": {"version": "temporal_management_v1", "defaultPlanId": "base", "plans": [{"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}}]}},
    )


def _grammar() -> TypedFragmentGrammar:
    if not CORE_PYTHON.is_file(): pytest.skip("Dashboard native authority environment is unavailable")
    return TypedFragmentGrammar(_context(), native_authority=DashboardNativeAuthority(CORE_PYTHON))


def test_management_stages_are_one_shot_and_cannot_starve_later_exit() -> None:
    grammar = TypedFragmentGrammar(_context(), native_authority=object())
    fragments = (
        Fragment("a", "arm_level", {"group": "g_rsi"}, {"threshold": 35.0}),
        Fragment("e", "enter_on_level", {"group": "g_rsi", "plan": "base"}, {"threshold": 45.0}),
        Fragment("m0", "deactivate_trailing", {}, {"bars": 2}),
        Fragment("m1", "move_break_even", {}, {"r": 0.5}),
        Fragment("x", "exit_on_age", {}, {"bars": 5}),
    )
    _canonical, _built, profile = grammar._profile_payload(ModuleProgram("long", fragments))
    transitions = {row["id"]: row for row in profile["graph"]["transitions"]}
    assert transitions["f2_deactivate_trailing_request"]["sourceStateId"] == "position_idle"
    assert transitions["f2_deactivate_trailing_applied"]["destinationStateId"] == "position_after_management_2"
    assert transitions["f3_move_break_even_request"]["sourceStateId"] == "position_after_management_2"
    assert transitions["f3_move_break_even_rejected"]["destinationStateId"] == "position_after_management_3"
    assert transitions["f4_exit_on_age_exit"]["sourceStateId"] == "position_after_management_3"


@pytest.mark.parametrize("seed", ["mean_reversion", "breakout", "trend"])
def test_registry_seeds_compile_v2_modules_through_real_dashboard_authority(seed: str) -> None:
    grammar = _grammar()
    compiled = grammar.compile_module(grammar.seed(direction="long", name=seed), candidate_id=f"typed_fragment_{seed}")
    assert compiled.profile["version"] == "v2"
    assert compiled.native_report["candidateAcceptable"] is True
    assert compiled.native_report["rawSourceProfileSha256"] == compiled.identities["rawModuleSha256"]
    assert compiled.identities["nativeProgramSha256"] == compiled.native_report["programSha256"]
    assert all(item["schemaVersion"] == "temporal_typed_fragment_activation_recipe_v1" for item in compiled.activation_witnesses)
    assert inspect_module(compiled.profile)["diagnosticOnly"] is True


def test_fragments_are_composed_generically_and_never_emit_program_metadata_into_profile() -> None:
    grammar = _grammar()
    mean = grammar.compile_module(grammar.seed(direction="long", name="mean_reversion"), candidate_id="typed_fragment_mean")
    breakout = grammar.compile_module(grammar.seed(direction="long", name="breakout"), candidate_id="typed_fragment_breakout")
    assert mean.profile != breakout.profile
    assert "typedMotifGrammar" not in mean.profile and "typedFragmentGrammar" not in mean.profile
    assert module_signatures(mean_to_program(mean))["programShapeSha256"] != module_signatures(mean_to_program(breakout))["programShapeSha256"]


def mean_to_program(compiled):
    from autoresearch.temporal_typed_motif_grammar import Fragment, ModuleProgram
    return ModuleProgram(compiled.program["direction"], tuple(Fragment(f"read_{index}", item["productionId"], item["resources"], item["choices"]) for index, item in enumerate(compiled.program["fragments"])))


def test_operations_are_deterministic_closed_and_typed() -> None:
    grammar = _grammar(); program = grammar.seed(direction="short", name="breakout")
    first = grammar.enumerate_operations(program); assert first == grammar.enumerate_operations(copy.deepcopy(program))
    operation = next(item for item in first if item["operation"] == "add_branch")
    child = grammar.apply(program, operation)
    assert child.lineage[-1]["operation"] == "add_branch"
    with pytest.raises(GrammarError, match="canonical and applicable"):
        grammar.apply(program, {"operation": "insert", "productionId": "not_a_fragment"})
    bad = copy.deepcopy(child)
    broken = type(bad)(bad.direction, tuple((*bad.fragments[:-1], type(bad.fragments[-1])(bad.fragments[-1].uid, bad.fragments[-1].production_id, {"group": "missing"}, bad.fragments[-1].choices))), bad.lineage)
    with pytest.raises(GrammarError, match="resource"):
        grammar.validate(broken)


def test_duplicate_specialize_move_and_crossover_are_typed_operations() -> None:
    grammar = _grammar()
    left = grammar.seed(direction="long", name="mean_reversion")
    # Add two gates so a same-family move is valid.
    for production in ("gate_delay", "gate_fresh_event"):
        operation = next(row for row in grammar.enumerate_operations(left) if row.get("productionId") == production and row["operation"] == "insert")
        left = grammar.apply(left, operation)
    duplicate = next(row for row in grammar.enumerate_operations(left) if row["operation"] == "duplicate_specialize")
    duplicated = grammar.apply(left, duplicate)
    move = next(row for row in grammar.enumerate_operations(duplicated) if row["operation"] == "move")
    moved = grammar.apply(duplicated, move)
    right = grammar.seed(direction="short", name="trend")
    child = grammar.crossover(moved, right, direction="long")
    grammar.compile_module(child, candidate_id="typed_fragment_crossover")


def test_pair_compilation_uses_canonical_dashboard_compiler_and_archive_boundary() -> None:
    grammar = _grammar()
    long = grammar.compile_module(grammar.seed(direction="long", name="mean_reversion"), candidate_id="typed_fragment_long")
    short = grammar.compile_module(grammar.seed(direction="short", name="trend"), candidate_id="typed_fragment_short")
    pair = grammar.compile_pair(long, short, candidate_id="typed_fragment_pair")
    assert pair["profile"]["version"] == "v3"
    assert pair["profile"]["directionMode"] == "both"
    assert pair["validation"]["candidateAcceptable"] is True


def test_generator_proves_canary_scale_diversity_without_retry_until_valid() -> None:
    grammar = _grammar(); programs = grammar.generate(count=1024, seed=20260803)
    assert len({canonical_sha256(item.canonical()) for item in programs}) == 1024
    compiled = grammar.compile_generation_native(programs)
    signatures = [module_signatures(item) for item in programs]
    assert sum(item.direction == "long" for item in programs) == 512
    assert sum(item.direction == "short" for item in programs) == 512
    assert len({item["directionSha256"] for item in signatures}) == 2
    assert len({item["parameterSha256"] for item in signatures}) >= 512
    assert len({item["motifCompositionSha256"] for item in signatures}) >= 128
    assert len({item["programShapeSha256"] for item in signatures}) >= 512
    assert len({item.identities["rawModuleSha256"] for item in compiled}) == 1024
    assert all(item.native_report["candidateAcceptable"] is True for item in compiled)
    graph_shapes = {item.identities["compiledGraphStructureSha256"] for item in compiled}
    assert len(graph_shapes) >= 128


def test_native_authority_is_mandatory_and_malformed_context_fails_closed() -> None:
    with pytest.raises(GrammarError, match="mandatory"):
        TypedFragmentGrammar(_context(), native_authority=None)  # type: ignore[arg-type]
    malformed = _context()
    duplicate = type(malformed)(malformed.instrument, malformed.indicators, malformed.evidence_groups, (*malformed.event_bindings, malformed.event_bindings[0]), malformed.execution_config)
    with pytest.raises(GrammarError, match="duplicate"):
        duplicate.normalized()


def test_native_validator_rejects_malformed_action_authoritatively() -> None:
    grammar = _grammar()
    profile = grammar.compile_module(grammar.seed(direction="long", name="breakout"), candidate_id="typed_fragment_good").profile
    broken = copy.deepcopy(profile)
    entry = next(t for t in broken["graph"]["transitions"] if t["actions"] and t["actions"][0]["kind"] == "enter_next_open")
    entry["actions"][0]["kind"] = "invented_action"
    report = grammar.native_authority.validate_v2(profile=broken, candidate_id="typed_fragment_bad_action")
    assert report["candidateAcceptable"] is False


def test_every_sealed_production_has_a_native_evaluable_adjacent_lifecycle_embedding() -> None:
    """One batched authority call covers every action/fragment family."""
    grammar = _grammar()
    programs = []
    for production, spec in REGISTRY.items():
        if spec.family == "arm":
            base = grammar.seed(direction="long", name="mean_reversion")
            if production != "arm_level":
                operation = next(row for row in grammar.enumerate_operations(base) if row["operation"] == "substitute" and row["index"] == 0 and row["productionId"] == production)
                base = grammar.apply(base, operation)
        elif spec.family == "entry":
            base = grammar.seed(direction="long", name="mean_reversion")
            entry_index = next(index for index, item in enumerate(base.fragments) if item.production_id.startswith("enter_"))
            if base.fragments[entry_index].production_id != production:
                operation = next(row for row in grammar.enumerate_operations(base) if row["operation"] == "substitute" and row["index"] == entry_index and row["productionId"] == production)
                base = grammar.apply(base, operation)
        elif spec.family == "exit":
            base = grammar.seed(direction="long", name="mean_reversion")
            if production != "exit_on_age":
                operation = next(row for row in grammar.enumerate_operations(base) if row.get("productionId") == production and row["operation"] == "add_branch")
                base = grammar.apply(base, operation)
        elif spec.family in {"gate", "management", "recovery"}:
            base = grammar.seed(direction="long", name="mean_reversion")
            operation = next(row for row in grammar.enumerate_operations(base) if row.get("productionId") == production and row["operation"] in {"insert", "add_branch"})
            base = grammar.apply(base, operation)
        else:
            continue
        programs.append(base)
    compiled = grammar.compile_generation_native(programs, candidate_prefix="typed_fragment_exhaustive")
    assert len(compiled) == len(programs) == len(REGISTRY)
    assert all(item.native_report["candidateAcceptable"] is True for item in compiled)


def test_recovery_and_watch_expiry_are_one_way_and_no_resources_are_unused() -> None:
    grammar = _grammar()
    program = grammar.seed(direction="long", name="mean_reversion")
    for production in ("cooldown", "cooldown"):
        operation = next(row for row in grammar.enumerate_operations(program) if row.get("productionId") == production and row["operation"] == "insert")
        program = grammar.apply(program, operation)
    compiled = grammar.compile_module(program, candidate_id="typed_fragment_recovery")
    graph = compiled.profile["graph"]
    recovery = [state["id"] for state in graph["states"] if state["id"].startswith("recovery_")]
    assert recovery
    assert not any(t["sourceStateId"] == "ready" and t["destinationStateId"] in recovery for t in graph["transitions"])
    assert any(t["sourceStateId"] == "position_idle" and t["destinationStateId"] == recovery[0] and t["eventClass"] == "execution" for t in graph["transitions"])
    for state in [item["id"] for item in graph["states"] if item["id"].startswith("watch_")]:
        assert any(t["sourceStateId"] == state and t["destinationStateId"] == "ready" and t["priority"] > 10 for t in graph["transitions"])
    referenced_groups = {fragment["resources"].get("group") for fragment in compiled.program["fragments"]} - {None}
    referenced_events = {fragment["resources"].get("event") for fragment in compiled.program["fragments"]} - {None}
    assert {item["id"] for item in graph["evidenceGroups"]} == referenced_groups
    assert {item["id"] for item in graph["eventBindings"]} == referenced_events


def test_edit_ids_and_lineage_do_not_change_canonical_program_identity_or_malformed_authority_pass() -> None:
    grammar = _grammar(); first = grammar.seed(direction="long", name="trend")
    cloned = type(first)(first.direction, tuple(type(item)(f"changed_{index}", item.production_id, item.resources, item.choices) for index, item in enumerate(first.fragments)), ({"operation": "different"},))
    assert first.canonical() == cloned.canonical()
    class BadAuthority:
        def validate_v2(self, **_kwargs): return {"schemaVersion": "wrong"}
    with pytest.raises(GrammarError, match="native validator rejected"):
        TypedFragmentGrammar(_context(), native_authority=BadAuthority()).compile_module(first, candidate_id="typed_fragment_bad_authority")


def test_noop_and_collision_operations_are_not_enumerated() -> None:
    grammar = _grammar(); program = grammar.seed(direction="long", name="mean_reversion")
    with pytest.raises(GrammarError, match="canonical and applicable"):
        grammar.apply(program, {"operation": "mutate_choice", "index": 0, "choice": "threshold", "value": 35.0})
    # A fragment cannot be substituted for itself and an identical adjacent
    # fragment pair never receives a meaningless move operation.
    operations = grammar.enumerate_operations(program)
    assert not any(item.get("operation") == "substitute" and item.get("productionId") == program.fragments[item.get("index", 0)].production_id for item in operations)
