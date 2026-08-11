"""Build a compact, hermetic Python oracle corpus for the native v5 operators.

The corpus is intentionally a test fixture, never a generation runtime.  It
opens only the checked-in stopped-run program and sealed v5 authority, then
drives the current deterministic Python operator authority.  It launches one
test-only frozen Dashboard JSONL validator process; there is no market data,
gateway, or wall-clock input.

Accepted rows retain the canonical parent/plan/application/child facts needed
by a Rust implementation to compare bytes as well as semantics.  Rejected and
no-op rows are first-class facts too: later native work must not turn a closed
rejection into an accidental child.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from autoresearch.evolvable_module_genome import (
    EffectKind,
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeNodeV1,
    ResourcePoolV1,
    decode_program,
)
from autoresearch.evolvable_module_qd_authority import (
    _choice as _authority_choice,
    _side_seed,
)
from autoresearch.evolvable_module_resource_operators import GenomeResourceOperatorLayer
from autoresearch.evolvable_module_temporal_operators import GenomeTemporalOperatorLayer
from autoresearch.evolvable_module_topology import (
    apply_crossover,
    apply_plan,
    make_crossover_plan,
    make_plan,
)
from autoresearch.temporal_bidirectional_genome import canonical_sha256, proposal_side
from autoresearch.temporal_bidirectional_genome import FrozenModule, FrozenPair
from autoresearch.temporal_qd_pair_factory import PairAuthorityBundle
from autoresearch.temporal_qd_initial_protection import (
    apply_initial_protection_plan,
    default_initial_protection_policy,
    enumerate_initial_protection_plans,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import (
    _operation_choices,
    _propose_crossover,
    _propose_pair_sequence,
    _select_operation,
    propose_same_side_crossover,
)


CORPUS_SCHEMA = "temporal_qd_v5_operator_python_oracle_corpus_v1"
CASE_SCHEMA = "temporal_qd_v5_operator_python_oracle_case_v1"
AUTHORITY_TRANSCRIPT_SCHEMA = "temporal_qd_v5_real_authority_transcript_v1"
DEFAULT_STOPPED = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "temporal_qd_v5_stopped_run_oracle.json"
DEFAULT_AUTHORITY = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "temporal_qd_v5_shared_authority_oracle.json.gz"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "temporal_qd_v5_operator_python_oracle_corpus.json.gz"

RESOURCE_KINDS = (
    "directional_event_insert",
    "evidence_group_create",
    "evidence_threshold_mutate",
    "indicator_instance_insert",
    "indicator_lookback_mutate",
    "indicator_period_mutate",
    "indicator_range_mutate",
    "indicator_substitute",
    "indicator_timeframe_mutate",
)
TEMPORAL_FAMILIES = (
    "action_cooldown",
    "consecutive_true",
    "fresh_event_absence",
    "fresh_event_age_window",
    "position_age",
    "predicate_edge",
    "state_or_condition_age",
    "utc_session_window",
)
TOPOLOGY_OPERATIONS = (
    "insert_setup",
    "remove_setup",
    "rewire_entry_branch",
    "insert_entry_branch",
    "remove_entry_branch",
    "insert_confirmation_rejection",
    "insert_timeout_rearm",
    "remove_timeout_rearm",
    "insert_management_region",
    "remove_management_region",
    "rewire_management_region",
    "insert_exit_region",
    "remove_exit_region",
    "rewire_exit_region",
)
CROSSOVER_PORTS = ("entry_setup", "management_hub", "exit_hub")
DISTINCT_CROSSOVER_MATE_LIMIT = 16
DISTINCT_CROSSOVER_SEED_LIMIT = 96
REAL_CROSSOVER_NO_OP_SEED_LIMIT = 32
SYNTHETIC_PER_FAMILY_EVIDENCE = "synthetic_per_family_coverage"
REAL_AUTHORITY_TRANSCRIPT_EVIDENCE = "real_authority_transcript"

# This literal is deliberately *not* recomputed.  It is the short-side
# lineage topology recorded by the frozen stopped Dashboard proposal journal.
# Current Python recompiles the stopped program to a different topology hash;
# retaining both facts prevents a native port from silently treating historical
# lineage and current compiler output as interchangeable.
HISTORICAL_SHORT_TOPOLOGY_SHA256 = (
    "sha256:d16635b9006a8220a7030bc4a64fd2a208cbbcde31954564b1e6945aa7f7ee06"
)


class CorpusError(RuntimeError):
    """A frozen fixture or a purported corpus is inconsistent."""


@dataclass
class _OracleRuntime:
    bundle: PairAuthorityBundle
    authority: Any
    stopped_proposal_seed: str
    short_module: FrozenModule | None = None
    selection_pair: FrozenPair | None = None
    module_validation_count: int = 0
    pair_compilation_count: int = 0


_RUNTIME: _OracleRuntime | None = None


def _runtime() -> _OracleRuntime:
    if _RUNTIME is None:
        raise CorpusError("operator oracle native authority is not open")
    return _RUNTIME


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _clone(value: Any) -> Any:
    return json.loads(_canonical_bytes(value))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CorpusError(f"fixture must be an object: {path}")
    return value


def _read_authority(path: Path) -> dict[str, Any]:
    value = json.loads(gzip.decompress(path.read_bytes()))
    if not isinstance(value, dict):
        raise CorpusError(f"authority fixture must be an object: {path}")
    return value


def _verify_fixture_identity(value: Mapping[str, Any], *, name: str) -> None:
    supplied = value.get("fixtureSha256")
    body = {key: item for key, item in value.items() if key != "fixtureSha256"}
    if supplied != _hash(body):
        raise CorpusError(f"{name} fixture identity mismatch")


def _compile(genome: EvolvableModuleGenomeV1, *, label: str) -> dict[str, Any]:
    """Shape a v2 profile; only ``_freeze_module`` may claim native facts."""

    return _runtime().authority.compiler.compile(genome, candidate_id=f"v5_oracle_{label}")


def _freeze_module(genome: EvolvableModuleGenomeV1, *, label: str) -> FrozenModule:
    runtime = _runtime()
    genome.validate()
    candidate_id = "v5_oracle_" + "".join(
        character if character.isascii() and character.isalnum() else "_"
        for character in label
    )
    profile = _compile(genome, label=label)["profile"]
    module = FrozenModule.validate_native(
        program=genome.canonical(),
        profile=profile,
        grammar_context=runtime.authority.grammar_context(genome.direction),
        catalog=runtime.authority.catalog_identity(genome.direction),
        policy=runtime.authority.module_policy(genome.direction),
        native_authority_identity=runtime.bundle.native_identity,
        native_validator=runtime.bundle.validator,
        candidate_id=candidate_id,
        lineage=[{"operation": "v5_operator_oracle", "label": label}],
    )
    runtime.module_validation_count += 1
    return module


def _facts(genome: EvolvableModuleGenomeV1, *, label: str) -> dict[str, Any]:
    module = _freeze_module(genome, label=label)
    payload = module.canonical_payload()
    # The complete snapshot payloads (especially the frozen indicator catalog)
    # are already sealed in the checked-in shared-authority fixture.  Repeating
    # them for every parent and child would turn this parity corpus into a
    # multi-megabyte catalog clone.  Keep the exact public surface and the
    # four snapshot identities here; Rust resolves their bytes from that one
    # pinned authority object before comparing module identities.
    compact_module = {
        "schemaVersion": payload["schemaVersion"],
        "direction": payload["direction"],
        "program": payload["program"],
        "profile": payload["profile"],
        "nativeReport": payload["nativeReport"],
        "lineage": payload["lineage"],
        "authoritySnapshotSha256s": {
            "grammarContext": payload["grammarContext"]["sha256"],
            "catalog": payload["catalog"]["sha256"],
            "policy": payload["policy"]["sha256"],
            "nativeAuthority": payload["nativeAuthority"]["sha256"],
        },
        "identities": payload["identities"],
    }
    return {
        "program": payload["program"],
        "profile": payload["profile"],
        "nativeValidation": payload["nativeReport"],
        "frozenModule": compact_module,
        "publicIdentity": payload["identities"],
    }


def _case(
    *,
    case_id: str,
    domain: str,
    family: str,
    disposition: str,
    parent: EvolvableModuleGenomeV1,
    plan: Mapping[str, Any] | None,
    child: EvolvableModuleGenomeV1 | None = None,
    application: Mapping[str, Any] | None = None,
    audit_extra: Mapping[str, Any] | None = None,
    error: str | None = None,
    depth: int | None = None,
    prerequisites: list[str] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schemaVersion": CASE_SCHEMA,
        # These hand-selected plans are a compact vocabulary matrix.  They are
        # intentionally kept separate from the factory/proposal transcripts
        # below, which are evidence of the production authority's own choices.
        "evidenceClass": SYNTHETIC_PER_FAMILY_EVIDENCE,
        "caseId": case_id,
        "domain": domain,
        "family": family,
        "disposition": disposition,
        "mutationDepth": depth,
        "prerequisites": list(prerequisites or []),
        "parent": _facts(parent, label=f"{case_id}-parent"),
        "plan": _clone(plan) if plan is not None else None,
        "child": _facts(child, label=f"{case_id}-child") if child is not None else None,
        "application": _clone(application) if application is not None else None,
        "error": error,
    }
    audit = {
        "schemaVersion": "temporal_qd_v5_operator_oracle_audit_v1",
        "caseId": case_id,
        "disposition": disposition,
        "parentProgramSha256": value["parent"]["publicIdentity"]["programSha256"],
        "childProgramSha256": (
            value["child"]["publicIdentity"]["programSha256"] if value["child"] else None
        ),
        "planSha256": (value["plan"] or {}).get("planSha256"),
        "allChecksPassed": disposition == "accepted",
        **_clone(audit_extra or {}),
    }
    audit["auditSha256"] = _hash(audit)
    value["audit"] = audit
    value["caseSha256"] = _hash({key: item for key, item in value.items() if key != "caseSha256"})
    return value


def _base(stopped: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
    source = stopped["construction"]["sides"]["long"]["program"]
    return decode_program(
        program_kind=str(source["programKind"]), codec=str(source["codec"]), payload=source
    )


def _short_base(stopped: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
    source = stopped["construction"]["sides"]["short"]["program"]
    return decode_program(
        program_kind=str(source["programKind"]), codec=str(source["codec"]), payload=source
    )


def _plan_with_kind(layer: GenomeResourceOperatorLayer, genome: EvolvableModuleGenomeV1, kind: str) -> dict[str, Any]:
    return next(plan for plan in layer.enumerate_plans(genome) if plan["construction"]["kind"] == kind)


def _temporal_parent(
    base: EvolvableModuleGenomeV1, layer: GenomeResourceOperatorLayer
) -> EvolvableModuleGenomeV1:
    event_plan = _plan_with_kind(layer, base, "directional_event_insert")
    eventful, _ = layer.apply(base, event_plan)
    event_id = str(event_plan["construction"]["eventId"])
    nodes: list[GenomeNodeV1] = []
    for node in eventful.nodes:
        if node.node_id == "setup":
            node = GenomeNodeV1(
                node.node_id,
                node.zone,
                node.kind,
                {
                    "kind": "all",
                    "guards": [
                        _clone(node.guard),
                        {"kind": "fresh_event", "eventId": event_id},
                        {"kind": "utc_time_window", "startMinute": 0, "endMinute": 360, "weekdays": None},
                    ],
                },
                node.resources,
                node.timeout_bars,
            )
        elif node.node_id == "manage":
            node = GenomeNodeV1(
                node.node_id, node.zone, node.kind,
                {"kind": "position_age_at_least", "events": 1}, node.resources, node.timeout_bars,
            )
        nodes.append(node)
    result = EvolvableModuleGenomeV1(
        eventful.direction, eventful.resources, tuple(nodes), eventful.edges,
        eventful.budget, eventful.program_kind, eventful.codec, eventful.instrument,
    )
    result.validate()
    return result


def _replace_management(
    genome: EvolvableModuleGenomeV1, *, plan_id: str, replacement: Mapping[str, Any]
) -> EvolvableModuleGenomeV1:
    rows = []
    found = False
    for row in genome.resources.management_refs:
        if row.get("id") == plan_id:
            rows.append(_clone(replacement))
            found = True
        else:
            rows.append(_clone(row))
    if not found:
        raise CorpusError("management plan disappeared while materializing corpus")
    child = EvolvableModuleGenomeV1(
        genome.direction,
        ResourcePoolV1(genome.resources.indicators, genome.resources.evidence_groups, genome.resources.events, tuple(rows)),
        genome.nodes,
        genome.edges,
        genome.budget,
        genome.program_kind,
        genome.codec,
        genome.instrument,
    )
    child.validate()
    return child


def _hold_child(
    genome: EvolvableModuleGenomeV1, *, plan_id: str, hold: Mapping[str, Any]
) -> tuple[EvolvableModuleGenomeV1, dict[str, Any]]:
    before = next(row for row in genome.resources.management_refs if row["id"] == plan_id)
    replacement = _clone(before)
    if hold["kind"] == "none":
        replacement.pop("holdPolicy", None)
    else:
        replacement["holdPolicy"] = _clone(hold)
    child = _replace_management(genome, plan_id=plan_id, replacement=replacement)
    application = {
        "schemaVersion": "temporal_qd_v5_hold_policy_application_v1",
        "managementPlanId": plan_id,
        "before": _clone(before.get("holdPolicy") or {"kind": "none"}),
        "after": _clone(hold),
    }
    application["applicationSha256"] = _hash(application)
    return child, application


def _initial_child(
    genome: EvolvableModuleGenomeV1, *, plan: Mapping[str, Any]
) -> tuple[EvolvableModuleGenomeV1, dict[str, Any]]:
    profile = _compile(genome, label="initial-parent")["profile"]
    profile_child, application = apply_initial_protection_plan(
        profile, plan, default_initial_protection_policy()
    )
    plan_id = str(plan["planId"])
    selected = next(
        row for row in profile_child["executionConfig"]["managementLibrary"]["plans"]
        if row["id"] == plan_id
    )
    before = next(row for row in genome.resources.management_refs if row["id"] == plan_id)
    replacement = _clone(before)
    replacement["initialStop"] = _clone(selected["initialStop"])
    replacement["initialTarget"] = _clone(selected["initialTarget"])
    return _replace_management(genome, plan_id=plan_id, replacement=replacement), application


def _topology_apply(
    genome: EvolvableModuleGenomeV1, operation: str, **arguments: Any
) -> tuple[EvolvableModuleGenomeV1, dict[str, Any], dict[str, Any]]:
    plan = make_plan(genome, operation=operation, **arguments)
    applied = apply_plan(genome, plan)
    application = applied.delta.canonical()
    application["applicationSha256"] = _hash(application)
    return applied.genome, plan.canonical() | {"planSha256": plan.identity_sha256}, application


def _topology_cases(base: EvolvableModuleGenomeV1) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    # Each inverse operation gets a freshly materialized valid parent.  This
    # avoids conflating the operation's behavior with earlier unrelated edits.
    definitions: list[tuple[str, EvolvableModuleGenomeV1, dict[str, Any], list[str]]] = []
    definitions.append(("insert_setup", base, {"edgeId": "start_setup", "kind": "context", "guard": {"kind": "always"}}, []))
    setup, _, _ = _topology_apply(base, "insert_setup", edgeId="start_setup", kind="context", guard={"kind": "always"})
    setup_id = next(node.node_id for node in setup.nodes if node.node_id not in {item.node_id for item in base.nodes})
    definitions.append(("remove_setup", setup, {"nodeId": setup_id}, ["insert_setup"]))
    definitions.append(("rewire_entry_branch", base, {"edgeId": "setup_entry", "sourceId": "setup", "priority": 10, "guard": {"kind": "always"}}, []))
    definitions.append(("insert_entry_branch", base, {"sourceId": "setup", "managementRefId": base.resources.management_refs[0]["id"], "priority": 90, "hubPriority": 10, "guard": {"kind": "always"}}, []))
    added_entry, _, _ = _topology_apply(base, "insert_entry_branch", sourceId="setup", managementRefId=base.resources.management_refs[0]["id"], priority=90, hubPriority=10, guard={"kind": "always"})
    new_entry = next(node.node_id for node in added_entry.nodes if node.node_id not in {item.node_id for item in base.nodes})
    definitions.append(("remove_entry_branch", added_entry, {"nodeId": new_entry}, ["insert_entry_branch"]))
    definitions.append(("insert_confirmation_rejection", base, {"edgeId": "setup_entry", "rejectPriority": 20, "rejectionTimeoutBars": 6, "confirmGuard": {"kind": "always"}, "rejectGuard": {}, "sourceRejectGuard": {"kind": "not", "guard": {"kind": "always"}}}, []))
    definitions.append(("insert_timeout_rearm", base, {"timeoutBars": 12, "guard": {"kind": "always"}}, []))
    rearm, _, _ = _topology_apply(base, "insert_timeout_rearm", timeoutBars=12, guard={"kind": "always"})
    rearm_id = next(node.node_id for node in rearm.nodes if node.node_id not in {item.node_id for item in base.nodes})
    definitions.append(("remove_timeout_rearm", rearm, {"nodeId": rearm_id}, ["insert_timeout_rearm"]))
    definitions.append(("insert_management_region", base, {"effect": EffectKind.TIGHTEN_STOP.value, "priority": 40, "kind": "tighten", "guard": {"kind": "always"}}, []))
    definitions.append(("remove_management_region", base, {"nodeId": "manage"}, []))
    definitions.append(("rewire_management_region", base, {"nodeId": "manage", "priority": 40, "effect": EffectKind.TIGHTEN_STOP.value, "guard": {"kind": "always"}}, []))
    definitions.append(("insert_exit_region", base, {"priority": 40, "kind": "risk_exit", "guard": {"kind": "always"}}, []))
    definitions.append(("remove_exit_region", base, {"nodeId": "exit"}, []))
    definitions.append(("rewire_exit_region", base, {"nodeId": "exit", "priority": 40, "guard": {"kind": "always"}}, []))
    for operation, parent, arguments, prerequisites in definitions:
        child, plan, application = _topology_apply(parent, operation, **arguments)
        cases.append(_case(
            case_id=f"topology.{operation}", domain="topology", family=operation,
            disposition="accepted", parent=parent, plan=plan, child=child,
            application=application, prerequisites=prerequisites,
        ))
    return cases


def _crossover_cases(base: EvolvableModuleGenomeV1, layer: GenomeResourceOperatorLayer) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    donors: dict[str, tuple[EvolvableModuleGenomeV1, str]] = {}
    entry, _, _ = _topology_apply(base, "insert_setup", edgeId="start_setup", kind="donor", guard={"kind": "always"})
    donors["entry_setup"] = (entry, "start_setup")
    management, _, _ = _topology_apply(base, "insert_management_region", effect=EffectKind.BREAK_EVEN.value, priority=40, kind="donor_break_even", guard={"kind": "always"})
    donors["management_hub"] = (management, next(edge.edge_id for edge in management.edges if edge.edge_id not in {item.edge_id for item in base.edges}))
    exit_, _, _ = _topology_apply(base, "insert_exit_region", priority=40, kind="donor_exit", guard={"kind": "always"})
    donors["exit_hub"] = (exit_, next(edge.edge_id for edge in exit_.edges if edge.edge_id not in {item.edge_id for item in base.edges}))
    for port, (donor, edge_id) in donors.items():
        plan = make_crossover_plan(base, donor, segment_map={port: [edge_id]})
        applied = apply_crossover(base, donor, plan)
        application = _clone(applied.semantic_delta)
        application["applicationSha256"] = _hash(application)
        plan_value = plan.canonical() | {"planSha256": plan.identity_sha256, "donorProgramSha256": donor.identity_sha256}
        runtime = _runtime()
        if runtime.short_module is None:
            raise CorpusError("crossover pair oracle lacks its sealed short anchor")
        long_module = _freeze_module(applied.genome, label=f"crossover_{port}_pair_long")
        pair = FrozenPair.compile(
            long=long_module,
            short=runtime.short_module,
            pair_compiler_identity=runtime.bundle.compiler_identity,
            pair_compiler=runtime.bundle.compiler,
            candidate_id=f"v5_oracle_crossover_{port}",
            side_targeted_lineage=[{"side": "long", "operation": "same_side_crossover", "port": port}],
        )
        runtime.pair_compilation_count += 1
        pair_payload = pair.canonical_payload()
        pair_fact = {
            "schemaVersion": pair_payload["schemaVersion"],
            "pairIdentitySha256": pair_payload["identities"]["pairIdentitySha256"],
            "longModulePublicIdentitySha256": long_module.identity_sha256,
            "shortModulePublicIdentitySha256": runtime.short_module.identity_sha256,
            "pairCompilerSha256": pair_payload["pairCompiler"]["sha256"],
            "profile": pair_payload["profile"],
            "validation": pair_payload["validation"],
            "sideTargetedLineage": pair_payload["sideTargetedLineage"],
            "identities": pair_payload["identities"],
        }
        cases.append(_case(
            case_id=f"crossover.{port}", domain="crossover", family=port,
            disposition="accepted", parent=base, plan=plan_value, child=applied.genome,
            application=application,
            audit_extra={
                "orderedDonorProgramSha256": donor.identity_sha256,
                "frozenPair": pair_fact,
            },
        ))
    # Same side/instrument/budget, but the donor setup guard requires a new
    # event resource that the recipient does not own.  The closure must fail
    # before a child can be materialized.
    event_plan = _plan_with_kind(layer, base, "directional_event_insert")
    incompatible, _ = layer.apply(base, event_plan)
    event_id = str(event_plan["construction"]["eventId"])
    nodes = tuple(
        GenomeNodeV1(node.node_id, node.zone, node.kind, {"kind": "fresh_event", "eventId": event_id}, node.resources, node.timeout_bars)
        if node.node_id == "setup" else node
        for node in incompatible.nodes
    )
    incompatible = EvolvableModuleGenomeV1(incompatible.direction, incompatible.resources, nodes, incompatible.edges, incompatible.budget, incompatible.program_kind, incompatible.codec, incompatible.instrument)
    incompatible.validate()
    rejected_plan = {"segmentMap": {"entry_setup": ["start_setup"]}, "donorProgramSha256": incompatible.identity_sha256}
    try:
        make_crossover_plan(base, incompatible, segment_map={"entry_setup": ["start_setup"]})
    except EvolvableGenomeError as exc:
        cases.append(_case(
            case_id="crossover.incompatible_resource_closure", domain="crossover",
            family="incompatible_resource_closure", disposition="rejected", parent=base,
            plan=rejected_plan, error=str(exc),
            audit_extra={"orderedDonorProgramSha256": incompatible.identity_sha256},
        ))
    else:  # pragma: no cover - a safety tripwire for a future permissive implementation
        raise CorpusError("incompatible same-side crossover unexpectedly materialized")
    return cases


def _selection_facts(
    *,
    base: EvolvableModuleGenomeV1,
    resource: GenomeResourceOperatorLayer,
    temporal: GenomeTemporalOperatorLayer,
    native: Mapping[str, Any],
    cases: list[dict[str, Any]],
) -> dict[str, Any]:
    """Capture the exact two-stage operator selection contract as data.

    This mirrors the frozen Python authority rather than assigning weights in
    the Rust-facing fixture.  In particular, resource and temporal plans are
    one ``indicator_learning`` family, and protection's 70/25/5 draw happens
    only after the equal-reachability family draw.
    """

    runtime = _runtime()
    if runtime.selection_pair is None:
        runtime.selection_pair = runtime.authority.factory.create_pair(
            proposal_seed=runtime.stopped_proposal_seed
        )
    selection_pair = runtime.selection_pair
    selection_module = selection_pair.long
    # This is the exact production wrapper projection: no reconstructed
    # resource/temporal/topology wrapper is allowed in the selection oracle.
    ordered = _operation_choices(selection_module, runtime.authority.operator)
    parent = selection_pair.identity_sha256
    resource_rows = [
        row for row in ordered
        if row["kind"] == "indicator_learning"
        and row["plan"].get("operatorId") == "evolvable_resource_v1"
    ]
    temporal_rows = [
        row for row in ordered
        if row["kind"] == "indicator_learning"
        and row["plan"].get("operatorId") == "evolvable_temporal_v1"
    ]
    topology_rows = [row for row in ordered if row["kind"] == "typed_grammar"]
    hold_rows = [row for row in ordered if row["kind"] == "hold"]
    protection_rows = [row for row in ordered if row["kind"] == "initial_protection"]
    transcripts = []
    for seed in ("selection-family-a", "selection-family-b", "selection-family-c", "selection-family-d"):
        selected = _select_operation(seed=seed, parent_identity_sha256=parent, choices=ordered)
        transcripts.append({
            "seed": seed,
            "selectedKind": selected["kind"],
            "selectedSemanticOperationSha256": canonical_sha256(selected.get("plan", selected)),
            "selectedChoiceSha256": canonical_sha256(selected),
            "selectedChoice": _clone(selected),
        })
    by_class: dict[str, list[dict[str, Any]]] = {}
    for row in protection_rows:
        by_class.setdefault(str(row["plan"].get("mutationClass") or ""), []).append(row)
    result = {
        "schemaVersion": "temporal_qd_v5_operator_selection_oracle_v1",
        "parentPairIdentitySha256": parent,
        "highLevelFamilySelection": "uniform_sorted_kind_family_then_uniform_sorted_plan_v1",
        "families": ["indicator_learning", "typed_grammar", "hold", "initial_protection"],
        "indicatorLearning": {
            "composes": ["evolvable_resource_v1", "evolvable_temporal_v1"],
            "selection": "uniform_sorted_plan_v1",
            "resourcePlanCount": len(resource_rows),
            "temporalPlanCount": len(temporal_rows),
        },
        "typedGrammar": {
            "operator": "evolvable_topology_v1",
            "selection": "uniform_sorted_plan_v1",
            "representedOperations": sorted({str(row["plan"]["plan"]["operation"]) for row in topology_rows}),
        },
        "hold": {
            "selection": "uniform_sorted_plan_v1",
            "eligibleKinds": sorted({row["newHold"]["kind"] for row in hold_rows}),
            "terminalNoOp": {"kind": "none", "reason": "hold_policy_already_none"},
        },
        "initialProtection": {
            "selection": "weighted_renormalized_class_then_uniform_sorted_plan_v1",
            "classWeights": {"adjacent": 70, "jump": 25, "kind_switch": 5},
            "eligibleClasses": sorted(key for key in by_class if key),
            "renormalizedWeightTotal": sum({"adjacent": 70, "jump": 25, "kind_switch": 5}[key] for key in by_class if key),
            "eligiblePlanCounts": {key: len(value) for key, value in sorted(by_class.items())},
            "dynamicConstructionPlanCount": sum(1 for row in protection_rows if row["plan"].get("kind") == "dynamic_construction"),
        },
        "prePlanEnumerationFailure": {
            "kind": "typed_grammar",
            "availablePlans": [],
            "disposition": "rejected",
            "reason": "no_eligible_operation",
        },
        "selectionParentFrozenPair": selection_pair.canonical_payload(),
        "selectionParentFrozenPairIdentitySha256": selection_pair.identity_sha256,
        "legacyChoiceOrderingSha256": canonical_sha256(ordered),
        "orderedChoices": _clone(ordered),
        "orderedChoiceSha256s": [canonical_sha256(row) for row in ordered],
        "transcripts": transcripts,
    }
    result["selectionSha256"] = _hash(result)
    return result


def _fresh_current_pair_facts(stopped: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the current-authority pair golden and the historical drift fact."""

    runtime = _runtime()
    if runtime.selection_pair is None:
        raise CorpusError("selection pair must be materialized before pair facts")
    pair = runtime.selection_pair
    payload = pair.canonical_payload()
    current_short_lineage = payload["short"]["lineage"][0]
    recomputed_short = _short_base(stopped)
    current_topology = recomputed_short.semantic_topology_signature()
    historical = {
        "schemaVersion": "temporal_qd_v5_historical_lineage_drift_v1",
        "side": "short",
        "historicalLiteral": {
            "semanticTopologySha256": HISTORICAL_SHORT_TOPOLOGY_SHA256,
            "source": "frozen_dashboard_proposal_journal_generation_0001",
        },
        "currentRecomputation": {
            "stoppedProgramSha256": recomputed_short.identity_sha256,
            "semanticTopologySha256": current_topology,
        },
        "freshCurrentPairRecordedLineage": {
            "semanticTopologySha256": current_short_lineage["semanticTopologySha256"],
            "lineage": _clone(current_short_lineage),
        },
        "assertion": "historical_literal_differs_from_current_python_recomputation",
        "driftDetected": HISTORICAL_SHORT_TOPOLOGY_SHA256 != current_topology,
    }
    if not historical["driftDetected"]:
        raise CorpusError("historical short lineage drift unexpectedly disappeared")
    historical["historicalDriftSha256"] = _hash(historical)
    fresh = {
        "schemaVersion": "temporal_qd_v5_fresh_current_python_pair_v1",
        "proposalSeed": runtime.stopped_proposal_seed,
        "frozenPair": payload,
        "pairIdentitySha256": pair.identity_sha256,
        "factoryAudit": runtime.authority.factory.audit_pair(pair),
    }
    fresh["freshCurrentPairSha256"] = _hash(fresh)
    return fresh, historical


def _real_crossover_projection(
    *, parent: FrozenPair, mate: FrozenPair, proposal_seed: str
) -> dict[str, Any]:
    """Project the real operator's selected port and assert its exact result.

    ``EvolvableModulePairOperator.crossover`` currently returns only a child
    program.  This mirrors its port-selection branch with the same private
    authority selector, then later byte-compares the projection with the real
    public proposal result.  It is therefore evidence about the real operator,
    not a separately accepted synthetic crossover.
    """

    runtime = _runtime()
    side = proposal_side(proposal_seed)
    parent_module = parent.long if side == "long" else parent.short
    mate_module = mate.long if side == "long" else mate.short
    ordered = sorted(
        (parent_module, mate_module),
        key=lambda item: canonical_sha256(
            {"proposalSeed": proposal_seed, "moduleIdentitySha256": item.identity_sha256}
        ),
    )
    recipient, donor = (
        runtime.authority.decode_module(ordered[0]),
        runtime.authority.decode_module(ordered[1]),
    )
    recipient_nodes = {node.node_id: node for node in recipient.nodes}
    donor_nodes = {node.node_id: node for node in donor.nodes}
    eligible: list[tuple[str, list[str]]] = []
    for port, zone in (("entry_setup", "setup"), ("management_hub", "management"), ("exit_hub", "exit")):
        donor_ids = [edge.edge_id for edge in donor.edges if donor_nodes[edge.target_id].zone.value == zone]
        recipient_ids = [edge.edge_id for edge in recipient.edges if recipient_nodes[edge.target_id].zone.value == zone]
        if not donor_ids or not recipient_ids:
            continue
        compatible: list[str] = []
        for edge_id in sorted(donor_ids):
            try:
                make_crossover_plan(recipient, donor, segment_map={port: [edge_id]})
            except EvolvableGenomeError:
                continue
            compatible.append(edge_id)
        if compatible:
            eligible.append((port, compatible))
    if not eligible:
        raise TemporalDiscoveryContractError("real crossover projection has no compatible port")
    port, donor_ids = _authority_choice(proposal_seed, axis="crossover_port", values=eligible)
    donor_edge_id = _authority_choice(proposal_seed, axis="crossover_donor", values=donor_ids)
    plan = make_crossover_plan(recipient, donor, segment_map={port: [donor_edge_id]})
    applied = apply_crossover(recipient, donor, plan)
    application = _clone(applied.semantic_delta)
    application["applicationSha256"] = _hash(application)
    selection = {
        "schemaVersion": "temporal_qd_v5_real_crossover_selection_v1",
        "side": side,
        "orderedParentModuleIdentitySha256": [
            item.identity_sha256 for item in ordered
        ],
        "eligiblePorts": [
            {"port": item_port, "donorEdgeIds": edge_ids}
            for item_port, edge_ids in eligible
        ],
        "selectedPort": port,
        "selectedDonorEdgeId": donor_edge_id,
    }
    selection["selectionSha256"] = _hash(selection)
    result = {
        "schemaVersion": "temporal_qd_v5_real_crossover_projection_v1",
        "side": side,
        # Retain the flattened fields for Rust-facing replay while making the
        # exact two-axis production selection independently hashable.
        "selection": selection,
        "eligiblePorts": selection["eligiblePorts"],
        "selectedPort": selection["selectedPort"],
        "selectedDonorEdgeId": selection["selectedDonorEdgeId"],
        "orderedParentModuleIdentitySha256": selection[
            "orderedParentModuleIdentitySha256"
        ],
        "recipientFrozenModuleIdentitySha256": ordered[0].identity_sha256,
        "donorFrozenModuleIdentitySha256": ordered[1].identity_sha256,
        "recipientProgramSha256": recipient.identity_sha256,
        "donorProgramSha256": donor.identity_sha256,
        "plan": plan.canonical() | {"planSha256": plan.identity_sha256},
        "application": application,
        "childProgram": applied.genome.canonical(),
        "childProgramSha256": applied.genome.identity_sha256,
    }
    result["projectionSha256"] = _hash(result)
    return result


def _authority_transcripts() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Exercise public pair proposals, preserving their real lineage/payload."""

    runtime = _runtime()
    if runtime.selection_pair is None:
        raise CorpusError("selection pair must be materialized before authority transcripts")
    parent = runtime.selection_pair
    rows: list[dict[str, Any]] = []
    sequence_mates: list[tuple[FrozenPair, dict[str, Any]]] = []

    def seal(row: dict[str, Any]) -> dict[str, Any]:
        row["transcriptSha256"] = _hash(row)
        return row

    parent_factory = {
        "schemaVersion": "temporal_qd_v5_pair_factory_input_v1",
        "authority": "PairAuthorityBundle",
        "proposalSeed": runtime.stopped_proposal_seed,
    }
    for depth in (1, 2, 3):
        for ordinal in range(32):
            seed = f"stopped-v5-authority-depth-{depth}-{ordinal}"
            child, proposal = _propose_pair_sequence(
                proposal_seed=seed,
                parent=parent,
                mutation_depth=depth,
                module_authority=runtime.authority.operator,
                native_validator=runtime.bundle.validator,
                pair_compiler=runtime.bundle.compiler,
            )
            if child is not None:
                sequence_row = seal({
                    "schemaVersion": AUTHORITY_TRANSCRIPT_SCHEMA,
                    "evidenceClass": REAL_AUTHORITY_TRANSCRIPT_EVIDENCE,
                    "kind": "proposal_sequence",
                    "mutationDepth": depth,
                    "factoryGeneration": {"parent": _clone(parent_factory)},
                    "parentPair": parent.canonical_payload(),
                    "parentPairIdentitySha256": parent.identity_sha256,
                    "proposal": proposal,
                    "childPair": child.canonical_payload(),
                    "childPairIdentitySha256": child.identity_sha256,
                    "authorityAudit": runtime.authority.factory.audit_pair(child),
                })
                rows.append(sequence_row)
                sequence_mates.append((child, {
                    "schemaVersion": "temporal_qd_v5_pair_mate_origin_v1",
                    "kind": "factory_rooted_proposal_sequence",
                    "rootFactoryInput": _clone(parent_factory),
                    "proposalSeed": seed,
                    "mutationDepth": depth,
                    "sourceProposalSha256": proposal["proposalSha256"],
                    "sourceTranscriptSha256": sequence_row["transcriptSha256"],
                    "pairIdentitySha256": child.identity_sha256,
                }))
                break
        else:
            raise CorpusError(f"no accepted real authority proposal at depth {depth}")

    # Distinct parents are drawn from factory roots and accepted production
    # children only—never from a hand-built layer genome.  The factory-rooted
    # children preserve a compatible recipient resource closure for entry/setup
    # while still being a genuinely distinct pair identity.
    witnesses: dict[str, dict[str, Any]] = {}
    mate_pairs_constructed = 0
    distinct_mate_pairs = 0
    candidate_seeds_examined = 0
    compatible_projection_attempts = 0
    public_production_attempts = 0
    distinct_terminal_attempts = 0
    factory_rooted_mate_candidates = 0

    def capture_from_mate(
        *, mate: FrozenPair, mate_origin: Mapping[str, Any], seed_prefix: str
    ) -> bool:
        """Retain each first materialized real port witness from one mate."""

        nonlocal distinct_mate_pairs
        nonlocal candidate_seeds_examined
        nonlocal compatible_projection_attempts
        nonlocal public_production_attempts
        nonlocal distinct_terminal_attempts
        if mate.identity_sha256 == parent.identity_sha256:
            return set(witnesses) == set(CROSSOVER_PORTS)
        distinct_mate_pairs += 1
        for seed_ordinal in range(DISTINCT_CROSSOVER_SEED_LIMIT):
            seed = f"{seed_prefix}-{seed_ordinal}"
            candidate_seeds_examined += 1
            try:
                projection = _real_crossover_projection(
                    parent=parent, mate=mate, proposal_seed=seed
                )
            except (EvolvableGenomeError, TemporalDiscoveryContractError):
                continue
            port = str(projection["selectedPort"])
            if port in witnesses:
                continue
            compatible_projection_attempts += 1
            try:
                child, proposal = _propose_crossover(
                    proposal_seed=seed,
                    parent=parent,
                    mate=mate,
                    module_authority=runtime.authority.operator,
                    pair_compiler=runtime.bundle.compiler,
                    parent_selection=None,
                    mate_selection=None,
                    mate_selection_attempts=(),
                )
            except (EvolvableGenomeError, TemporalDiscoveryContractError, ValueError, RuntimeError):
                continue
            public_production_attempts += 1
            if child is None:
                distinct_terminal_attempts += 1
                continue
            if proposal.get("disposition") != "materialized":
                raise CorpusError("materialized real crossover has a terminal proposal disposition")
            audit = proposal.get("crossoverAudit")
            if not isinstance(audit, Mapping):
                raise CorpusError("materialized real crossover omitted its audit")
            operation = audit.get("operation") or {}
            child_payload = child.canonical_payload()
            child_program = child_payload[str(projection["side"])]["program"]
            if (
                operation.get("orderedParentModuleIdentitySha256")
                != projection["orderedParentModuleIdentitySha256"]
                or operation.get("childProgramSha256") != projection["childProgramSha256"]
                or child_program != projection["childProgram"]
            ):
                raise CorpusError("real crossover plan projection diverged from authority result")
            witnesses[port] = seal({
                "schemaVersion": AUTHORITY_TRANSCRIPT_SCHEMA,
                "evidenceClass": REAL_AUTHORITY_TRANSCRIPT_EVIDENCE,
                "kind": "same_side_crossover_distinct",
                "port": port,
                "factoryGeneration": {
                    "parent": _clone(parent_factory),
                },
                "mateOrigin": _clone(mate_origin),
                "parentPair": parent.canonical_payload(),
                "parentPairIdentitySha256": parent.identity_sha256,
                "matePair": mate.canonical_payload(),
                "matePairIdentitySha256": mate.identity_sha256,
                "proposalSeed": seed,
                "selection": _clone(projection["selection"]),
                "projection": projection,
                "plan": _clone(projection["plan"]),
                "application": _clone(projection["application"]),
                "proposal": proposal,
                "audit": _clone(audit),
                "child": {
                    "side": projection["side"],
                    "programSha256": projection["childProgramSha256"],
                    "sideModuleIdentitySha256": child_payload[str(projection["side"])]["identities"]["moduleIdentitySha256"],
                    "pairIdentitySha256": child.identity_sha256,
                },
                "childPair": child_payload,
                "childPairIdentitySha256": child.identity_sha256,
                "authorityAudit": runtime.authority.factory.audit_pair(child),
            })
            if set(witnesses) == set(CROSSOVER_PORTS):
                return True
        return False

    # A factory-rooted, accepted production child is the first mate source.
    # It is distinct at pair level while retaining a closure-compatible
    # unchanged side, so entry/setup remains a real authority witness rather
    # than an impossible cross-seed resource import.
    for source_ordinal, (mate, mate_origin) in enumerate(sequence_mates):
        factory_rooted_mate_candidates += 1
        if capture_from_mate(
            mate=mate,
            mate_origin=mate_origin,
            seed_prefix=(
                "stopped-v5-authority-factory-rooted-crossover-"
                f"{source_ordinal}"
            ),
        ):
            break

    # Keep a bounded direct-factory fallback after the closure-preserving
    # factory-rooted mates.  This preserves the stated factory search bound
    # without ever fabricating a synthetic crossover parent.
    if set(witnesses) != set(CROSSOVER_PORTS):
        for mate_ordinal in range(DISTINCT_CROSSOVER_MATE_LIMIT):
            mate_seed = f"stopped-v5-authority-distinct-mate-{mate_ordinal}"
            mate = runtime.authority.factory.create_pair(proposal_seed=mate_seed)
            mate_pairs_constructed += 1
            direct_mate_origin = {
                "schemaVersion": "temporal_qd_v5_pair_mate_origin_v1",
                "kind": "factory_pair",
                "factoryInput": {
                    "schemaVersion": "temporal_qd_v5_pair_factory_input_v1",
                    "authority": "PairAuthorityBundle",
                    "proposalSeed": mate_seed,
                },
                "pairIdentitySha256": mate.identity_sha256,
            }
            if capture_from_mate(
                mate=mate,
                mate_origin=direct_mate_origin,
                seed_prefix=f"stopped-v5-authority-distinct-crossover-{mate_ordinal}",
            ):
                break
    rows.extend(witnesses[port] for port in CROSSOVER_PORTS if port in witnesses)

    # This is the public production crossover wrapper, not a layer-level
    # no-op.  A same-pair attempt is retained only when the wrapper itself
    # classifies it as terminal; no synthetic plan is supplied to force it.
    terminal_row: dict[str, Any] | None = None
    same_pair_terminal_attempt_count = 0
    for ordinal in range(REAL_CROSSOVER_NO_OP_SEED_LIMIT):
        seed = f"stopped-v5-authority-crossover-no-op-{ordinal}"
        same_pair_terminal_attempt_count += 1
        child, proposal = _propose_crossover(
            proposal_seed=seed,
            parent=parent,
            mate=parent,
            module_authority=runtime.authority.operator,
            pair_compiler=runtime.bundle.compiler,
            parent_selection=None,
            mate_selection=None,
            mate_selection_attempts=(),
        )
        if child is None and proposal.get("disposition") in {"no_op_proposal", "operation_rejected"}:
            terminal_row = seal({
                "schemaVersion": AUTHORITY_TRANSCRIPT_SCHEMA,
                "evidenceClass": REAL_AUTHORITY_TRANSCRIPT_EVIDENCE,
                "kind": "same_side_crossover_terminal",
                "terminalDisposition": proposal["disposition"],
                "factoryGeneration": {
                    "parent": _clone(parent_factory),
                },
                "mateOrigin": {
                    "schemaVersion": "temporal_qd_v5_pair_mate_origin_v1",
                    "kind": "factory_pair",
                    "factoryInput": _clone(parent_factory),
                    "pairIdentitySha256": parent.identity_sha256,
                },
                "parentPair": parent.canonical_payload(),
                "parentPairIdentitySha256": parent.identity_sha256,
                "matePair": parent.canonical_payload(),
                "matePairIdentitySha256": parent.identity_sha256,
                "proposalSeed": seed,
                "proposal": proposal,
            })
            rows.append(terminal_row)
            break

    # Independent factory seeds necessarily derive different setup resources.
    # A seed whose selected side has neither a management nor an exit motif
    # therefore has no compatible port against the stopped factory parent.
    # This is a real production rejection search; it never injects a plan or
    # mutates either factory pair by hand.
    factory_terminal_pair_attempt_count = 0
    factory_terminal_production_attempt_count = 0
    factory_terminal_candidate_seed: str | None = None
    if terminal_row is None:
        for mate_ordinal in range(DISTINCT_CROSSOVER_MATE_LIMIT):
            mate_seed = f"stopped-v5-authority-terminal-rejection-mate-{mate_ordinal}"
            no_hub_sides = []
            for side in ("long", "short"):
                side_seed = _side_seed(mate_seed, side)
                management = _authority_choice(
                    side_seed,
                    axis="management_effect",
                    values=(None, "break_even", "tighten_stop", "activate_trailing"),
                )
                include_exit = _authority_choice(
                    side_seed, axis="include_exit", values=(False, True)
                )
                if management is None and include_exit is False:
                    no_hub_sides.append(side)
            if not no_hub_sides:
                continue
            factory_terminal_pair_attempt_count += 1
            mate = runtime.authority.factory.create_pair(proposal_seed=mate_seed)
            factory_terminal_candidate_seed = mate_seed
            mate_origin = {
                "schemaVersion": "temporal_qd_v5_pair_mate_origin_v1",
                "kind": "factory_pair",
                "factoryInput": {
                    "schemaVersion": "temporal_qd_v5_pair_factory_input_v1",
                    "authority": "PairAuthorityBundle",
                    "proposalSeed": mate_seed,
                },
                "pairIdentitySha256": mate.identity_sha256,
            }
            for side in no_hub_sides:
                for seed_ordinal in range(8):
                    seed = (
                        "stopped-v5-authority-crossover-rejection-"
                        f"{mate_ordinal}-{side}-{seed_ordinal}"
                    )
                    if proposal_side(seed) != side:
                        continue
                    factory_terminal_production_attempt_count += 1
                    child, proposal = _propose_crossover(
                        proposal_seed=seed,
                        parent=parent,
                        mate=mate,
                        module_authority=runtime.authority.operator,
                        pair_compiler=runtime.bundle.compiler,
                        parent_selection=None,
                        mate_selection=None,
                        mate_selection_attempts=(),
                    )
                    if child is None and proposal.get("disposition") in {
                        "no_op_proposal", "operation_rejected"
                    }:
                        terminal_row = seal({
                            "schemaVersion": AUTHORITY_TRANSCRIPT_SCHEMA,
                            "evidenceClass": REAL_AUTHORITY_TRANSCRIPT_EVIDENCE,
                            "kind": "same_side_crossover_terminal",
                            "terminalSearchKind": "factory_seeded_no_hub_rejection",
                            "terminalDisposition": proposal["disposition"],
                            "factoryGeneration": {
                                "parent": _clone(parent_factory),
                            },
                            "mateOrigin": mate_origin,
                            "parentPair": parent.canonical_payload(),
                            "parentPairIdentitySha256": parent.identity_sha256,
                            "matePair": mate.canonical_payload(),
                            "matePairIdentitySha256": mate.identity_sha256,
                            "proposalSeed": seed,
                            "proposal": proposal,
                        })
                        rows.append(terminal_row)
                    break
                if terminal_row is not None:
                    break
            if terminal_row is not None:
                break
    search = {
        "schemaVersion": "temporal_qd_v5_real_distinct_crossover_search_v2",
        "evidenceClass": REAL_AUTHORITY_TRANSCRIPT_EVIDENCE,
        "parentFactoryProposalSeed": runtime.stopped_proposal_seed,
        "mateLimit": DISTINCT_CROSSOVER_MATE_LIMIT,
        "seedLimitPerMate": DISTINCT_CROSSOVER_SEED_LIMIT,
        "matePairsConstructed": mate_pairs_constructed,
        "distinctMatePairs": distinct_mate_pairs,
        "factoryRootedProposalMateCandidates": factory_rooted_mate_candidates,
        "candidateSeedsExamined": candidate_seeds_examined,
        "projectionAttemptsForUncapturedPorts": compatible_projection_attempts,
        "publicProductionAttempts": public_production_attempts,
        "distinctTerminalAttempts": distinct_terminal_attempts,
        "capturedPorts": [port for port in CROSSOVER_PORTS if port in witnesses],
        "missingPorts": [port for port in CROSSOVER_PORTS if port not in witnesses],
        "realTerminalNoOpOrRejectionSeed": (
            terminal_row["proposalSeed"] if terminal_row is not None else None
        ),
        "realTerminalNoOpOrRejectionDisposition": (
            terminal_row["terminalDisposition"] if terminal_row is not None else None
        ),
        "samePairTerminalSearchSeedLimit": REAL_CROSSOVER_NO_OP_SEED_LIMIT,
        "samePairTerminalSearchAttemptCount": same_pair_terminal_attempt_count,
        "factoryTerminalMateLimit": DISTINCT_CROSSOVER_MATE_LIMIT,
        "factoryTerminalCandidatePairAttemptCount": factory_terminal_pair_attempt_count,
        "factoryTerminalProductionAttemptCount": factory_terminal_production_attempt_count,
        "factoryTerminalCandidateSeed": factory_terminal_candidate_seed,
        "terminalSearchSeedLimit": REAL_CROSSOVER_NO_OP_SEED_LIMIT,
        "terminalSearchAttemptCount": (
            same_pair_terminal_attempt_count + factory_terminal_production_attempt_count
        ),
    }
    search["searchSha256"] = _hash(search)
    return rows, search


def build_corpus(
    *, stopped_path: Path = DEFAULT_STOPPED, authority_path: Path = DEFAULT_AUTHORITY
) -> dict[str, Any]:
    stopped = _read_json(stopped_path)
    authority = _read_authority(authority_path)
    _verify_fixture_identity(stopped, name="stopped-run")
    _verify_fixture_identity(authority, name="shared-authority")
    global _RUNTIME
    if _RUNTIME is not None:
        raise CorpusError("operator oracle authority cannot be opened recursively")
    bundle = PairAuthorityBundle(authority["authorityInputs"]["pairSourceAuthority"])
    _RUNTIME = _OracleRuntime(
        bundle=bundle,
        authority=bundle.open_evolvable_module_authority(
            authority["authorityInputs"]["evolvableModuleAuthority"]
        ),
        stopped_proposal_seed=str(stopped["construction"]["proposalSeed"]),
    )
    native = authority["authorityInputs"]["nativeOperatorAuthority"]
    base = _base(stopped)
    _runtime().short_module = _freeze_module(_short_base(stopped), label="short_anchor")
    catalog = authority["authorityInputs"]["pairSourceAuthority"]["longModule"]["catalog"]
    resource = GenomeResourceOperatorLayer(catalog)
    temporal = GenomeTemporalOperatorLayer()
    cases: list[dict[str, Any]] = []

    for kind in RESOURCE_KINDS:
        plan = _plan_with_kind(resource, base, kind)
        child, application = resource.apply(base, plan)
        cases.append(_case(
            case_id=f"resource.{kind}", domain="resource", family=kind,
            disposition="accepted", parent=base, plan=plan, child=child, application=application,
        ))

    temporal_parent = _temporal_parent(base, resource)
    by_family: dict[str, dict[str, Any]] = {}
    for plan in temporal.enumerate_plans(temporal_parent):
        by_family.setdefault(str(plan["construction"]["family"]), plan)
    for family in TEMPORAL_FAMILIES:
        plan = by_family[family]
        child, application = temporal.apply(temporal_parent, plan)
        cases.append(_case(
            case_id=f"temporal.{family}", domain="temporal", family=family,
            disposition="accepted", parent=temporal_parent, plan=plan, child=child, application=application,
        ))

    plan_id = str(base.resources.management_refs[0]["id"])
    hold_choices = native["holdOperatorPolicy"]["choices"]
    for kind in ("none", "market_bars", "elapsed_calendar"):
        hold = _clone(next(item for item in hold_choices if item["kind"] == kind))
        child, application = _hold_child(base, plan_id=plan_id, hold=hold)
        cases.append(_case(
            case_id=f"hold.{kind}", domain="hold", family=kind, disposition="accepted",
            parent=base, plan={"managementPlanId": plan_id, "after": hold, "planSha256": _hash({"managementPlanId": plan_id, "after": hold})}, child=child, application=application,
        ))

    profile = _compile(base, label="initial-enumeration")["profile"]
    protection = enumerate_initial_protection_plans(profile, default_initial_protection_policy())
    for mutation_class in ("adjacent", "jump", "kind_switch"):
        plan = next(item for item in protection if item["mutationClass"] == mutation_class)
        child, application = _initial_child(base, plan=plan)
        cases.append(_case(
            case_id=f"initial_protection.{mutation_class}", domain="initial_protection",
            family=mutation_class, disposition="accepted", parent=base, plan=plan,
            child=child, application=application,
        ))
    dynamic_parent_module = _freeze_module(base, label="dynamic_initial_parent")
    dynamic_plan = next(
        item
        for item in _runtime().authority.operator.initial_protection_plans(dynamic_parent_module)
        if item.get("kind") == "dynamic_construction"
    )
    dynamic_child_module, dynamic_application = _runtime().authority.operator.apply_initial_protection(
        dynamic_parent_module, dynamic_plan, candidate_id="v5_oracle_dynamic_initial"
    )
    dynamic_child = _runtime().authority.decode_program(
        dynamic_child_module.canonical_payload()["program"]
    )
    cases.append(_case(
        case_id="initial_protection.dynamic_construction", domain="initial_protection",
        family="dynamic_construction", disposition="accepted", parent=base,
        plan=dynamic_plan, child=dynamic_child, application=dynamic_application,
        audit_extra={
            "dynamicParentModuleIdentitySha256": dynamic_parent_module.identity_sha256,
            "dynamicChildModuleIdentitySha256": dynamic_child_module.identity_sha256,
        },
    ))

    cases.extend(_topology_cases(base))
    cases.extend(_crossover_cases(base, resource))

    # Depth records cover composition rather than a new mutation vocabulary.
    resource_plan = _plan_with_kind(resource, base, "evidence_threshold_mutate")
    depth1, app1 = resource.apply(base, resource_plan)
    cases.append(_case(case_id="sequence.depth_1", domain="sequence", family="accepted_depth", disposition="accepted", parent=base, plan=resource_plan, child=depth1, application=app1, depth=1))
    step2, plan2, app2 = _topology_apply(depth1, "insert_management_region", effect=EffectKind.TIGHTEN_STOP.value, priority=40, kind="depth_two", guard={"kind": "always"})
    cases.append(_case(case_id="sequence.depth_2", domain="sequence", family="accepted_depth", disposition="accepted", parent=depth1, plan=plan2, child=step2, application=app2, depth=2, prerequisites=["sequence.depth_1"]))
    step3, plan3, app3 = _topology_apply(step2, "insert_exit_region", priority=45, kind="depth_three", guard={"kind": "always"})
    cases.append(_case(case_id="sequence.depth_3", domain="sequence", family="accepted_depth", disposition="accepted", parent=step2, plan=plan3, child=step3, application=app3, depth=3, prerequisites=["sequence.depth_1", "sequence.depth_2"]))
    try:
        _topology_apply(depth1, "remove_entry_branch", nodeId="entry")
    except EvolvableGenomeError as exc:
        cases.append(_case(case_id="sequence.intermediate_failure", domain="sequence", family="intermediate_failure", disposition="rejected", parent=depth1, plan={"operation": "remove_entry_branch", "nodeId": "entry"}, error=str(exc), depth=2, prerequisites=["sequence.depth_1"]))
    else:  # pragma: no cover
        raise CorpusError("removing the only entry branch unexpectedly materialized")
    cases.append(_case(
        case_id="topology.no_eligible_preplan", domain="topology",
        family="no_eligible_preplan", disposition="rejected", parent=base,
        plan=None, error="no_eligible_operation",
        audit_extra={"phase": "pre_plan_enumeration", "availablePlanCount": 0},
    ))
    cases.append(_case(case_id="hold.no_op", domain="hold", family="no_op", disposition="no_op", parent=base, plan={"managementPlanId": plan_id, "after": {"kind": "none"}}, error="hold_policy_already_none"))

    coverage = {
        "evidenceClass": SYNTHETIC_PER_FAMILY_EVIDENCE,
        "resourceKinds": list(RESOURCE_KINDS),
        "temporalFamilies": list(TEMPORAL_FAMILIES),
        "holdKinds": ["none", "market_bars", "elapsed_calendar"],
        "initialProtectionMutationClasses": ["adjacent", "jump", "kind_switch"],
        "initialProtectionDynamicConstruction": True,
        "topologyOperations": list(TOPOLOGY_OPERATIONS),
        "crossoverPorts": list(CROSSOVER_PORTS),
        "dispositions": ["accepted", "no_op", "rejected"],
        "mutationDepths": [1, 2, 3],
        "missingRepresentableFamilies": [],
    }
    selection = _selection_facts(
        base=base, resource=resource, temporal=temporal, native=native, cases=cases
    )
    fresh_current_pair, historical_drift = _fresh_current_pair_facts(stopped)
    authority_transcripts, distinct_crossover_search = _authority_transcripts()
    corpus: dict[str, Any] = {
        "schemaVersion": CORPUS_SCHEMA,
        "authority": {
            "stoppedRunFixtureSha256": stopped["fixtureSha256"],
            "sharedAuthorityFixtureSha256": authority["fixtureSha256"],
            "nativeOperatorAuthoritySha256": native["nativeOperatorAuthoritySha256"],
            "sourceOperatorImplementationSha256": native["sourceOperatorImplementationSha256"],
            "temporalDomainsSha256": native["temporalDomains"]["temporalDomainsSha256"],
            "programSha256": base.identity_sha256,
        },
        "coverage": coverage,
        "selection": selection,
        "freshCurrentPythonPair": fresh_current_pair,
        "historicalDrift": historical_drift,
        "authorityCrossoverSearch": distinct_crossover_search,
        "authorityTranscripts": authority_transcripts,
        "cases": cases,
    }
    corpus["corpusSha256"] = _hash(corpus)
    verify_corpus(corpus)
    runtime = _runtime()
    corpus["execution"] = {
        "schemaVersion": "temporal_qd_v5_operator_oracle_execution_v1",
        # These are direct helper invocations only.  Factory/proposal internals
        # may perform additional real validations, so do not present either as
        # a total process-wide count.
        "directFrozenModuleValidationCount": runtime.module_validation_count,
        "directFrozenPairCompilationCount": runtime.pair_compilation_count,
        "persistentJsonlProcessCount": 1 if runtime.bundle.client._persistent_transport is not None else 0,
    }
    corpus["execution"]["executionSha256"] = _hash(corpus["execution"])
    corpus["corpusSha256"] = _hash({key: item for key, item in corpus.items() if key != "corpusSha256"})
    try:
        verify_corpus(corpus)
        return corpus
    finally:
        bundle.close()
        _RUNTIME = None


def verify_corpus(value: Mapping[str, Any]) -> dict[str, Any]:
    corpus = _clone(value)
    supplied = corpus.pop("corpusSha256", None)
    if corpus.get("schemaVersion") != CORPUS_SCHEMA or supplied != _hash(corpus):
        raise CorpusError("operator oracle corpus identity mismatch")
    cases = corpus.get("cases")
    if not isinstance(cases, list) or not cases:
        raise CorpusError("operator oracle corpus has no cases")
    ids: set[str] = set()
    for case in cases:
        if not isinstance(case, dict) or case.get("schemaVersion") != CASE_SCHEMA:
            raise CorpusError("operator oracle case schema drifted")
        if case.get("evidenceClass") != SYNTHETIC_PER_FAMILY_EVIDENCE:
            raise CorpusError("operator oracle synthetic coverage label drifted")
        case_sha = case.get("caseSha256")
        if case_sha != _hash({key: item for key, item in case.items() if key != "caseSha256"}):
            raise CorpusError(f"operator oracle case identity mismatch: {case.get('caseId')}")
        if not isinstance(case.get("caseId"), str) or case["caseId"] in ids:
            raise CorpusError("operator oracle case IDs are not unique")
        ids.add(case["caseId"])
        for endpoint in ("parent", "child"):
            facts = case.get(endpoint)
            if facts is None:
                continue
            frozen = facts.get("frozenModule")
            if not isinstance(frozen, dict) or frozen.get("program") != facts["program"] or frozen.get("profile") != facts["profile"] or frozen.get("nativeReport") != facts["nativeValidation"] or frozen.get("identities") != facts["publicIdentity"]:
                raise CorpusError(f"{case['caseId']} {endpoint} frozen module public surface mismatch")
            snapshots = frozen.get("authoritySnapshotSha256s")
            if not isinstance(snapshots, dict) or set(snapshots) != {"grammarContext", "catalog", "policy", "nativeAuthority"} or not all(isinstance(item, str) and item.startswith("sha256:") for item in snapshots.values()):
                raise CorpusError(f"{case['caseId']} {endpoint} frozen authority snapshot mismatch")
            if facts["publicIdentity"]["programSha256"] != canonical_sha256(facts["program"]):
                raise CorpusError(f"{case['caseId']} {endpoint} program identity mismatch")
            if facts["publicIdentity"]["profileSha256"] != canonical_sha256(facts["profile"]):
                raise CorpusError(f"{case['caseId']} {endpoint} profile identity mismatch")
        audit = case.get("audit")
        if not isinstance(audit, dict) or audit.get("auditSha256") != _hash({key: item for key, item in audit.items() if key != "auditSha256"}):
            raise CorpusError(f"{case['caseId']} audit identity mismatch")
        if "frozenPair" in audit:
            pair = audit["frozenPair"]
            if not isinstance(pair, dict) or pair.get("schemaVersion") != "temporal_bidirectional_pair_snapshot_v1" or not isinstance(pair.get("identities"), dict):
                raise CorpusError(f"{case['caseId']} frozen pair identity mismatch")
    expected = corpus["coverage"]
    if not isinstance(expected, dict) or expected.get("evidenceClass") != SYNTHETIC_PER_FAMILY_EVIDENCE:
        raise CorpusError("operator oracle synthetic coverage label drifted")
    selection = corpus.get("selection")
    if not isinstance(selection, dict) or selection.get("selectionSha256") != _hash(
        {key: item for key, item in selection.items() if key != "selectionSha256"}
    ):
        raise CorpusError("operator oracle selection identity mismatch")
    if selection.get("families") != ["indicator_learning", "typed_grammar", "hold", "initial_protection"]:
        raise CorpusError("operator oracle high-level selection order drifted")
    if selection.get("initialProtection", {}).get("classWeights") != {"adjacent": 70, "jump": 25, "kind_switch": 5}:
        raise CorpusError("operator oracle protection selection weights drifted")
    selection_pair = selection.get("selectionParentFrozenPair")
    if not isinstance(selection_pair, dict) or (
        FrozenPair.from_payload(selection_pair).identity_sha256
        != selection.get("selectionParentFrozenPairIdentitySha256")
    ):
        raise CorpusError("operator oracle selection parent pair drifted")
    if selection.get("parentPairIdentitySha256") != selection.get("selectionParentFrozenPairIdentitySha256"):
        raise CorpusError("operator oracle selection parent identity label drifted")
    fresh = corpus.get("freshCurrentPythonPair")
    if not isinstance(fresh, dict) or fresh.get("freshCurrentPairSha256") != _hash(
        {key: item for key, item in fresh.items() if key != "freshCurrentPairSha256"}
    ):
        raise CorpusError("operator oracle fresh current pair identity mismatch")
    fresh_pair = fresh.get("frozenPair")
    if not isinstance(fresh_pair, dict) or FrozenPair.from_payload(fresh_pair).identity_sha256 != fresh.get("pairIdentitySha256"):
        raise CorpusError("operator oracle fresh current pair payload drifted")
    if fresh.get("pairIdentitySha256") != selection.get("selectionParentFrozenPairIdentitySha256"):
        raise CorpusError("operator oracle selection parent is not the fresh stopped-seed pair")
    drift = corpus.get("historicalDrift")
    if not isinstance(drift, dict) or drift.get("historicalDriftSha256") != _hash(
        {key: item for key, item in drift.items() if key != "historicalDriftSha256"}
    ) or drift.get("historicalLiteral", {}).get("semanticTopologySha256") != HISTORICAL_SHORT_TOPOLOGY_SHA256 or not drift.get("driftDetected"):
        raise CorpusError("operator oracle historical lineage drift fact mismatch")
    if drift.get("currentRecomputation", {}).get("semanticTopologySha256") == HISTORICAL_SHORT_TOPOLOGY_SHA256:
        raise CorpusError("operator oracle historical lineage drift was erased")
    fresh_audit = fresh.get("factoryAudit")
    if not isinstance(fresh_audit, dict) or fresh_audit.get("pairIdentitySha256") != fresh.get("pairIdentitySha256"):
        raise CorpusError("operator oracle fresh current pair audit drifted")
    fresh_short_topology = (((fresh_audit.get("sides") or {}).get("short") or {}).get("semanticTopologySha256"))
    if fresh_short_topology != drift.get("currentRecomputation", {}).get("semanticTopologySha256"):
        raise CorpusError("operator oracle fresh pair is not current-Python recomputation")
    transcripts = corpus.get("authorityTranscripts")
    if not isinstance(transcripts, list):
        raise CorpusError("operator oracle authority transcript matrix drifted")

    def transcript_pair(row: Mapping[str, Any], key: str) -> FrozenPair:
        payload = row.get(key)
        identity = row.get(f"{key}IdentitySha256")
        if not isinstance(payload, dict) or not isinstance(identity, str):
            raise CorpusError("operator oracle authority transcript pair missing")
        try:
            pair = FrozenPair.from_payload(payload)
        except Exception as exc:  # frozen payload parsing is a fail-closed boundary
            raise CorpusError("operator oracle authority transcript pair drifted") from exc
        if pair.identity_sha256 != identity:
            raise CorpusError("operator oracle authority transcript pair identity drifted")
        return pair

    def factory_input_value(value: Any) -> Mapping[str, Any]:
        if (
            not isinstance(value, Mapping)
            or value.get("schemaVersion") != "temporal_qd_v5_pair_factory_input_v1"
            or value.get("authority") != "PairAuthorityBundle"
            or not isinstance(value.get("proposalSeed"), str)
        ):
            raise CorpusError("operator oracle authority factory input drifted")
        return value

    def factory_input(row: Mapping[str, Any], key: str) -> Mapping[str, Any]:
        generation = row.get("factoryGeneration")
        if not isinstance(generation, Mapping):
            raise CorpusError("operator oracle authority factory input missing")
        return factory_input_value(generation.get(key))

    def projection_value(row: Mapping[str, Any]) -> Mapping[str, Any]:
        projection = row.get("projection")
        if (
            not isinstance(projection, Mapping)
            or projection.get("schemaVersion") != "temporal_qd_v5_real_crossover_projection_v1"
            or projection.get("projectionSha256")
            != _hash({key: item for key, item in projection.items() if key != "projectionSha256"})
        ):
            raise CorpusError("operator oracle crossover projection identity drifted")
        selection_value = projection.get("selection")
        if (
            not isinstance(selection_value, Mapping)
            or selection_value.get("schemaVersion")
            != "temporal_qd_v5_real_crossover_selection_v1"
            or selection_value.get("selectionSha256")
            != _hash({key: item for key, item in selection_value.items() if key != "selectionSha256"})
            or projection.get("eligiblePorts") != selection_value.get("eligiblePorts")
            or projection.get("selectedPort") != selection_value.get("selectedPort")
            or projection.get("selectedDonorEdgeId")
            != selection_value.get("selectedDonorEdgeId")
            or projection.get("orderedParentModuleIdentitySha256")
            != selection_value.get("orderedParentModuleIdentitySha256")
        ):
            raise CorpusError("operator oracle crossover selection drifted")
        application = projection.get("application")
        if (
            not isinstance(application, Mapping)
            or application.get("applicationSha256")
            != _hash({key: item for key, item in application.items() if key != "applicationSha256"})
            or projection.get("childProgramSha256")
            != canonical_sha256(projection.get("childProgram"))
        ):
            raise CorpusError("operator oracle crossover application drifted")
        return projection

    def common_real_row(
        row: Mapping[str, Any], *, mate_required: bool, child_required: bool
    ) -> tuple[FrozenPair, FrozenPair | None, FrozenPair | None]:
        if (
            row.get("schemaVersion") != AUTHORITY_TRANSCRIPT_SCHEMA
            or row.get("evidenceClass") != REAL_AUTHORITY_TRANSCRIPT_EVIDENCE
            or row.get("transcriptSha256")
            != _hash({key: item for key, item in row.items() if key != "transcriptSha256"})
        ):
            raise CorpusError("operator oracle authority transcript identity drifted")
        parent_pair = transcript_pair(row, "parentPair")
        parent_input = factory_input(row, "parent")
        if parent_input["proposalSeed"] != fresh.get("proposalSeed"):
            raise CorpusError("operator oracle authority parent factory seed drifted")
        mate_pair = transcript_pair(row, "matePair") if mate_required else None
        child_pair = transcript_pair(row, "childPair") if child_required else None
        return parent_pair, mate_pair, child_pair

    sequence_rows = [
        row for row in transcripts
        if isinstance(row, dict) and row.get("kind") == "proposal_sequence"
    ]
    distinct_rows = [
        row for row in transcripts
        if isinstance(row, dict) and row.get("kind") == "same_side_crossover_distinct"
    ]
    terminal_rows = [
        row for row in transcripts
        if isinstance(row, dict) and row.get("kind") == "same_side_crossover_terminal"
    ]
    if len(sequence_rows) + len(distinct_rows) + len(terminal_rows) != len(transcripts):
        raise CorpusError("operator oracle authority transcript kind drifted")
    if [row.get("mutationDepth") for row in sequence_rows] != [1, 2, 3]:
        raise CorpusError("operator oracle authority transcript matrix drifted")
    if len(terminal_rows) > 1:
        raise CorpusError("operator oracle crossover transcript matrix drifted")
    sequence_by_transcript_sha: dict[str, Mapping[str, Any]] = {}
    for row in sequence_rows:
        parent_pair, _, child_pair = common_real_row(row, mate_required=False, child_required=True)
        proposal = row.get("proposal")
        if (
            not isinstance(proposal, Mapping)
            or proposal.get("disposition") != "materialized"
            or proposal.get("parentPairIdentitySha256") != parent_pair.identity_sha256
            or proposal.get("pairIdentitySha256") != child_pair.identity_sha256
        ):
            raise CorpusError("operator oracle authority sequence proposal drifted")
        transcript_sha = row.get("transcriptSha256")
        if not isinstance(transcript_sha, str) or transcript_sha in sequence_by_transcript_sha:
            raise CorpusError("operator oracle authority sequence transcript identity drifted")
        sequence_by_transcript_sha[transcript_sha] = row

    def mate_origin(row: Mapping[str, Any], mate_pair: FrozenPair) -> Mapping[str, Any]:
        origin = row.get("mateOrigin")
        if (
            not isinstance(origin, Mapping)
            or origin.get("schemaVersion") != "temporal_qd_v5_pair_mate_origin_v1"
            or origin.get("pairIdentitySha256") != mate_pair.identity_sha256
        ):
            raise CorpusError("operator oracle crossover mate origin drifted")
        if origin.get("kind") == "factory_pair":
            factory_input_value(origin.get("factoryInput"))
            return origin
        if origin.get("kind") == "factory_rooted_proposal_sequence":
            root_input = factory_input_value(origin.get("rootFactoryInput"))
            source_sha = origin.get("sourceTranscriptSha256")
            source = sequence_by_transcript_sha.get(source_sha)
            if (
                root_input.get("proposalSeed") != fresh.get("proposalSeed")
                or not isinstance(source_sha, str)
                or source is None
                or origin.get("proposalSeed") != source.get("proposal", {}).get("proposalSeed")
                or origin.get("mutationDepth") != source.get("mutationDepth")
                or origin.get("sourceProposalSha256")
                != source.get("proposal", {}).get("proposalSha256")
                or origin.get("pairIdentitySha256")
                != source.get("childPairIdentitySha256")
            ):
                raise CorpusError("operator oracle factory-rooted mate provenance drifted")
            return origin
        raise CorpusError("operator oracle crossover mate origin kind drifted")

    search = corpus.get("authorityCrossoverSearch")
    if (
        not isinstance(search, Mapping)
        or search.get("schemaVersion") != "temporal_qd_v5_real_distinct_crossover_search_v2"
        or search.get("evidenceClass") != REAL_AUTHORITY_TRANSCRIPT_EVIDENCE
        or search.get("searchSha256")
        != _hash({key: item for key, item in search.items() if key != "searchSha256"})
    ):
        raise CorpusError("operator oracle real crossover search identity drifted")
    captured_ports = search.get("capturedPorts")
    missing_ports = search.get("missingPorts")
    if (
        not isinstance(captured_ports, list)
        or not isinstance(missing_ports, list)
        or captured_ports != [port for port in CROSSOVER_PORTS if port in captured_ports]
        or missing_ports != [port for port in CROSSOVER_PORTS if port not in captured_ports]
    ):
        raise CorpusError("operator oracle real crossover port search drifted")
    if [row.get("port") for row in distinct_rows] != captured_ports:
        raise CorpusError("operator oracle real crossover witness order drifted")
    for row in distinct_rows:
        parent_pair, mate_pair, child_pair = common_real_row(
            row, mate_required=True, child_required=True
        )
        if parent_pair.identity_sha256 == mate_pair.identity_sha256:
            raise CorpusError("operator oracle distinct crossover parents collapsed")
        mate_origin(row, mate_pair)
        projection = projection_value(row)
        audit = row.get("audit")
        proposal = row.get("proposal")
        child_facts = row.get("child")
        side = projection.get("side")
        if (
            row.get("port") not in CROSSOVER_PORTS
            or row.get("port") != projection.get("selectedPort")
            or row.get("selection") != projection.get("selection")
            or row.get("plan") != projection.get("plan")
            or row.get("application") != projection.get("application")
            or not isinstance(audit, Mapping)
            or not isinstance(proposal, Mapping)
            or proposal.get("disposition") != "materialized"
            or proposal.get("proposalSeed") != row.get("proposalSeed")
            or proposal.get("parentPairIdentitySha256") != parent_pair.identity_sha256
            or proposal.get("matePairIdentitySha256") != mate_pair.identity_sha256
            or proposal.get("pairIdentitySha256") != child_pair.identity_sha256
            or proposal.get("pair") != row.get("childPair")
            or proposal.get("crossoverAudit") != audit
            or audit.get("operation", {}).get("orderedParentModuleIdentitySha256")
            != projection.get("orderedParentModuleIdentitySha256")
            or audit.get("operation", {}).get("childProgramSha256")
            != projection.get("childProgramSha256")
            or not isinstance(side, str)
            or row["childPair"][side]["program"] != projection.get("childProgram")
            or not isinstance(child_facts, Mapping)
            or child_facts.get("side") != side
            or child_facts.get("programSha256") != projection.get("childProgramSha256")
            or child_facts.get("pairIdentitySha256") != child_pair.identity_sha256
            or child_facts.get("sideModuleIdentitySha256")
            != row["childPair"][side]["identities"].get("moduleIdentitySha256")
        ):
            raise CorpusError("operator oracle real crossover witness drifted")
    for row in terminal_rows:
        parent_pair, mate_pair, _ = common_real_row(row, mate_required=True, child_required=False)
        origin = mate_origin(row, mate_pair)
        proposal = row.get("proposal")
        if (
            not isinstance(proposal, Mapping)
            or row.get("terminalDisposition") not in {"no_op_proposal", "operation_rejected"}
            or proposal.get("disposition") != row.get("terminalDisposition")
            or proposal.get("parentPairIdentitySha256") != parent_pair.identity_sha256
            or proposal.get("matePairIdentitySha256") != mate_pair.identity_sha256
            or (
                parent_pair.identity_sha256 == mate_pair.identity_sha256
                and (
                    origin.get("kind") != "factory_pair"
                    or factory_input_value(origin.get("factoryInput")).get("proposalSeed")
                    != fresh.get("proposalSeed")
                )
            )
            or (
                parent_pair.identity_sha256 != mate_pair.identity_sha256
                and row.get("terminalSearchKind")
                != "factory_seeded_no_hub_rejection"
            )
        ):
            raise CorpusError("operator oracle terminal crossover witness drifted")
    expected_order = [*sequence_rows, *distinct_rows, *terminal_rows]
    if transcripts != expected_order:
        raise CorpusError("operator oracle authority transcript ordering drifted")
    terminal = terminal_rows[0] if terminal_rows else None
    if (
        search.get("parentFactoryProposalSeed") != fresh.get("proposalSeed")
        or search.get("realTerminalNoOpOrRejectionSeed")
        != (terminal.get("proposalSeed") if terminal is not None else None)
        or search.get("realTerminalNoOpOrRejectionDisposition")
        != (terminal.get("terminalDisposition") if terminal is not None else None)
        or not isinstance(search.get("samePairTerminalSearchAttemptCount"), int)
        or not isinstance(search.get("samePairTerminalSearchSeedLimit"), int)
        or search["samePairTerminalSearchAttemptCount"] < 0
        or search["samePairTerminalSearchAttemptCount"]
        > search["samePairTerminalSearchSeedLimit"]
        or not isinstance(search.get("factoryTerminalCandidatePairAttemptCount"), int)
        or not isinstance(search.get("factoryTerminalProductionAttemptCount"), int)
        or not isinstance(search.get("factoryTerminalMateLimit"), int)
        or search["factoryTerminalCandidatePairAttemptCount"] < 0
        or search["factoryTerminalCandidatePairAttemptCount"]
        > search["factoryTerminalMateLimit"]
        or search["factoryTerminalProductionAttemptCount"] < 0
        or not isinstance(search.get("terminalSearchAttemptCount"), int)
        or not isinstance(search.get("terminalSearchSeedLimit"), int)
        or search["terminalSearchAttemptCount"] < 0
        or search["terminalSearchAttemptCount"]
        != search["samePairTerminalSearchAttemptCount"]
        + search["factoryTerminalProductionAttemptCount"]
    ):
        raise CorpusError("operator oracle terminal crossover search drifted")
    execution = corpus.get("execution")
    if execution is not None and (
        not isinstance(execution, dict)
        or execution.get("executionSha256")
        != _hash({key: item for key, item in execution.items() if key != "executionSha256"})
        or execution.get("persistentJsonlProcessCount") != 1
    ):
        raise CorpusError("operator oracle execution evidence drifted")
    observed = {
        "resourceKinds": {item["family"] for item in cases if item["domain"] == "resource" and item["disposition"] == "accepted"},
        "temporalFamilies": {item["family"] for item in cases if item["domain"] == "temporal" and item["disposition"] == "accepted"},
        "topologyOperations": {item["family"] for item in cases if item["domain"] == "topology" and item["disposition"] == "accepted"},
        "crossoverPorts": {item["family"] for item in cases if item["domain"] == "crossover" and item["disposition"] == "accepted"},
    }
    for key, found in observed.items():
        if found != set(expected[key]):
            raise CorpusError(f"operator oracle coverage drifted: {key}")
    return _clone(value)


def write_corpus(
    path: Path, corpus: Mapping[str, Any], *, replace: bool = False
) -> None:
    payload = gzip.compress(_canonical_bytes(corpus) + b"\n", compresslevel=9, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() != payload and not replace:
        raise CorpusError(f"refusing to overwrite divergent operator oracle corpus: {path}")
    path.write_bytes(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stopped", type=Path, default=DEFAULT_STOPPED)
    parser.add_argument("--authority", type=Path, default=DEFAULT_AUTHORITY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="replace an intentionally superseded generated corpus after verification",
    )
    args = parser.parse_args()
    if args.verify and args.replace:
        raise CorpusError("--verify and --replace cannot be combined")
    corpus = build_corpus(stopped_path=args.stopped, authority_path=args.authority)
    payload = gzip.compress(_canonical_bytes(corpus) + b"\n", compresslevel=9, mtime=0)
    if args.verify:
        if args.output.read_bytes() != payload:
            raise CorpusError("checked-in operator oracle corpus differs from current frozen authority")
    else:
        write_corpus(args.output, corpus, replace=args.replace)
    print(json.dumps({"output": str(args.output), "corpusSha256": corpus["corpusSha256"], "caseCount": len(corpus["cases"])}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CorpusError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
