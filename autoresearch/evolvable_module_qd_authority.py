"""Opt-in QD authority for the evolvable module genome.

The historical typed-fragment pair authority remains the default and is never
rewritten by this module.  This is a separately versioned construction and
mutation authority that compiles a v1 genome to a Dashboard v2 module, then
uses the existing Dashboard-owned v3 pair compiler through ``FrozenPair``.

It deliberately exposes the existing ``TypedPairFactory`` / ``PairModuleOperator``
protocol rather than changing the population journal.  A caller must opt in by
opening this authority and passing ``authority.factory`` and
``authority.operator`` to ``generate_pair_population``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from .evolvable_module_genome import (
    BudgetContractV1,
    CompilerPolicyIdentityV1,
    EffectKind,
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
    Zone,
    decode_program,
)
from .evolvable_module_resource_operators import (
    GenomeResourceOperatorLayer,
    _fuzzy_evidence_contract,
)
from .evolvable_module_temporal_operators import GenomeTemporalOperatorLayer
from .evolvable_module_topology import (
    apply_crossover,
    apply_plan,
    make_crossover_plan,
    make_plan,
)
from .temporal_bidirectional_genome import (
    FrozenModule,
    FrozenPair,
    IdentitySnapshot,
    canonical_hold,
    canonical_json,
    canonical_sha256,
)
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_initial_protection import enumerate_initial_protection_plans


EVOLVABLE_QD_AUTHORITY_SCHEMA = "temporal_qd_evolvable_module_authority_v1"
EVOLVABLE_QD_OPERATOR_SCHEMA = "temporal_qd_evolvable_module_operator_registry_v1"
EVOLVABLE_QD_CAPACITY_SCHEMA = "temporal_qd_evolvable_module_capacity_contract_v1"
EVOLVABLE_QD_FACTORY_SCHEMA = "temporal_qd_evolvable_module_factory_v1"
EVOLVABLE_QD_AUDIT_SCHEMA = "temporal_qd_evolvable_module_factory_audit_v1"
EVOLVABLE_QD_BEHAVIOR_ATTRIBUTION_SCHEMA = "temporal_qd_behavior_attribution_requirement_v1"
EVOLVABLE_QD_CAPACITY_RECEIPT_SCHEMA = "temporal_qd_evolvable_module_capacity_receipt_v1"


def _authority_config_sha256(config: Mapping[str, Any]) -> str:
    """Identity of the executable authority, excluding its evidence attachment.

    A capacity receipt is an admission *witness* for this authority, not an
    input to candidate construction.  Including the receipt in the authority
    hash would make binding it circular: the receipt must name an authority,
    while adding it would create a different authority.  Its exact contents
    are still validated on open and sealed into fresh generation bindings.
    """

    return canonical_sha256({
        key: value
        for key, value in config.items()
        if key not in {"authoritySha256", "capacityReceipt"}
    })


def evolvable_behavior_attribution_requirement() -> dict[str, Any]:
    """The v5-only observer contract required for every evaluated window.

    It is an observation requirement, not an economic scoring knob.  Its
    identity is nevertheless task-bound so a resume cannot silently turn off
    member/branch attribution or restore a checkpoint with incompatible
    observer state.
    """

    result = {
        "schemaVersion": EVOLVABLE_QD_BEHAVIOR_ATTRIBUTION_SCHEMA,
        "observerSchema": "temporal_candidate_behavior_attribution_v1",
        "required": True,
        "fuzzyMemberAttribution": "required_fail_closed_v1",
        "checkpointRestore": "required_exact_v1",
        "taskIdentityBinding": "required_v5_candidate_window_v1",
    }
    result["requirementSha256"] = canonical_sha256(result)
    return result


def _clone(value: Any, *, name: str = "value") -> Any:
    try:
        return __import__("json").loads(canonical_json(_thaw(value)))
    except Exception as exc:  # canonical_json gives the semantic error below
        raise TemporalDiscoveryContractError(f"{name} must be finite canonical JSON") from exc


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _token(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not token or len(token) > 240:
        raise TemporalDiscoveryContractError(f"{name} must be a nonempty explicit identifier")
    return token


def _choice(seed: str, *, axis: str, values: Sequence[Any]) -> Any:
    """One deterministic unbiased finite selection without ambient PRNG state."""

    if not values:
        raise TemporalDiscoveryContractError(f"evolvable module selection axis is empty: {axis}")
    ordered = list(values)
    limit = (1 << 256) - ((1 << 256) % len(ordered))
    nonce = 0
    while True:
        raw = int(canonical_sha256({"schemaVersion": EVOLVABLE_QD_FACTORY_SCHEMA, "seed": seed, "axis": axis, "nonce": nonce})[7:], 16)
        if raw < limit:
            return ordered[raw % len(ordered)]
        nonce += 1


def _side_seed(seed: str, side: str) -> str:
    return canonical_sha256({"schemaVersion": EVOLVABLE_QD_FACTORY_SCHEMA, "proposalSeed": str(seed), "side": side})


def default_evolvable_module_capacity_contract() -> dict[str, Any]:
    return {
        "schemaVersion": EVOLVABLE_QD_CAPACITY_SCHEMA,
        "previewStreamSize": 8192,
        "minimumUniquePairs": 4096,
        "minimumUniqueTopologiesPerSide": 8,
        "minimumUniqueResourceFingerprintsPerSide": 64,
        "requiredDirections": ["long", "short"],
        "admission": "no_market_native_v2_and_compiled_v3_v1",
    }


def build_evolvable_module_authority_config(
    *,
    pair_run_config_sha256: str,
    catalog_sha256: str,
    budget: BudgetContractV1 | None = None,
    capacity_contract: Mapping[str, Any] | None = None,
    archive_policy_authority: Mapping[str, Any] | None = None,
    capacity_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the only new authority record accepted by this module.

    The legacy frozen pair run config stays outside this record, referenced by
    its exact hash.  That makes a new campaign authority explicit and prevents
    old campaign/config hashes from changing merely because this code exists.
    """

    pair_sha = _token(pair_run_config_sha256, name="pair run config SHA-256")
    catalog_sha = _token(catalog_sha256, name="catalog SHA-256")
    if not pair_sha.startswith("sha256:") or not catalog_sha.startswith("sha256:"):
        raise TemporalDiscoveryContractError("evolvable authority inputs must be canonical SHA-256 identities")
    compiler = CompilerPolicyIdentityV1()
    if archive_policy_authority is None:
        # Import at the construction seam so the old pair factory retains no
        # dependency on the direction-aware archive implementation.
        from .temporal_qd_evolution import directional_qd_archive_policy_authority

        archive_policy_authority = directional_qd_archive_policy_authority()
    # The registry identity is fully closed below when the authority is opened
    # against the actual frozen catalog.  Do not fabricate a catalog-dependent
    # resource spec at config-authoring time.
    config = {
        "schemaVersion": EVOLVABLE_QD_AUTHORITY_SCHEMA,
        "programKind": "evolvable_module_genome_v1",
        "codec": "evolvable_module_genome_json_v1",
        "pairRunConfigSha256": pair_sha,
        "catalogSha256": catalog_sha,
        "compilerPolicy": compiler.canonical(),
        "compilerPolicySha256": compiler.sha256,
        "budget": (budget or BudgetContractV1()).canonical(),
        "capacityContract": _clone(
            default_evolvable_module_capacity_contract() if capacity_contract is None else capacity_contract,
            name="capacity contract",
        ),
        "archivePolicyAuthority": _clone(
            archive_policy_authority, name="direction-aware archive policy authority"
        ),
        "behaviorAttributionRequirement": evolvable_behavior_attribution_requirement(),
        "operatorRegistry": {
            "schemaVersion": EVOLVABLE_QD_OPERATOR_SCHEMA,
            "resourceOperators": "evolvable_module_resource_operators_v1",
            "temporalOperators": "evolvable_module_temporal_operators_v1",
            "topologyOperators": "evolvable_module_topology_operator_v1",
            "crossover": "evolvable_module_compatible_motif_crossover_v1",
            "crossoverPorts": ["entry_setup", "management_hub", "exit_hub"],
            "crossoverResourceTransfer": "recipient_closed_only_no_implicit_import_v1",
            "selection": "sha256_length_prefixed_rejection_uniform_v1",
            "replay": "content_bound_plan_then_native_compile_v1",
        },
    }
    if capacity_receipt is not None:
        config["capacityReceipt"] = _clone(capacity_receipt, name="capacity receipt")
    config["authoritySha256"] = _authority_config_sha256(config)
    return config


def _validate_authority_config(value: Mapping[str, Any], *, pair_run_config_sha256: str, catalog_sha256: str) -> dict[str, Any]:
    raw = _clone(value, name="evolvable module authority config")
    supplied = raw.pop("authoritySha256", None)
    expected = build_evolvable_module_authority_config(
        pair_run_config_sha256=pair_run_config_sha256,
        catalog_sha256=catalog_sha256,
        budget=BudgetContractV1(**{
            "max_states": raw.get("budget", {}).get("maxStates"),
            "max_transitions": raw.get("budget", {}).get("maxTransitions"),
            "max_evidence_groups": raw.get("budget", {}).get("maxEvidenceGroups"),
            "max_group_members": raw.get("budget", {}).get("maxGroupMembers"),
            "max_events": raw.get("budget", {}).get("maxEvents"),
            "max_indicators": raw.get("budget", {}).get("maxIndicators"),
            "max_entry_branches": raw.get("budget", {}).get("maxEntryBranches"),
            "max_management_regions": raw.get("budget", {}).get("maxManagementRegions"),
            "max_exit_regions": raw.get("budget", {}).get("maxExitRegions"),
            "max_recovery_regions": raw.get("budget", {}).get("maxRecoveryRegions"),
            "max_scc_nodes": raw.get("budget", {}).get("maxSccNodes"),
            "max_timeout_bars": raw.get("budget", {}).get("maxTimeoutBars"),
            "max_guard_depth": raw.get("budget", {}).get("maxGuardDepth"),
        }),
        capacity_contract=raw.get("capacityContract"),
        archive_policy_authority=raw.get("archivePolicyAuthority"),
        capacity_receipt=raw.get("capacityReceipt"),
    )
    if raw != {key: item for key, item in expected.items() if key != "authoritySha256"} or supplied != expected["authoritySha256"]:
        raise TemporalDiscoveryContractError("evolvable module authority config identity or policy drifted")
    capacity = raw["capacityContract"]
    if (
        not isinstance(capacity, Mapping)
        or capacity.get("schemaVersion") != EVOLVABLE_QD_CAPACITY_SCHEMA
        or not isinstance(capacity.get("previewStreamSize"), int)
        or not isinstance(capacity.get("minimumUniquePairs"), int)
        or capacity["minimumUniquePairs"] < 1
        or capacity["previewStreamSize"] < capacity["minimumUniquePairs"]
        or capacity.get("admission") != "no_market_native_v2_and_compiled_v3_v1"
    ):
        raise TemporalDiscoveryContractError("evolvable module capacity contract is invalid")
    from .temporal_qd_evolution import directional_qd_archive_policy_authority

    if raw.get("archivePolicyAuthority") != directional_qd_archive_policy_authority():
        raise TemporalDiscoveryContractError(
            "evolvable module authority requires the exact v5 direction-aware archive policy"
        )
    return expected


def _side_context(bundle: Any, side: str) -> dict[str, Any]:
    source = bundle.config.get(f"{side}Module")
    if not isinstance(source, Mapping) or not isinstance(source.get("context"), Mapping):
        raise TemporalDiscoveryContractError("frozen pair run config lacks side construction context")
    return _clone(source["context"], name="frozen side construction context")


def _side_catalog(bundle: Any, side: str) -> dict[str, Any]:
    source = bundle.config.get(f"{side}Module")
    if not isinstance(source, Mapping) or not isinstance(source.get("catalog"), Mapping):
        raise TemporalDiscoveryContractError("frozen pair run config lacks side catalog")
    return _clone(source["catalog"], name="frozen side catalog")


def _owned(row: Mapping[str, Any], side: str) -> dict[str, Any]:
    result = _clone(row, name="frozen module resource")
    result["ownerSide"] = side
    return result


def _indicator_id(row: Mapping[str, Any]) -> str:
    meta = row.get("meta") if isinstance(row.get("meta"), Mapping) else {}
    return _token(meta.get("instanceId"), name="indicator instance ID")


def _fingerprint(genome: EvolvableModuleGenomeV1) -> str:
    return canonical_sha256({
        "indicators": [((row.get("meta") or {}).get("id"), (row.get("config") or {}).get("timeframe")) for row in genome.resources.canonical()["indicators"]],
        "groups": [row.get("indicatorInstanceIds") for row in genome.resources.canonical()["evidenceGroups"]],
        "events": [row.get("indicatorInstanceId") for row in genome.resources.canonical()["events"]],
        "management": [edge.effect.value for edge in genome.edges if edge.effect in {EffectKind.BREAK_EVEN, EffectKind.TIGHTEN_STOP, EffectKind.ACTIVATE_TRAILING, EffectKind.DEACTIVATE_TRAILING, EffectKind.SET_TARGET, EffectKind.CANCEL_TARGET}],
        "exits": sum(1 for edge in genome.edges if edge.effect is EffectKind.EXIT),
    })


def _admission_rejection_reason(exc: Exception) -> str:
    """Stable bounded categories; never persist environment-specific stderr."""

    message = str(exc).lower()
    if "entry decision route" in message or "indicator cap" in message:
        return "entry_route_indicator_cap"
    if "schema" in message or "native module report" in message:
        return "native_schema_or_admission"
    if "cross-side" in message or "side" in message and "drift" in message:
        return "cross_side_or_authority_drift"
    if "budget" in message or "exceeds" in message:
        return "budget_or_compiler_cap"
    if "pair" in message or "compiler" in message:
        return "v3_pair_compilation"
    return "other_admission_rejection"


@dataclass(frozen=True)
class PreviewPairV1:
    long: EvolvableModuleGenomeV1
    short: EvolvableModuleGenomeV1
    selectors: Mapping[str, Mapping[str, Any]]

    @property
    def semantic_sha256(self) -> str:
        return canonical_sha256({"schemaVersion": "temporal_qd_evolvable_module_pair_semantics_v1", "long": self.long.identity_sha256, "short": self.short.identity_sha256})


class EvolvableModulePairFactory:
    """Deterministic catalog-bound seed factory, with native admission at freeze."""

    def __init__(self, authority: "EvolvableModulePairAuthority") -> None:
        self.authority = authority
        self.construction_policy = {
            "schemaVersion": EVOLVABLE_QD_FACTORY_SCHEMA,
            "authoritySha256": authority.config["authoritySha256"],
            "seedPolicy": "independent_symmetric_sides_catalog_bound_v1",
            "programKind": authority.config["programKind"],
            "codec": authority.config["codec"],
            # The live pair-population generator applies this fail-closed
            # acceptance tripwire to every immigrant factory.  Keep it inside
            # the authority-bound construction policy so the capacity receipt,
            # generation config, journal, and restart identity all agree on the
            # exact collapse threshold used at runtime.
            "collisionTripwire": {
                "minimumImmigrantAttempts": 512,
                "minimumAcceptedRatio": 0.25,
            },
        }

    def _build_side(self, side: str, proposal_seed: str) -> tuple[EvolvableModuleGenomeV1, dict[str, Any]]:
        context = _side_context(self.authority.bundle, side)
        seed = _side_seed(proposal_seed, side)
        indicators = [item for item in context.get("indicators") or [] if isinstance(item, Mapping)]
        events = [item for item in context.get("events") or [] if isinstance(item, Mapping)]
        plans = (((context.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
        if not indicators or not plans:
            raise TemporalDiscoveryContractError("evolvable module factory requires frozen indicators and management plans")
        event_indicator_ids = {str(event.get("indicatorInstanceId") or "") for event in events}
        states = [
            item for item in indicators
            if _indicator_id(item) not in event_indicator_ids
            and _fuzzy_evidence_contract((item.get("meta") or {})) is not None
        ]
        if not states:
            raise TemporalDiscoveryContractError(
                "frozen side context has no catalog-proven fuzzy evidence indicator"
            )
        selected_event = _choice(
            seed, axis="event", values=[None, *sorted(events, key=lambda item: str(item.get("id")))]
        )
        # The legacy entry-route cap remains a hard native admission rule:
        # state evidence plus fresh event can consume at most three indicators.
        state_cap = min(3 - (1 if selected_event is not None else 0), len(states))
        contracts: dict[str, list[Mapping[str, Any]]] = {}
        for item in states:
            contract = _fuzzy_evidence_contract((item.get("meta") or {}))
            contracts.setdefault(canonical_sha256(contract), []).append(item)
        selected_contract = _choice(seed, axis="fuzzy_contract", values=sorted(contracts))
        compatible_states = sorted(contracts[selected_contract], key=_indicator_id)
        state_cap = min(state_cap, len(compatible_states))
        count = int(_choice(seed, axis="state_count", values=tuple(range(1, state_cap + 1))))
        # Independent axes produce ordered selections; canonical resources are
        # later sorted, while the chosen set remains semantic rather than UID-only.
        selected_states: list[Mapping[str, Any]] = []
        available = compatible_states
        for ordinal in range(count):
            selected = _choice(seed, axis=f"state_{ordinal}", values=available)
            selected_states.append(selected)
        dedup_states = { _indicator_id(item): item for item in selected_states }
        selected_states = [dedup_states[key] for key in sorted(dedup_states)]
        event_indicator = None
        if selected_event is not None:
            event_indicator = next((item for item in indicators if _indicator_id(item) == str(selected_event.get("indicatorInstanceId"))), None)
            if event_indicator is None:
                raise TemporalDiscoveryContractError("frozen event binding has no frozen indicator instance")
        all_indicators = { _indicator_id(item): _owned(item, side) for item in selected_states }
        if event_indicator is not None:
            all_indicators[_indicator_id(event_indicator)] = _owned(event_indicator, side)
        group_id = "g_" + canonical_sha256({"seed": seed, "side": side, "resource": "group"})[7:19]
        group = {
            "id": group_id,
            "indicatorInstanceIds": sorted(_indicator_id(item) for item in selected_states),
            "ownerSide": side,
        }
        event_row = None
        if selected_event is not None:
            event_row = _owned(selected_event, side)
            event_row["id"] = "e_" + canonical_sha256({"seed": seed, "side": side, "resource": "event"})[7:19]
        management = _clone(_choice(seed, axis="management_plan", values=sorted(plans, key=lambda item: str(item.get("id")))), name="management plan")
        management["id"] = "m_" + canonical_sha256({"seed": seed, "side": side, "resource": "management"})[7:19]
        management["ownerSide"] = side
        threshold = float(_choice(seed, axis="threshold", values=(45.0, 50.0, 55.0, 60.0, 65.0, 70.0)))
        uses = [ResourceUse(ResourceKind.EVIDENCE_GROUP, group_id)]
        guards: list[dict[str, Any]] = [{"kind": "evidence_at_least", "groupId": group_id, "thresholdPercent": threshold}]
        if event_row is not None:
            uses.append(ResourceUse(ResourceKind.EVENT, event_row["id"]))
            guards.append({"kind": "fresh_event", "eventId": event_row["id"]})
        # Seed a native session site on a deterministic subset. The temporal
        # operator, rather than this factory, owns the full bounded session
        # domain and all later temporal grain rewrites.
        include_session = bool(_choice(seed, axis="seed_session_filter", values=(False, True)))
        if include_session:
            guards.append({"kind": "utc_time_window", "startMinute": 0, "endMinute": 1439, "weekdays": [0, 1, 2, 3, 4, 5, 6]})
        setup_guard: dict[str, Any] = guards[0] if len(guards) == 1 else {"kind": "all", "guards": guards}
        start = GenomeNodeV1("start", Zone.ENTRY, "start", {"kind": "position_exists", "expected": False})
        setup = GenomeNodeV1("setup", Zone.SETUP, "setup", setup_guard, tuple(uses))
        entry = GenomeNodeV1("entry", Zone.ENTRY, "entry", resources=(ResourceUse(ResourceKind.MANAGEMENT_REF, management["id"]),))
        hub = GenomeNodeV1("hub", Zone.POSITION, "position_hub")
        nodes: list[GenomeNodeV1] = [start, setup, entry, hub]
        edges: list[GenomeEdgeV1] = [
            GenomeEdgeV1("start_setup", "start", "setup", priority=10),
            GenomeEdgeV1("setup_entry", "setup", "entry", priority=10, effect=EffectKind.ENTER),
            GenomeEdgeV1("entry_hub", "entry", "hub", priority=10),
        ]
        # Seed meaningful management/exit variability without changing the
        # one-position runtime.  State-age guards prevent immediate churn.
        management_effects = (EffectKind.BREAK_EVEN, EffectKind.TIGHTEN_STOP, EffectKind.ACTIVATE_TRAILING)
        selected_management = _choice(seed, axis="management_effect", values=(None, *management_effects))
        if selected_management is not None:
            node = GenomeNodeV1("manage", Zone.MANAGEMENT, selected_management.value, {"kind": "state_age_at_least", "events": int(_choice(seed, axis="management_age", values=(3, 6, 12, 24)))})
            nodes.append(node)
            edges.append(GenomeEdgeV1("hub_manage", "hub", node.node_id, priority=20, guard={"kind": "always"}, effect=selected_management))
        include_exit = bool(_choice(seed, axis="include_exit", values=(False, True)))
        if include_exit:
            node = GenomeNodeV1("exit", Zone.EXIT, "timed_exit", {"kind": "state_age_at_least", "events": int(_choice(seed, axis="exit_age", values=(12, 24, 48, 96)))})
            nodes.append(node)
            edges.append(GenomeEdgeV1("hub_exit", "hub", node.node_id, priority=30, guard={"kind": "always"}, effect=EffectKind.EXIT))
        genome = EvolvableModuleGenomeV1(
            direction=side,
            resources=ResourcePoolV1(
                indicators=tuple(all_indicators.values()),
                evidence_groups=(group,),
                events=() if event_row is None else (event_row,),
                management_refs=(management,),
            ),
            nodes=tuple(nodes),
            edges=tuple(edges),
            budget=self.authority.budget,
            instrument=str(context.get("instrument") or "EURUSD"),
        )
        genome.validate()
        return genome, {
            "side": side,
            "stateCount": len(selected_states),
            "stateIndicatorIds": [str((item.get("meta") or {}).get("id")) for item in selected_states],
            "eventBindingId": None if event_row is None else event_row["id"],
            "thresholdPercent": threshold,
            "hasSessionFilter": include_session,
            "managementEffect": None if selected_management is None else selected_management.value,
            "hasExit": include_exit,
            "resourceFingerprintSha256": _fingerprint(genome),
            "semanticTopologySha256": genome.semantic_topology_signature(),
            "genomeSha256": genome.identity_sha256,
        }

    def preview_pair(self, *, proposal_seed: str) -> PreviewPairV1:
        long, long_selector = self._build_side("long", proposal_seed)
        short, short_selector = self._build_side("short", proposal_seed)
        return PreviewPairV1(long=long, short=short, selectors={"long": long_selector, "short": short_selector})

    def _freeze(self, genome: EvolvableModuleGenomeV1, *, proposal_seed: str) -> FrozenModule:
        candidate_id = "qd_evolvable_module_" + canonical_sha256({"seed": proposal_seed, "side": genome.direction, "genome": genome.identity_sha256})[7:35]
        compiled = self.authority.compiler.compile(genome, candidate_id=candidate_id, native_validator=self.authority.bundle.validator)
        return FrozenModule.freeze(
            program=genome.canonical(),
            profile=compiled["profile"],
            grammar_context=self.authority.grammar_context(genome.direction),
            catalog=self.authority.catalog_identity(genome.direction),
            policy=self.authority.module_policy(genome.direction),
            native_authority=self.authority.bundle.native_identity,
            native_report=compiled["nativeValidation"],
            lineage=[{
                "operation": "evolvable_module_seed",
                "side": genome.direction,
                "proposalSeed": str(proposal_seed),
                "programKind": genome.program_kind,
                "codec": genome.codec,
                "compilerPolicySha256": compiled["compilerPolicySha256"],
                "genomeSha256": genome.identity_sha256,
                "semanticTopologySha256": genome.semantic_topology_signature(),
            }],
        )

    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        preview = self.preview_pair(proposal_seed=proposal_seed)
        long, short = self._freeze(preview.long, proposal_seed=proposal_seed), self._freeze(preview.short, proposal_seed=proposal_seed)
        lineage = [
            {
                "operation": "evolvable_module_pair_seed",
                "side": side,
                "proposalSeed": str(proposal_seed),
                "programKind": self.authority.config["programKind"],
                "codec": self.authority.config["codec"],
                "authoritySha256": self.authority.config["authoritySha256"],
                "compilerPolicySha256": self.authority.compiler.policy.sha256,
                "genomeSha256": genome.identity_sha256,
            }
            for side, genome in (("long", preview.long), ("short", preview.short))
        ]
        return FrozenPair.compile(
            long=long,
            short=short,
            pair_compiler_identity=self.authority.bundle.compiler_identity,
            pair_compiler=self.authority.bundle.compiler,
            candidate_id="qd_evolvable_pair_" + canonical_sha256({"seed": proposal_seed})[7:35],
            side_targeted_lineage=lineage,
        )

    def audit_pair(self, pair: FrozenPair) -> dict[str, Any]:
        sides: dict[str, Any] = {}
        for module in (pair.long, pair.short):
            genome = self.authority.decode_module(module)
            sides[module.direction] = {
                "programKind": genome.program_kind,
                "codec": genome.codec,
                "genomeSha256": genome.identity_sha256,
                "semanticTopologySha256": genome.semantic_topology_signature(),
                "resourceFingerprintSha256": _fingerprint(genome),
            }
        result = {"schemaVersion": EVOLVABLE_QD_AUDIT_SCHEMA, "authoritySha256": self.authority.config["authoritySha256"], "pairIdentitySha256": pair.identity_sha256, "sides": sides}
        result["auditSha256"] = canonical_sha256(result)
        return result


class EvolvableModulePairOperator:
    """Adapter from content-bound v1 resource/topology plans to pair protocol."""

    def __init__(self, authority: "EvolvableModulePairAuthority") -> None:
        self.authority = authority

    def _genome(self, module: FrozenModule) -> EvolvableModuleGenomeV1:
        return self.authority.decode_module(module)

    def _freeze(self, template: FrozenModule, genome: EvolvableModuleGenomeV1, *, candidate_id: str, lineage: Mapping[str, Any]) -> FrozenModule:
        if genome.direction != template.direction:
            raise TemporalDiscoveryContractError("evolvable module operator attempted cross-side mutation")
        compiled = self.authority.compiler.compile(genome, candidate_id=candidate_id, native_validator=self.authority.bundle.validator)
        return FrozenModule.freeze(
            program=genome.canonical(), profile=compiled["profile"],
            grammar_context=template.grammar_context, catalog=template.catalog, policy=template.policy,
            native_authority=template.native_authority, native_report=compiled["nativeValidation"],
            lineage=[*[_clone(item) for item in template.lineage], _clone(lineage)],
        )

    @staticmethod
    def _replace_management_ref(
        genome: EvolvableModuleGenomeV1,
        *,
        management_ref_id: str,
        replacement: Mapping[str, Any],
    ) -> EvolvableModuleGenomeV1:
        """Apply a management gene to the genotype, never a detached profile."""

        current = genome.resources.mapping(ResourceKind.MANAGEMENT_REF)
        if management_ref_id not in current:
            raise TemporalDiscoveryContractError("management mutation names an unknown genome reference")
        rows = []
        for identifier, row in current.items():
            rows.append(_clone(replacement) if identifier == management_ref_id else _clone(row))
        child = EvolvableModuleGenomeV1(
            direction=genome.direction,
            instrument=genome.instrument,
            resources=ResourcePoolV1(
                indicators=genome.resources.indicators,
                evidence_groups=genome.resources.evidence_groups,
                events=genome.resources.events,
                management_refs=tuple(rows),
            ),
            nodes=genome.nodes,
            edges=genome.edges,
            budget=genome.budget,
        )
        child.validate()
        return child

    def grammar_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        genome = self._genome(module)
        plans: dict[str, dict[str, Any]] = {}
        def add(operation: str, **arguments: Any) -> None:
            try:
                plan = make_plan(genome, operation=operation, **arguments)
                result = apply_plan(genome, plan)
                # compile-through admission is mandatory, not merely graph valid.
                self.authority.compiler.compile(result.genome, candidate_id="qd_evolvable_topology_preview_" + plan.identity_sha256[7:23])
                wrapped = {"operatorId": "evolvable_topology_v1", "plan": plan.canonical(), "planSha256": plan.identity_sha256}
                plans[wrapped["planSha256"]] = wrapped
            except (EvolvableGenomeError, TemporalDiscoveryContractError):
                return
        # Structural grammar deliberately has no temporal fact grid. The
        # dedicated temporal layer owns the exact Dashboard-native domains and
        # their content-addressed rewrites; topology only supplies a neutral
        # placement guard.
        guard_domain: list[Mapping[str, Any]] = [{"kind": "always"}]
        for edge in genome.edges:
            if edge.source_id in {node.node_id for node in genome.nodes if node.zone in {Zone.ENTRY, Zone.SETUP}} and edge.effect is None and edge.target_id != "hub":
                for guard in guard_domain:
                    add("insert_setup", edgeId=edge.edge_id, kind="context", guard=guard)
        nodes = {node.node_id: node for node in genome.nodes}
        management_ref = sorted(genome.resources.mapping(ResourceKind.MANAGEMENT_REF))[0]
        enter_edges = [edge for edge in genome.edges if nodes[edge.target_id].zone is Zone.ENTRY and nodes[edge.target_id].kind == "entry" and edge.effect is EffectKind.ENTER]
        # A semantic entry node has no executable state of its own; only the
        # authored start/setup states can originate an additional enter branch.
        pre_position_sources = [
            node.node_id
            for node in genome.nodes
            if node.zone is Zone.SETUP or (node.zone is Zone.ENTRY and node.kind == "start")
        ]
        for edge in enter_edges:
            for guard in guard_domain:
                add("rewire_entry_branch", edgeId=edge.edge_id, sourceId=edge.source_id, priority=edge.priority, guard=guard)
                add("insert_confirmation_rejection", edgeId=edge.edge_id, rejectPriority=edge.priority + 10, rejectionTimeoutBars=6, confirmGuard=guard, rejectGuard={}, sourceRejectGuard={"kind": "not", "guard": guard})
        if len(enter_edges) < genome.budget.max_entry_branches:
            for source_id in pre_position_sources:
                for guard in guard_domain:
                    add("insert_entry_branch", sourceId=source_id, managementRefId=management_ref, priority=90, hubPriority=10, guard=guard)
        for node in genome.nodes:
            if node.zone is Zone.SETUP:
                add("remove_setup", nodeId=node.node_id)
            elif node.zone is Zone.ENTRY and node.kind == "entry":
                add("remove_entry_branch", nodeId=node.node_id)
        for effect, priority in ((EffectKind.BREAK_EVEN, 30), (EffectKind.TIGHTEN_STOP, 35), (EffectKind.ACTIVATE_TRAILING, 40)):
            for guard in guard_domain:
                add("insert_management_region", effect=effect.value, priority=priority, kind=effect.value, guard=guard)
        for guard in guard_domain:
            add("insert_exit_region", priority=50, kind="timed_exit", guard=guard)
            add("insert_timeout_rearm", timeoutBars=12, guard=guard)
        for node in genome.nodes:
            if node.zone is Zone.MANAGEMENT:
                for guard in guard_domain:
                    add("rewire_management_region", nodeId=node.node_id, priority=45, effect=EffectKind.TIGHTEN_STOP.value, guard=guard)
                add("remove_management_region", nodeId=node.node_id)
            elif node.zone is Zone.EXIT:
                for guard in guard_domain:
                    add("rewire_exit_region", nodeId=node.node_id, priority=55, guard=guard)
                add("remove_exit_region", nodeId=node.node_id)
            elif node.zone is Zone.RECOVERY:
                add("remove_timeout_rearm", nodeId=node.node_id)
        return [plans[key] for key in sorted(plans)]

    def apply_grammar(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        genome = self._genome(module)
        canonical = next((item for item in self.grammar_plans(module) if item == _clone(plan)), None)
        if canonical is None:
            raise TemporalDiscoveryContractError("evolvable topology plan is stale, foreign, or noncanonical")
        raw = canonical["plan"]
        topology = make_plan(genome, operation=str(raw["operation"]), **dict(raw["arguments"]))
        if topology.canonical() != raw or topology.identity_sha256 != canonical["planSha256"]:
            raise TemporalDiscoveryContractError("evolvable topology plan identity drifted")
        applied = apply_plan(genome, topology)
        frozen = self._freeze(module, applied.genome, candidate_id=candidate_id, lineage={
            "operation": "evolvable_topology", "side": module.direction,
            "plan": topology.canonical(), "planSha256": topology.identity_sha256,
            "application": applied.delta.canonical(),
        })
        audit = {"schemaVersion": "temporal_qd_evolvable_topology_audit_v1", "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "topologyPlanSha256": topology.identity_sha256, "topologyDelta": applied.delta.canonical(), "nativeValidationReportSha256": frozen.native_validation_report_sha256}
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def indicator_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        genome = self._genome(module)
        plans = [
            {"operatorId": "evolvable_resource_v1", "plan": plan, "planSha256": plan["planSha256"]}
            for plan in self.authority.resource_layer.enumerate_plans(genome)
        ]
        plans.extend(
            {"operatorId": "evolvable_temporal_v1", "plan": plan, "planSha256": plan["planSha256"]}
            for plan in self.authority.temporal_layer.enumerate_plans(genome)
        )
        return sorted(plans, key=lambda item: item["planSha256"])

    def apply_indicator(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        genome = self._genome(module)
        canonical = next((item for item in self.indicator_plans(module) if item == _clone(plan)), None)
        if canonical is None:
            raise TemporalDiscoveryContractError("evolvable resource plan is stale, foreign, or noncanonical")
        if canonical["operatorId"] == "evolvable_resource_v1":
            child, application = self.authority.resource_layer.apply(genome, canonical["plan"])
            operation = "evolvable_resource"
            audit_schema = "temporal_qd_evolvable_resource_audit_v1"
        elif canonical["operatorId"] == "evolvable_temporal_v1":
            child, application = self.authority.temporal_layer.apply(genome, canonical["plan"])
            operation = "evolvable_temporal"
            audit_schema = "temporal_qd_evolvable_temporal_audit_v1"
        else:
            raise TemporalDiscoveryContractError("evolvable indicator plan has an unknown operator")
        frozen = self._freeze(module, child, candidate_id=candidate_id, lineage={
            "operation": operation, "side": module.direction,
            "plan": canonical["plan"], "planSha256": canonical["planSha256"], "application": application,
        })
        audit = {"schemaVersion": audit_schema, "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "operatorId": canonical["operatorId"], "planSha256": canonical["planSha256"], "applicationSha256": application["applicationSha256"], "nativeValidationReportSha256": frozen.native_validation_report_sha256}
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def hold_policy_choices(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        self._genome(module)  # side/policy admission before exposing a choice
        policy = self.authority.bundle.config.get("holdOperatorPolicy")
        if not isinstance(policy, Mapping) or policy.get("enabled") is not True:
            return ()
        choices = policy.get("choices")
        if not isinstance(choices, list):
            raise TemporalDiscoveryContractError("frozen hold operator policy lacks choices")
        return tuple(canonical_hold(item) for item in choices)

    def apply_hold_policy(
        self,
        module: FrozenModule,
        *,
        plan_id: str,
        new_hold: Mapping[str, Any],
        candidate_id: str,
    ) -> tuple[FrozenModule, Mapping[str, Any]]:
        genome = self._genome(module)
        before = genome.resources.mapping(ResourceKind.MANAGEMENT_REF).get(str(plan_id))
        if before is None:
            raise TemporalDiscoveryContractError("hold mutation management plan drifted")
        canonical = canonical_hold(new_hold)
        if canonical not in self.hold_policy_choices(module):
            raise TemporalDiscoveryContractError("hold mutation is outside the frozen authority policy")
        current = canonical_hold(before.get("holdPolicy"))
        if current == canonical:
            raise TemporalDiscoveryContractError("hold mutation is a no-op")
        replacement = _clone(before)
        if canonical["kind"] == "none":
            replacement.pop("holdPolicy", None)
        else:
            replacement["holdPolicy"] = canonical
        child = self._replace_management_ref(genome, management_ref_id=str(plan_id), replacement=replacement)
        frozen = self._freeze(module, child, candidate_id=candidate_id, lineage={
            "operation": "evolvable_hold_policy", "side": module.direction,
            "managementPlanId": str(plan_id), "before": current, "after": canonical,
        })
        audit = {
            "schemaVersion": "temporal_qd_evolvable_hold_application_v1",
            "parentModuleIdentitySha256": module.identity_sha256,
            "childModuleIdentitySha256": frozen.identity_sha256,
            "managementPlanId": str(plan_id), "before": current, "after": canonical,
            "nativeValidationReportSha256": frozen.native_validation_report_sha256,
        }
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def initial_protection_plans(self, module: FrozenModule) -> Sequence[Mapping[str, Any]]:
        genome = self._genome(module)
        policy = self.authority.bundle.config.get("initialProtectionOperatorPolicy")
        if not isinstance(policy, Mapping):
            raise TemporalDiscoveryContractError("frozen initial protection operator policy is unavailable")
        # The compiled profile is the native vocabulary oracle.  The returned
        # plan is accepted only if its management plan maps back to a genome
        # resource and can be replayed through that resource exactly.
        by_id = genome.resources.mapping(ResourceKind.MANAGEMENT_REF)
        result: list[dict[str, Any]] = []
        # ``FrozenModule.profile`` is deeply frozen (lists become tuples);
        # the legacy enumerator intentionally accepts mutable JSON. Recover
        # the canonical payload rather than weakening its validation contract.
        native_profile = module.canonical_payload()["profile"]
        for plan in enumerate_initial_protection_plans(native_profile, policy):
            if str(plan.get("planId") or "") in by_id:
                result.append(_clone(plan))
        # Reuse the existing catalog-authorized scalar construction vocabulary,
        # but persist its result in the genome management ref rather than as a
        # detached compiled-profile mutation.
        from .temporal_operator_construction_v3 import ConstructionCatalog, ScalarDynamicManagementConstructionOperator
        catalog = module.catalog.canonical_payload()["payload"].get("catalog")
        if not isinstance(catalog, Mapping):
            raise TemporalDiscoveryContractError("frozen module catalog payload is unavailable")
        scalar_operator = ScalarDynamicManagementConstructionOperator(ConstructionCatalog(catalog))
        for construction_plan in scalar_operator.enumerate_plans(native_profile):
            construction = construction_plan.get("construction")
            if not isinstance(construction, Mapping) or construction.get("site") not in {"initial_stop", "initial_target"}:
                continue
            if str(construction.get("planId") or "") not in by_id:
                continue
            wrapped = {"kind": "dynamic_construction", "constructionPlan": _clone(construction_plan), "mutationClass": "kind_switch"}
            wrapped["planSha256"] = canonical_sha256(wrapped)
            result.append(wrapped)
        return tuple(sorted(result, key=lambda item: str(item["planSha256"])))

    def apply_initial_protection(self, module: FrozenModule, plan: Mapping[str, Any], *, candidate_id: str) -> tuple[FrozenModule, Mapping[str, Any]]:
        genome = self._genome(module)
        canonical = next((item for item in self.initial_protection_plans(module) if item == _clone(plan)), None)
        if canonical is None:
            raise TemporalDiscoveryContractError("initial protection plan is stale, foreign, or noncanonical")
        if canonical.get("kind") == "dynamic_construction":
            from .temporal_operator_construction_v3 import ConstructionCatalog, ScalarDynamicManagementConstructionOperator
            raw = canonical["constructionPlan"]
            catalog = module.catalog.canonical_payload()["payload"].get("catalog")
            if not isinstance(catalog, Mapping):
                raise TemporalDiscoveryContractError("frozen module catalog payload is unavailable")
            profile = module.canonical_payload()["profile"]
            preview = ScalarDynamicManagementConstructionOperator(ConstructionCatalog(catalog)).preview(profile, raw)
            construction = raw["construction"]
            plan_id = str(construction["planId"])
            selected = next(item for item in preview["executionConfig"]["managementLibrary"]["plans"] if item.get("id") == plan_id)
            replacement = _clone(genome.resources.mapping(ResourceKind.MANAGEMENT_REF)[plan_id])
            for key in ("initialStop", "initialTarget", "trailingStop"):
                if key in selected:
                    replacement[key] = _clone(selected[key])
            replacement["scalarBindings"] = _clone(preview["executionConfig"]["managementLibrary"].get("scalarBindings") or [])
            child = self._replace_management_ref(genome, management_ref_id=plan_id, replacement=replacement)
            frozen = self._freeze(module, child, candidate_id=candidate_id, lineage={"operation": "evolvable_dynamic_initial_protection", "side": module.direction, "plan": canonical, "planSha256": canonical["planSha256"]})
            audit = {"schemaVersion": "temporal_qd_evolvable_dynamic_initial_protection_application_v1", "parentModuleIdentitySha256": module.identity_sha256, "childModuleIdentitySha256": frozen.identity_sha256, "managementPlanId": plan_id, "constructionPlanSha256": raw["planSha256"], "nativeValidationReportSha256": frozen.native_validation_report_sha256}
            audit["auditSha256"] = canonical_sha256(audit)
            return frozen, audit
        plan_id = str(canonical["planId"])
        before = genome.resources.mapping(ResourceKind.MANAGEMENT_REF)[plan_id]
        replacement = _clone(before)
        key = "initialStop" if canonical["site"] == "stop" else "initialTarget"
        replacement[key] = _clone(canonical["replacement"])
        child = self._replace_management_ref(genome, management_ref_id=plan_id, replacement=replacement)
        frozen = self._freeze(module, child, candidate_id=candidate_id, lineage={
            "operation": "evolvable_initial_protection", "side": module.direction,
            "plan": canonical, "planSha256": canonical["planSha256"],
            "before": _clone(before.get(key)), "after": _clone(replacement[key]),
        })
        audit = {
            "schemaVersion": "temporal_qd_evolvable_initial_protection_application_v1",
            "parentModuleIdentitySha256": module.identity_sha256,
            "childModuleIdentitySha256": frozen.identity_sha256,
            "managementPlanId": plan_id, "site": canonical["site"],
            "mutationClass": canonical["mutationClass"],
            "before": _clone(before.get(key)), "after": _clone(replacement[key]),
            "nativeValidationReportSha256": frozen.native_validation_report_sha256,
        }
        audit["auditSha256"] = canonical_sha256(audit)
        return frozen, audit

    def crossover(self, left_program: Mapping[str, Any], right_program: Mapping[str, Any], *, direction: str, proposal_seed: str) -> Mapping[str, Any]:
        left = self.authority.decode_program(left_program)
        right = self.authority.decode_program(right_program)
        if left.direction != direction or right.direction != direction:
            raise TemporalDiscoveryContractError("evolvable crossover side drifted")
        # The topology layer verifies typed ports, resource closure and ordered
        # parent identities. Entry/setup is deliberately a linear compatible
        # segment, while management/exit remain shared-hub motifs.
        ports: list[tuple[str, list[str]]] = []
        for port, predicate in (
            ("entry_setup", lambda node: node.zone is Zone.SETUP),
            ("management_hub", lambda node: node.zone is Zone.MANAGEMENT),
            ("exit_hub", lambda node: node.zone is Zone.EXIT),
        ):
            right_ids = [edge.edge_id for edge in right.edges if predicate({node.node_id: node for node in right.nodes}[edge.target_id])]
            left_ids = [edge.edge_id for edge in left.edges if predicate({node.node_id: node for node in left.nodes}[edge.target_id])]
            if right_ids and left_ids:
                compatible = []
                for donor in sorted(right_ids):
                    try:
                        make_crossover_plan(left, right, segment_map={port: [donor]})
                    except EvolvableGenomeError:
                        continue
                    compatible.append(donor)
                if compatible:
                    ports.append((port, compatible))
        if not ports:
            raise TemporalDiscoveryContractError("evolvable crossover has no compatible same-side motif port")
        port, donor_ids = _choice(str(proposal_seed), axis="crossover_port", values=ports)
        donor = _choice(str(proposal_seed), axis="crossover_donor", values=donor_ids)
        plan = make_crossover_plan(left, right, segment_map={port: [donor]})
        return apply_crossover(left, right, plan).genome.canonical()

    def compile_program(self, template: FrozenModule, program: Mapping[str, Any], *, candidate_id: str) -> FrozenModule:
        genome = self.authority.decode_program(program)
        if genome.direction != template.direction:
            raise TemporalDiscoveryContractError("evolvable crossover compiler emitted a cross-side module")
        return self._freeze(template, genome, candidate_id=candidate_id, lineage={
            "operation": "evolvable_same_side_crossover", "side": genome.direction,
            "childProgramSha256": canonical_sha256(genome.canonical()),
            "semanticTopologySha256": genome.semantic_topology_signature(),
        })


class EvolvableModulePairAuthority:
    """Runtime authority opened only from an exact legacy pair authority plus opt-in config."""

    def __init__(self, bundle: Any, config: Mapping[str, Any]) -> None:
        self.bundle = bundle
        long_catalog = _side_catalog(bundle, "long")
        short_catalog = _side_catalog(bundle, "short")
        if canonical_sha256(long_catalog) != canonical_sha256(short_catalog):
            raise TemporalDiscoveryContractError("evolvable module authority requires symmetric frozen long/short catalogs")
        # Existing pair configs record an IndicatorLearning catalog identity;
        # use that exact field rather than trusting a caller-provided alias.
        configured_catalog_sha = str(bundle.config["longModule"].get("catalogSha256") or "")
        if not configured_catalog_sha:
            raise TemporalDiscoveryContractError("frozen pair authority lacks catalog SHA-256")
        self.config = _validate_authority_config(config, pair_run_config_sha256=bundle.config["pairRunConfigSha256"], catalog_sha256=configured_catalog_sha)
        self.budget = BudgetContractV1(**{
            "max_states": self.config["budget"]["maxStates"], "max_transitions": self.config["budget"]["maxTransitions"],
            "max_evidence_groups": self.config["budget"]["maxEvidenceGroups"], "max_group_members": self.config["budget"]["maxGroupMembers"],
            "max_events": self.config["budget"]["maxEvents"], "max_indicators": self.config["budget"]["maxIndicators"],
            "max_entry_branches": self.config["budget"]["maxEntryBranches"], "max_management_regions": self.config["budget"]["maxManagementRegions"],
            "max_exit_regions": self.config["budget"]["maxExitRegions"], "max_recovery_regions": self.config["budget"]["maxRecoveryRegions"],
            "max_scc_nodes": self.config["budget"]["maxSccNodes"], "max_timeout_bars": self.config["budget"]["maxTimeoutBars"], "max_guard_depth": self.config["budget"]["maxGuardDepth"],
        })
        self.compiler = EvolvableModuleCompilerV1(CompilerPolicyIdentityV1())
        self.resource_layer = GenomeResourceOperatorLayer(long_catalog)
        self.temporal_layer = GenomeTemporalOperatorLayer(
            compiler=self.compiler,
            native_validator=bundle.validator,
        )
        self.factory = EvolvableModulePairFactory(self)
        self.operator = EvolvableModulePairOperator(self)
        if "capacityReceipt" in self.config:
            validate_capacity_receipt(self, self.config["capacityReceipt"])
        # Opening verifies registry content is actually the one named by config.
        registry = self.config["operatorRegistry"]
        if registry.get("resourceOperators") != "evolvable_module_resource_operators_v1" or registry.get("temporalOperators") != "evolvable_module_temporal_operators_v1" or registry.get("topologyOperators") != "evolvable_module_topology_operator_v1":
            raise TemporalDiscoveryContractError("evolvable module operator registry drifted")
        if self.config["catalogSha256"] != configured_catalog_sha:
            raise TemporalDiscoveryContractError("evolvable module authority catalog identity drifted")

    def grammar_context(self, side: str) -> IdentitySnapshot:
        return IdentitySnapshot.create(kind="grammarContext", schema_version="evolvable_module_context_v1", payload={"authoritySha256": self.config["authoritySha256"], "side": side, "context": _side_context(self.bundle, side)})

    def catalog_identity(self, side: str) -> IdentitySnapshot:
        return IdentitySnapshot.create(kind="catalog", schema_version="evolvable_module_catalog_v1", payload={"catalog": _side_catalog(self.bundle, side), "catalogSha256": self.config["catalogSha256"], "side": side})

    def module_policy(self, side: str) -> IdentitySnapshot:
        return IdentitySnapshot.create(kind="policy", schema_version="evolvable_module_policy_v1", payload={"authoritySha256": self.config["authoritySha256"], "side": side, "budget": self.budget.canonical(), "compilerPolicySha256": self.compiler.policy.sha256, "resourceOperatorSpecSha256": self.resource_layer.specification["operatorSpecSha256"]})

    def decode_program(self, program: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
        genome = decode_program(program_kind=str(program.get("programKind") or ""), codec=str(program.get("codec") or ""), payload=program)
        genome.validate()
        if genome.budget.canonical() != self.budget.canonical():
            raise TemporalDiscoveryContractError("evolvable module program budget drifted from authority")
        return genome

    def decode_module(self, module: FrozenModule) -> EvolvableModuleGenomeV1:
        if module.policy.sha256 != self.module_policy(module.direction).sha256:
            raise TemporalDiscoveryContractError("evolvable module policy authority drifted")
        if module.catalog.sha256 != self.catalog_identity(module.direction).sha256:
            raise TemporalDiscoveryContractError("evolvable module catalog authority drifted")
        return self.decode_program(module.canonical_payload()["program"])

    def generation_bindings(self, run_config: Mapping[str, Any]) -> dict[str, Any]:
        """Seal the fresh v5 authority into a generic generation invocation.

        The direction/archive owner wires this material into journals and task
        manifests.  Keeping this adapter here ensures the evolvable authority
        is the sole author of its exact v5 policy and observer requirement.
        """

        frozen = _clone(run_config, name="evolvable generation run config")
        archive = _clone(self.config["archivePolicyAuthority"], name="archive policy authority")
        behavior = _clone(
            self.config["behaviorAttributionRequirement"],
            name="behavior attribution requirement",
        )
        receipt = (
            validate_capacity_receipt(self, self.config["capacityReceipt"])
            if "capacityReceipt" in self.config
            else None
        )
        operator_implementation = {
            "schemaVersion": "temporal_qd_evolvable_module_operator_implementation_v1",
            "authoritySha256": self.config["authoritySha256"],
            "programKind": self.config["programKind"],
            "codec": self.config["codec"],
            "compilerPolicySha256": self.config["compilerPolicySha256"],
            "operatorRegistry": _clone(self.config["operatorRegistry"]),
            "budget": _clone(self.config["budget"]),
            "capacityContract": _clone(self.config["capacityContract"]),
            "archivePolicyAuthoritySha256": canonical_sha256(archive),
            "behaviorAttributionRequirementSha256": behavior["requirementSha256"],
        }
        if receipt is not None:
            operator_implementation["capacityReceiptSha256"] = receipt["semanticReceiptSha256"]
        operator_implementation["operatorImplementationSha256"] = canonical_sha256(operator_implementation)
        for field, expected in (
            ("archivePolicyAuthority", archive),
            ("behaviorAttributionRequirement", behavior),
        ):
            supplied = frozen.get(field)
            if supplied is not None and supplied != expected:
                raise TemporalDiscoveryContractError(f"evolvable generation {field} drifted")
            frozen[field] = expected
        if receipt is not None:
            supplied_receipt = frozen.get("capacityReceipt")
            if supplied_receipt is not None and supplied_receipt != receipt:
                raise TemporalDiscoveryContractError("evolvable generation capacityReceipt drifted")
            frozen["capacityReceipt"] = receipt
        supplied_operator = frozen.get("operatorImplementation")
        if supplied_operator is not None and supplied_operator != operator_implementation:
            raise TemporalDiscoveryContractError("evolvable generation operatorImplementation drifted")
        frozen["operatorImplementation"] = operator_implementation
        return {
            "runConfig": frozen,
            "archivePolicyAuthority": archive,
            "behaviorAttributionRequirement": behavior,
            "operatorImplementation": operator_implementation,
            "capacityReceipt": receipt,
        }


def open_evolvable_module_pair_authority(*, bundle: Any, config: Mapping[str, Any]) -> EvolvableModulePairAuthority:
    """Explicit runtime constructor; it never mutates the legacy bundle."""

    return EvolvableModulePairAuthority(bundle, config)


def capacity_receipt(authority: EvolvableModulePairAuthority, probe: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze only deterministic capacity evidence, never wall-clock telemetry."""

    source = _clone(probe, name="capacity probe")
    if (
        source.get("schemaVersion") != "temporal_qd_evolvable_module_capacity_probe_v1"
        or source.get("authoritySha256") != authority.config["authoritySha256"]
        or source.get("capacityContract") != authority.config["capacityContract"]
        or source.get("previewStreamSize") != authority.config["capacityContract"]["previewStreamSize"]
    ):
        raise TemporalDiscoveryContractError("capacity receipt requires the exact authority-owned capacity probe")
    semantic = {
        "schemaVersion": EVOLVABLE_QD_CAPACITY_RECEIPT_SCHEMA,
        "authoritySha256": authority.config["authoritySha256"],
        "pairRunConfigSha256": authority.config["pairRunConfigSha256"],
        "catalogSha256": authority.config["catalogSha256"],
        "programKind": authority.config["programKind"],
        "codec": authority.config["codec"],
        "compilerPolicySha256": authority.config["compilerPolicySha256"],
        "operatorRegistrySha256": canonical_sha256(authority.config["operatorRegistry"]),
        "factoryPolicySha256": canonical_sha256(authority.factory.construction_policy),
        "capacityContract": _clone(authority.config["capacityContract"]),
        "noMarket": source.get("noMarket") is True,
        "previewStreamSize": source.get("previewStreamSize"),
        "rawPreview": source.get("rawPreview"),
        "compiledAdmittedCandidateCount": source.get("compiledAdmittedCandidateCount"),
        "uniqueSemanticPairCount": source.get("uniqueSemanticPairCount"),
        "uniqueCompiledV3ProfileCount": source.get("uniqueCompiledV3ProfileCount"),
        "nativeOrCompilerRejectionCounts": source.get("nativeOrCompilerRejectionCounts"),
        "perSide": source.get("perSide"),
        "passed": source.get("passed") is True,
        "admission": "native_v2_then_compiled_v3_no_market_v1",
    }
    semantic["semanticReceiptSha256"] = canonical_sha256(semantic)
    return semantic


def validate_capacity_receipt(authority: EvolvableModulePairAuthority, receipt: Mapping[str, Any]) -> dict[str, Any]:
    raw = _clone(receipt, name="capacity receipt")
    supplied = raw.pop("semanticReceiptSha256", None)
    expected_bindings = {
        "schemaVersion": EVOLVABLE_QD_CAPACITY_RECEIPT_SCHEMA,
        "authoritySha256": authority.config["authoritySha256"],
        "pairRunConfigSha256": authority.config["pairRunConfigSha256"],
        "catalogSha256": authority.config["catalogSha256"],
        "programKind": authority.config["programKind"],
        "codec": authority.config["codec"],
        "compilerPolicySha256": authority.config["compilerPolicySha256"],
        "operatorRegistrySha256": canonical_sha256(authority.config["operatorRegistry"]),
        "factoryPolicySha256": canonical_sha256(authority.factory.construction_policy),
        "capacityContract": authority.config["capacityContract"],
        "admission": "native_v2_then_compiled_v3_no_market_v1",
    }
    expected_keys = {
        *expected_bindings,
        "noMarket", "previewStreamSize", "rawPreview",
        "compiledAdmittedCandidateCount", "uniqueSemanticPairCount",
        "uniqueCompiledV3ProfileCount", "nativeOrCompilerRejectionCounts",
        "perSide", "passed",
    }
    if (
        set(raw) != expected_keys
        or supplied != canonical_sha256(raw)
        or any(raw.get(key) != value for key, value in expected_bindings.items())
    ):
        raise TemporalDiscoveryContractError("evolvable capacity receipt identity or authority drifted")
    if raw.get("noMarket") is not True or raw.get("passed") is not True:
        raise TemporalDiscoveryContractError("evolvable capacity receipt is not a passing no-market admission")
    contract = authority.config["capacityContract"]
    preview_size = raw.get("previewStreamSize")
    admitted = raw.get("compiledAdmittedCandidateCount")
    unique_pairs = raw.get("uniqueSemanticPairCount")
    unique_profiles = raw.get("uniqueCompiledV3ProfileCount")
    if (
        not all(isinstance(value, int) and not isinstance(value, bool) for value in (preview_size, admitted, unique_pairs, unique_profiles))
        or preview_size != contract["previewStreamSize"]
        or admitted < 0 or admitted > preview_size
        or unique_pairs < 0 or unique_pairs > admitted
        or unique_profiles < 0 or unique_profiles > admitted
        or unique_pairs < contract["minimumUniquePairs"]
    ):
        raise TemporalDiscoveryContractError("evolvable capacity receipt lacks required admitted pair diversity")
    rejections = raw.get("nativeOrCompilerRejectionCounts")
    if (
        not isinstance(rejections, Mapping)
        or any(not isinstance(key, str) or not isinstance(value, int) or isinstance(value, bool) or value < 0 for key, value in rejections.items())
        or sum(rejections.values()) + admitted != preview_size
    ):
        raise TemporalDiscoveryContractError("evolvable capacity receipt native admission outcomes are invalid")
    raw_preview = raw.get("rawPreview")
    per_side = raw.get("perSide")
    if (
        not isinstance(raw_preview, Mapping)
        or set(raw_preview) != {"uniqueSemanticPairCount", "perSide"}
        or not isinstance(raw_preview.get("uniqueSemanticPairCount"), int)
        or isinstance(raw_preview.get("uniqueSemanticPairCount"), bool)
        or raw_preview["uniqueSemanticPairCount"] < unique_pairs
        or raw_preview["uniqueSemanticPairCount"] > preview_size
        or not isinstance(per_side, Mapping)
        or set(per_side) != {"long", "short"}
    ):
        raise TemporalDiscoveryContractError("evolvable capacity receipt diversity evidence is invalid")
    for side in ("long", "short"):
        side_evidence = per_side.get(side)
        raw_side = raw_preview.get("perSide", {}).get(side) if isinstance(raw_preview.get("perSide"), Mapping) else None
        if (
            not isinstance(side_evidence, Mapping)
            or not isinstance(raw_side, Mapping)
            or set(side_evidence) != {
                "uniqueSemanticTopologyCount", "uniqueResourceFingerprintCount",
                "managementEffectCounts", "exitPresenceCounts",
            }
            or set(raw_side) != {"uniqueSemanticTopologyCount", "uniqueResourceFingerprintCount"}
            or not isinstance(side_evidence.get("uniqueSemanticTopologyCount"), int)
            or not isinstance(side_evidence.get("uniqueResourceFingerprintCount"), int)
            or isinstance(side_evidence["uniqueSemanticTopologyCount"], bool)
            or isinstance(side_evidence["uniqueResourceFingerprintCount"], bool)
            or not isinstance(raw_side.get("uniqueSemanticTopologyCount"), int)
            or not isinstance(raw_side.get("uniqueResourceFingerprintCount"), int)
            or isinstance(raw_side["uniqueSemanticTopologyCount"], bool)
            or isinstance(raw_side["uniqueResourceFingerprintCount"], bool)
            or raw_side["uniqueSemanticTopologyCount"] < side_evidence["uniqueSemanticTopologyCount"]
            or raw_side["uniqueResourceFingerprintCount"] < side_evidence["uniqueResourceFingerprintCount"]
            or not isinstance(side_evidence.get("managementEffectCounts"), Mapping)
            or not isinstance(side_evidence.get("exitPresenceCounts"), Mapping)
            or any(
                not isinstance(key, str) or not isinstance(value, int) or isinstance(value, bool) or value < 0
                for counts in (side_evidence["managementEffectCounts"], side_evidence["exitPresenceCounts"])
                for key, value in counts.items()
            )
            or side_evidence["uniqueSemanticTopologyCount"] < contract["minimumUniqueTopologiesPerSide"]
            or side_evidence["uniqueResourceFingerprintCount"] < contract["minimumUniqueResourceFingerprintsPerSide"]
        ):
            raise TemporalDiscoveryContractError("evolvable capacity receipt side diversity is insufficient")
    return {**raw, "semanticReceiptSha256": supplied}


def capacity_probe(authority: EvolvableModulePairAuthority, *, preview_stream_size: int | None = None) -> dict[str, Any]:
    """No-market capacity proof through the actual v2/v3 admission boundary.

    The raw preview stream is retained only as diagnostic observability.  The
    capacity claim counts a candidate *after* its genome compiled to v2,
    passed the native v2 validator, and compiled/admitted as a v3 pair.
    """

    contract = authority.config["capacityContract"]
    count = int(contract["previewStreamSize"] if preview_stream_size is None else preview_stream_size)
    if count != int(contract["previewStreamSize"]):
        raise TemporalDiscoveryContractError("capacity probe stream must equal its frozen capacity contract")
    raw_pair_ids: set[str] = set()
    raw_topology: dict[str, set[str]] = {"long": set(), "short": set()}
    raw_resources: dict[str, set[str]] = {"long": set(), "short": set()}
    pair_ids: set[str] = set()
    topology: dict[str, set[str]] = {"long": set(), "short": set()}
    resources: dict[str, set[str]] = {"long": set(), "short": set()}
    management: dict[str, Counter[str]] = {"long": Counter(), "short": Counter()}
    exits: dict[str, Counter[str]] = {"long": Counter(), "short": Counter()}
    rejection_counts: Counter[str] = Counter()
    compiled_profile_ids: set[str] = set()
    timings: list[float] = []
    admitted_count = 0
    for ordinal in range(count):
        preview = authority.factory.preview_pair(proposal_seed=f"capacity-{ordinal:08d}")
        raw_pair_ids.add(preview.semantic_sha256)
        for side, genome in (("long", preview.long), ("short", preview.short)):
            raw_topology[side].add(genome.semantic_topology_signature())
            raw_resources[side].add(_fingerprint(genome))
            management[side][str(preview.selectors[side]["managementEffect"])] += 1
            exits[side][str(preview.selectors[side]["hasExit"])] += 1
        started = perf_counter()
        try:
            pair = authority.factory.create_pair(proposal_seed=f"capacity-{ordinal:08d}")
        except Exception as exc:
            rejection_counts[_admission_rejection_reason(exc)] += 1
            continue
        admitted_count += 1
        timings.append(perf_counter() - started)
        long, short = authority.decode_module(pair.long), authority.decode_module(pair.short)
        # Pair/module identity contains lineage and native snapshots.  The
        # capacity semantic deliberately excludes only those non-strategy
        # details while retaining both admitted program and v3 source shape.
        pair_ids.add(canonical_sha256({
            "longProgramSha256": pair.long.program_sha256,
            "shortProgramSha256": pair.short.program_sha256,
        }))
        compiled_profile_ids.add(pair.profile_sha256)
        for side, genome in (("long", long), ("short", short)):
            topology[side].add(genome.semantic_topology_signature())
            resources[side].add(_fingerprint(genome))
    result = {
        "schemaVersion": "temporal_qd_evolvable_module_capacity_probe_v1",
        "authoritySha256": authority.config["authoritySha256"],
        "noMarket": True,
        "previewStreamSize": count,
        "rawPreview": {
            "uniqueSemanticPairCount": len(raw_pair_ids),
            "perSide": {
                side: {
                    "uniqueSemanticTopologyCount": len(raw_topology[side]),
                    "uniqueResourceFingerprintCount": len(raw_resources[side]),
                }
                for side in ("long", "short")
            },
        },
        "compiledAdmittedCandidateCount": admitted_count,
        "uniqueSemanticPairCount": len(pair_ids),
        "uniqueCompiledV3ProfileCount": len(compiled_profile_ids),
        "nativeOrCompilerRejectionCounts": dict(sorted(rejection_counts.items())),
        "timing": {
            "compiledCandidateCount": len(timings),
            "totalSeconds": sum(timings),
            "meanSeconds": (sum(timings) / len(timings)) if timings else None,
        },
        "perSide": {
            side: {
                "uniqueSemanticTopologyCount": len(topology[side]),
                "uniqueResourceFingerprintCount": len(resources[side]),
                "managementEffectCounts": dict(sorted(management[side].items())),
                "exitPresenceCounts": dict(sorted(exits[side].items())),
            }
            for side in ("long", "short")
        },
        "capacityContract": _clone(contract),
    }
    result["passed"] = (
        result["uniqueSemanticPairCount"] >= contract["minimumUniquePairs"]
        and all(result["perSide"][side]["uniqueSemanticTopologyCount"] >= contract["minimumUniqueTopologiesPerSide"] for side in ("long", "short"))
        and all(result["perSide"][side]["uniqueResourceFingerprintCount"] >= contract["minimumUniqueResourceFingerprintsPerSide"] for side in ("long", "short"))
    )
    if not result["passed"]:
        raise TemporalDiscoveryContractError("evolvable module capacity probe did not satisfy its frozen diversity contract")
    # Timing is operational telemetry only. It is intentionally excluded from
    # the deterministic capacity identity used by restart/admission records.
    semantic = {key: value for key, value in result.items() if key != "timing"}
    result["semanticProbeSha256"] = canonical_sha256(semantic)
    result["probeSha256"] = canonical_sha256(result)
    return result


__all__ = [
    "EVOLVABLE_QD_AUTHORITY_SCHEMA", "EvolvableModulePairAuthority",
    "EvolvableModulePairFactory", "EvolvableModulePairOperator",
    "PreviewPairV1", "build_evolvable_module_authority_config", "capacity_probe", "capacity_receipt", "validate_capacity_receipt",
    "open_evolvable_module_pair_authority",
]
