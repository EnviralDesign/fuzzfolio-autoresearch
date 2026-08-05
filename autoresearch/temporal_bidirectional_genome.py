"""Immutable, restart-safe identities for temporal v2-module / v3-pair genomes.

This is deliberately a boundary layer.  It does not generate a graph, execute
market logic, or launch a Dashboard process.  Callers give it already hydrated
v2 modules plus narrow native-validation and canonical-pair-compilation
authorities.  The layer freezes every authority input needed to reconstruct a
proposal after restart and refuses to turn a standalone v2 module into an
economic candidate.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Protocol


GENOME_SCHEMA = "temporal_bidirectional_genome_v1"
MODULE_SCHEMA = "temporal_bidirectional_module_snapshot_v1"
PAIR_SCHEMA = "temporal_bidirectional_pair_snapshot_v1"
HOLD_MUTATION_SCHEMA = "temporal_management_plan_hold_mutation_v1"
TRANSITION_DEDUPLICATION_SCHEMA = "temporal_behaviorally_redundant_transition_deduplication_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_SIDE = frozenset(("long", "short"))
_MUTABLE_ALIAS_KEYS = frozenset(
    (
        "alias",
        "catalogAlias",
        "catalogPath",
        "catalogRef",
        "currentCatalog",
        "mutableCatalog",
        "policyAlias",
        "policyPath",
        "authorityAlias",
        "authorityRef",
    )
)


class BidirectionalGenomeError(ValueError):
    """A persisted genome or requested operation violated the sealed contract."""


class NativeModuleValidator(Protocol):
    """Native v2 admission.  Implement over a persistent transport if needed."""

    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]: ...


class CanonicalPairCompiler(Protocol):
    """The Dashboard-owned v3 bidirectional compiler; no local compiler exists here."""

    def compile_pair(
        self,
        *,
        long_profile: Mapping[str, Any],
        short_profile: Mapping[str, Any],
        candidate_id: str,
    ) -> Mapping[str, Any]: ...


class SameSideCrossover(Protocol):
    """Grammar-owned crossover for two programs with one identical direction."""

    def crossover(
        self,
        left_program: Mapping[str, Any],
        right_program: Mapping[str, Any],
        *,
        direction: str,
        proposal_seed: str,
    ) -> Mapping[str, Any]: ...


def canonical_json(value: Any) -> str:
    """The single JSON representation used for payload cloning and identities."""

    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise BidirectionalGenomeError("payload must be finite canonical JSON") from exc


def canonical_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(canonical_json(value))
    except BidirectionalGenomeError as exc:
        raise BidirectionalGenomeError(f"{name} must be finite canonical JSON") from exc


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BidirectionalGenomeError(f"{name} must be an object")
    cloned = _clone(dict(value), name=name)
    if not isinstance(cloned, dict):  # defensive; json object stays a dict
        raise BidirectionalGenomeError(f"{name} must be an object")
    return cloned


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SHA.fullmatch(token):
        raise BidirectionalGenomeError(f"{name} must be an exact sha256 identity")
    return token


def _side(value: Any, *, name: str = "direction") -> str:
    token = str(value or "").strip().lower()
    if token not in _SIDE:
        raise BidirectionalGenomeError(f"{name} must be long or short")
    return token


def _identifier(value: Any, *, name: str) -> str:
    token = "" if value is None else str(value).strip()
    if not token or len(token) > 240:
        raise BidirectionalGenomeError(f"{name} must be a nonempty explicit identifier")
    return token


def _assert_no_mutable_alias(value: Any, *, name: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in _MUTABLE_ALIAS_KEYS:
                raise BidirectionalGenomeError(f"{name} must embed a frozen payload, not mutable alias {key!r}")
            _assert_no_mutable_alias(item, name=name)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_mutable_alias(item, name=name)


def normalize_behaviorally_redundant_transitions(
    profile: Mapping[str, Any],
    *,
    preserve_referenced_transition_ids: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Remove only exact duplicate graph transitions from an authored profile.

    Transition selection in the native temporal kernel is ordered by
    ``(priority, id)``.  For transitions whose every field other than those
    two selection/identity fields is canonically identical, selecting either
    one yields the same destination and actions.  Retaining that first native
    selection key is therefore behavior-preserving while avoiding redundant
    graph structure.

    This deliberately does *not* simplify guards, reorder actions, or treat
    labels/reason codes/unknown fields as cosmetic.  Those fields remain in
    the equivalence material so the rule stays conservative as the graph
    schema evolves.  An ID targeted by a guard or entry-arbitration structure
    also remains semantic and protects its whole equivalence group. Malformed
    transition rows are likewise left for the native validator to reject
    rather than being repaired here.
    """

    normalized = _mapping(
        _thaw(profile),
        name="transition-deduplication profile",
    )

    def empty_report() -> dict[str, Any]:
        report = {
            "schemaVersion": TRANSITION_DEDUPLICATION_SCHEMA,
            "transitionCount": 0,
            "duplicateGroupCount": 0,
            "removedTransitionCount": 0,
            "groups": [],
        }
        report["reportSha256"] = canonical_sha256(report)
        return report

    graph = normalized.get("graph")
    if not isinstance(graph, dict):
        return normalized, empty_report()
    transitions = graph.get("transitions")
    if not isinstance(transitions, list):
        return normalized, empty_report()

    referenced_transition_ids: set[str] = set()

    def collect_references(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if key in {"transitionId", "conflictTransitionId"} and isinstance(
                    item, str
                ):
                    referenced_transition_ids.add(item)
                elif key in {"supervisorTransitionIds", "transitionIds"} and isinstance(
                    item, (list, tuple)
                ):
                    referenced_transition_ids.update(
                        transition_id
                        for transition_id in item
                        if isinstance(transition_id, str)
                    )
                collect_references(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                collect_references(item)

    if preserve_referenced_transition_ids:
        collect_references(normalized)

    groups: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for index, transition in enumerate(transitions):
        if not isinstance(transition, dict):
            continue
        # Only valid native transition identity/priority shapes participate.
        # Invalid rows must remain visible to the downstream native validator.
        identifier = transition.get("id")
        priority = transition.get("priority")
        if (
            not isinstance(identifier, str)
            or not identifier
            or isinstance(priority, bool)
            or not isinstance(priority, int)
        ):
            continue
        semantics = {
            key: value
            for key, value in transition.items()
            if key not in {"id", "priority"}
        }
        groups.setdefault(canonical_json(semantics), []).append((index, transition))

    survivor_indexes = set(range(len(transitions)))
    audit_groups: list[dict[str, Any]] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        if preserve_referenced_transition_ids and any(
            transition["id"] in referenced_transition_ids
            for _index, transition in members
        ):
            continue
        # Match the native kernel's transition arbitration order exactly.
        survivor_index, survivor = min(
            members,
            key=lambda item: (int(item[1]["priority"]), str(item[1]["id"])),
        )
        removed = [
            item
            for item in members
            if item[0] != survivor_index
        ]
        survivor_indexes.difference_update(index for index, _item in removed)
        audit_groups.append(
            {
                "survivorTransitionId": survivor["id"],
                "survivorPriority": survivor["priority"],
                "removedTransitionIds": [item["id"] for _index, item in removed],
                "removedPriorities": [item["priority"] for _index, item in removed],
                "semanticTransitionSha256": canonical_sha256(
                    {
                        key: value
                        for key, value in survivor.items()
                        if key not in {"id", "priority"}
                    }
                ),
            }
        )

    if audit_groups:
        graph["transitions"] = [
            transition
            for index, transition in enumerate(transitions)
            if index in survivor_indexes
        ]
    report = {
        "schemaVersion": TRANSITION_DEDUPLICATION_SCHEMA,
        "transitionCount": len(transitions),
        "duplicateGroupCount": len(audit_groups),
        "removedTransitionCount": sum(
            len(group["removedTransitionIds"]) for group in audit_groups
        ),
        "groups": audit_groups,
    }
    report["reportSha256"] = canonical_sha256(report)
    return normalized, report


@dataclass(frozen=True)
class IdentitySnapshot:
    """Exact embedded authority/configuration payload with an independently checked hash."""

    kind: str
    schema_version: str
    payload: Mapping[str, Any]
    sha256: str

    @classmethod
    def create(cls, *, kind: str, schema_version: str, payload: Mapping[str, Any]) -> "IdentitySnapshot":
        token = _identifier(kind, name="snapshot kind")
        schema = _identifier(schema_version, name="snapshot schema version")
        body = _mapping(payload, name=f"{token} snapshot payload")
        _assert_no_mutable_alias(body, name=f"{token} snapshot")
        return cls(token, schema, _freeze(body), canonical_sha256(body))

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any], *, expected_kind: str | None = None) -> "IdentitySnapshot":
        value = _mapping(payload, name="identity snapshot")
        if set(value) != {"kind", "schemaVersion", "payload", "sha256"}:
            raise BidirectionalGenomeError("identity snapshot fields are not exact")
        result = cls.create(
            kind=_identifier(value["kind"], name="snapshot kind"),
            schema_version=_identifier(value["schemaVersion"], name="snapshot schema version"),
            payload=_mapping(value["payload"], name="snapshot payload"),
        )
        if expected_kind is not None and result.kind != expected_kind:
            raise BidirectionalGenomeError("identity snapshot kind is incompatible")
        if result.sha256 != _sha(value["sha256"], name="snapshot SHA-256"):
            raise BidirectionalGenomeError("identity snapshot payload hash mismatched")
        return result

    def canonical_payload(self) -> dict[str, Any]:
        return {"kind": self.kind, "schemaVersion": self.schema_version, "payload": _thaw(self.payload), "sha256": self.sha256}


@dataclass(frozen=True)
class FrozenModule:
    """A v2 module with all construction authority embedded for restart."""

    direction: str
    program: Mapping[str, Any]
    profile: Mapping[str, Any]
    grammar_context: IdentitySnapshot
    catalog: IdentitySnapshot
    policy: IdentitySnapshot
    native_authority: IdentitySnapshot
    native_report: Mapping[str, Any]
    lineage: tuple[Mapping[str, Any], ...]
    program_sha256: str
    profile_sha256: str
    native_snapshot_sha256: str
    native_program_sha256: str
    native_validation_report_sha256: str

    @classmethod
    def freeze(
        cls,
        *,
        program: Mapping[str, Any],
        profile: Mapping[str, Any],
        grammar_context: IdentitySnapshot,
        catalog: IdentitySnapshot,
        policy: IdentitySnapshot,
        native_authority: IdentitySnapshot,
        native_report: Mapping[str, Any],
        lineage: Sequence[Mapping[str, Any]] = (),
    ) -> "FrozenModule":
        grammar_context = IdentitySnapshot.from_payload(grammar_context.canonical_payload(), expected_kind="grammarContext")
        catalog = IdentitySnapshot.from_payload(catalog.canonical_payload(), expected_kind="catalog")
        policy = IdentitySnapshot.from_payload(policy.canonical_payload(), expected_kind="policy")
        native_authority = IdentitySnapshot.from_payload(native_authority.canonical_payload(), expected_kind="nativeAuthority")
        canonical_program = _mapping(program, name="v2 module program")
        canonical_profile = _mapping(profile, name="hydrated v2 module profile")
        # Every route that can become a frozen v2 module, including direct
        # factory output and later operator output, must obey the current
        # search-language entry-decision cap.  The import stays local so this
        # immutable genome layer does not own the grammar implementation.
        from .temporal_typed_motif_grammar import (
            validate_entry_route_decision_indicator_cap,
        )

        validate_entry_route_decision_indicator_cap(canonical_profile)
        direction = _side(canonical_program.get("direction"), name="module program direction")
        if set(canonical_program) != {"schemaVersion", "grammarVersion", "direction", "fragments"} or canonical_program.get("schemaVersion") != "temporal_typed_fragment_grammar_v2" or canonical_program.get("grammarVersion") != "3" or not isinstance(canonical_program.get("fragments"), list):
            raise BidirectionalGenomeError("module program is not a canonical typed v2 program")
        if canonical_profile.get("version") != "v2" or _side(canonical_profile.get("directionMode"), name="v2 profile direction") != direction:
            raise BidirectionalGenomeError("hydrated profile is not the matching v2 module")
        report = _mapping(native_report, name="native module validation report")
        profile_sha = canonical_sha256(canonical_profile)
        if report.get("schemaVersion") != "temporal_search_candidate_validation_v1":
            raise BidirectionalGenomeError("native module report schema is incompatible")
        if report.get("rawSourceProfileSha256") != profile_sha or report.get("status") != "valid_evaluable" or report.get("candidateAcceptable") is not True:
            raise BidirectionalGenomeError("native module report did not admit the exact v2 profile")
        native_snapshot = _sha(report.get("profileSnapshotSha256"), name="native module snapshot SHA-256")
        native_program = _sha(report.get("programSha256"), name="native module program SHA-256")
        native_validation = _sha(report.get("validationReportSha256"), name="native module validation report SHA-256")
        frozen_lineage = tuple(_freeze(_mapping(item, name="module lineage item")) for item in lineage)
        return cls(
            direction=direction,
            program=_freeze(canonical_program),
            profile=_freeze(canonical_profile),
            grammar_context=grammar_context,
            catalog=catalog,
            policy=policy,
            native_authority=native_authority,
            native_report=_freeze(report),
            lineage=frozen_lineage,
            program_sha256=canonical_sha256(canonical_program),
            profile_sha256=profile_sha,
            native_snapshot_sha256=native_snapshot,
            native_program_sha256=native_program,
            native_validation_report_sha256=native_validation,
        )

    @classmethod
    def validate_native(
        cls,
        *,
        program: Mapping[str, Any],
        profile: Mapping[str, Any],
        grammar_context: IdentitySnapshot,
        catalog: IdentitySnapshot,
        policy: IdentitySnapshot,
        native_authority_identity: IdentitySnapshot,
        native_validator: NativeModuleValidator,
        candidate_id: str,
        lineage: Sequence[Mapping[str, Any]] = (),
    ) -> "FrozenModule":
        if native_validator is None:
            raise BidirectionalGenomeError("native module validator is mandatory")
        candidate = _identifier(candidate_id, name="native module candidate id")
        normalized_profile, transition_report = (
            normalize_behaviorally_redundant_transitions(
                _mapping(profile, name="hydrated v2 module profile")
            )
        )
        report = native_validator.validate_v2(
            profile=normalized_profile,
            candidate_id=candidate,
        )
        normalized_lineage = list(lineage)
        if transition_report["removedTransitionCount"]:
            normalized_lineage.append(
                {
                    "operation": "deduplicate_behaviorally_redundant_transitions",
                    "side": normalized_profile.get("directionMode"),
                    "deduplication": transition_report,
                }
            )
        return cls.freeze(
            program=program,
            profile=normalized_profile,
            grammar_context=grammar_context,
            catalog=catalog,
            policy=policy,
            native_authority=native_authority_identity,
            native_report=report,
            lineage=normalized_lineage,
        )

    @classmethod
    def normalize_transitions(
        cls,
        module: "FrozenModule",
        *,
        native_validator: NativeModuleValidator,
        candidate_id: str,
    ) -> tuple["FrozenModule", dict[str, Any]]:
        """Re-admit an older frozen module only when its graph needs repair."""

        profile, report = normalize_behaviorally_redundant_transitions(
            module.profile
        )
        if not report["removedTransitionCount"]:
            return module, report
        return (
            cls.validate_native(
                program=_thaw(module.program),
                # Re-admit the original profile so ``validate_native`` records
                # the exact removed->survivor aliases in immutable lineage.
                # Passing ``profile`` here would be behaviorally correct but
                # would lose the provenance needed to map authored grammar
                # fragments to their surviving transition.
                profile=_thaw(module.profile),
                grammar_context=module.grammar_context,
                catalog=module.catalog,
                policy=module.policy,
                native_authority_identity=module.native_authority,
                native_validator=native_validator,
                candidate_id=candidate_id,
                lineage=[_thaw(item) for item in module.lineage],
            ),
            report,
        )

    def identity_material(self) -> dict[str, Any]:
        return {
            "direction": self.direction,
            "programSha256": self.program_sha256,
            "profileSha256": self.profile_sha256,
            "grammarContext": self.grammar_context.canonical_payload(),
            "catalog": self.catalog.canonical_payload(),
            "policy": self.policy.canonical_payload(),
            "nativeAuthority": self.native_authority.canonical_payload(),
            "nativeSnapshotSha256": self.native_snapshot_sha256,
            "nativeProgramSha256": self.native_program_sha256,
            "nativeValidationReportSha256": self.native_validation_report_sha256,
            "lineage": [_thaw(item) for item in self.lineage],
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256({"schemaVersion": MODULE_SCHEMA, **self.identity_material()})

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schemaVersion": MODULE_SCHEMA,
            "direction": self.direction,
            "program": _thaw(self.program),
            "profile": _thaw(self.profile),
            "grammarContext": self.grammar_context.canonical_payload(),
            "catalog": self.catalog.canonical_payload(),
            "policy": self.policy.canonical_payload(),
            "nativeAuthority": self.native_authority.canonical_payload(),
            "nativeReport": _thaw(self.native_report),
            "lineage": [_thaw(item) for item in self.lineage],
            "identities": {
                "programSha256": self.program_sha256,
                "profileSha256": self.profile_sha256,
                "nativeSnapshotSha256": self.native_snapshot_sha256,
                "nativeProgramSha256": self.native_program_sha256,
                "nativeValidationReportSha256": self.native_validation_report_sha256,
                "moduleIdentitySha256": self.identity_sha256,
            },
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "FrozenModule":
        value = _mapping(payload, name="frozen module payload")
        required = {"schemaVersion", "direction", "program", "profile", "grammarContext", "catalog", "policy", "nativeAuthority", "nativeReport", "lineage", "identities"}
        if set(value) != required or value.get("schemaVersion") != MODULE_SCHEMA:
            raise BidirectionalGenomeError("frozen module payload fields are not exact")
        lineage = value.get("lineage")
        if not isinstance(lineage, list):
            raise BidirectionalGenomeError("module lineage must be an ordered list")
        result = cls.freeze(
            program=_mapping(value["program"], name="module program"),
            profile=_mapping(value["profile"], name="module profile"),
            grammar_context=IdentitySnapshot.from_payload(_mapping(value["grammarContext"], name="grammar context"), expected_kind="grammarContext"),
            catalog=IdentitySnapshot.from_payload(_mapping(value["catalog"], name="catalog"), expected_kind="catalog"),
            policy=IdentitySnapshot.from_payload(_mapping(value["policy"], name="policy"), expected_kind="policy"),
            native_authority=IdentitySnapshot.from_payload(_mapping(value["nativeAuthority"], name="native authority"), expected_kind="nativeAuthority"),
            native_report=_mapping(value["nativeReport"], name="native report"),
            lineage=[_mapping(item, name="module lineage item") for item in lineage],
        )
        if result.direction != _side(value["direction"]):
            raise BidirectionalGenomeError("module direction mismatched its program")
        identities = _mapping(value["identities"], name="module identities")
        expected = result.canonical_payload()["identities"]
        if identities != expected:
            raise BidirectionalGenomeError("frozen module identity material mismatched payload")
        return result


def proposal_side(proposal_seed: str | int) -> str:
    """Stable side routing; no hidden PRNG state or invocation-order dependence."""

    seed = _identifier(proposal_seed, name="proposal seed")
    return "long" if int(canonical_sha256({"schemaVersion": GENOME_SCHEMA, "proposalSeed": seed})[-1], 16) % 2 == 0 else "short"


def deterministic_same_side_crossover(
    left: FrozenModule,
    right: FrozenModule,
    *,
    proposal_seed: str | int,
    crossover: SameSideCrossover,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Order same-side parents deterministically before invoking grammar crossover."""

    if crossover is None:
        raise BidirectionalGenomeError("same-side crossover authority is mandatory")
    if left.direction != right.direction:
        raise BidirectionalGenomeError("same-side crossover rejects opposite-side parents")
    if any(
        first.sha256 != second.sha256
        for first, second in (
            (left.grammar_context, right.grammar_context),
            (left.catalog, right.catalog),
            (left.policy, right.policy),
            (left.native_authority, right.native_authority),
        )
    ):
        raise BidirectionalGenomeError("same-side crossover requires identical frozen construction identities")
    seed = _identifier(proposal_seed, name="proposal seed")
    ordered = sorted((left, right), key=lambda item: canonical_sha256({"proposalSeed": seed, "moduleIdentitySha256": item.identity_sha256}))
    child = _mapping(
        crossover.crossover(
            left_program=_thaw(ordered[0].program),
            right_program=_thaw(ordered[1].program),
            direction=left.direction,
            proposal_seed=seed,
        ),
        name="same-side crossover child program",
    )
    if _side(child.get("direction"), name="crossover child direction") != left.direction:
        raise BidirectionalGenomeError("same-side crossover emitted a cross-side child")
    if child.get("schemaVersion") != "temporal_typed_fragment_grammar_v2" or child.get("grammarVersion") != "3":
        raise BidirectionalGenomeError("same-side crossover emitted a noncanonical v2 program")
    record = {
        "operation": "same_side_crossover",
        "side": left.direction,
        "proposalSeed": seed,
        "orderedParentModuleIdentitySha256": [item.identity_sha256 for item in ordered],
        "childProgramSha256": canonical_sha256(child),
    }
    return child, record


def canonical_hold(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Canonical Dashboard-native ``holdPolicy`` value for a management plan.

    ``none`` is the mutation vocabulary's representation of an absent optional
    ``holdPolicy`` field.  It is deliberately not written into a Dashboard
    management plan; :func:`apply_hold_mutation` removes the field instead.
    Bounds beyond the Dashboard type contract belong to a frozen operator
    policy, not this loader, so existing native seed profiles are not globally
    constrained by the QD search vocabulary.
    """

    raw = {"kind": "none"} if value is None else _mapping(value, name="management-plan hold")
    kind = str(raw.get("kind") or "").strip()
    if kind == "none" and set(raw) == {"kind"}:
        return {"kind": "none"}
    if kind == "market_bars" and set(raw) in ({"kind", "bars", "timeframe"}, {"kind", "bars", "timeframe", "onBreach"}):
        bars = raw["bars"]
        timeframe = str(raw["timeframe"] or "").strip().upper()
        if isinstance(bars, bool) or not isinstance(bars, int) or bars < 1 or not timeframe or len(timeframe) > 32:
            raise BidirectionalGenomeError("market_bars holdPolicy requires positive bars and a timeframe")
        if "onBreach" in raw and raw["onBreach"] != "exit_next_open":
            raise BidirectionalGenomeError("market_bars holdPolicy onBreach must be exit_next_open")
        return {"kind": kind, "bars": bars, "timeframe": timeframe}
    if kind == "elapsed_calendar" and set(raw) in ({"kind", "hours"}, {"kind", "hours", "onBreach"}):
        hours = raw["hours"]
        if isinstance(hours, bool) or not isinstance(hours, (int, float)) or not math.isfinite(float(hours)) or hours <= 0:
            raise BidirectionalGenomeError("elapsed_calendar holdPolicy requires positive hours")
        if "onBreach" in raw and raw["onBreach"] != "exit_next_open":
            raise BidirectionalGenomeError("elapsed_calendar holdPolicy onBreach must be exit_next_open")
        return {"kind": kind, "hours": float(hours)}
    raise BidirectionalGenomeError("holdPolicy must be none, market_bars (bars/timeframe), or elapsed_calendar (hours)")


@dataclass(frozen=True)
class HoldMutationPlan:
    side: str
    plan_id: str
    source_profile_sha256: str
    old_hold: Mapping[str, Any]
    new_hold: Mapping[str, Any]
    old_hold_sha256: str
    new_hold_sha256: str
    plan_id_sha256: str

    @classmethod
    def create(cls, module: FrozenModule, *, plan_id: str, new_hold: Mapping[str, Any] | None) -> "HoldMutationPlan":
        profile = _thaw(module.profile)
        plans = (((profile.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
        selected = [item for item in plans if isinstance(item, Mapping) and item.get("id") == plan_id]
        if len(selected) != 1:
            raise BidirectionalGenomeError("hold mutation requires one existing management plan id")
        old = canonical_hold(selected[0].get("holdPolicy"))
        new = canonical_hold(new_hold)
        if old == new:
            raise BidirectionalGenomeError("hold mutation must change the selected management plan")
        token = _identifier(plan_id, name="management plan id")
        return cls(module.direction, token, module.profile_sha256, _freeze(old), _freeze(new), canonical_sha256(old), canonical_sha256(new), canonical_sha256({"planId": token}))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schemaVersion": HOLD_MUTATION_SCHEMA,
            "side": self.side,
            "planId": self.plan_id,
            "sourceProfileSha256": self.source_profile_sha256,
            "oldHold": _thaw(self.old_hold),
            "newHold": _thaw(self.new_hold),
            "oldHoldSha256": self.old_hold_sha256,
            "newHoldSha256": self.new_hold_sha256,
            "planIdSha256": self.plan_id_sha256,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "HoldMutationPlan":
        value = _mapping(payload, name="hold mutation plan")
        required = {"schemaVersion", "side", "planId", "sourceProfileSha256", "oldHold", "newHold", "oldHoldSha256", "newHoldSha256", "planIdSha256"}
        if set(value) != required or value.get("schemaVersion") != HOLD_MUTATION_SCHEMA:
            raise BidirectionalGenomeError("hold mutation plan fields are not exact")
        old, new = canonical_hold(_mapping(value["oldHold"], name="old hold")), canonical_hold(_mapping(value["newHold"], name="new hold"))
        result = cls(
            _side(value["side"], name="hold mutation side"),
            _identifier(value["planId"], name="management plan id"),
            _sha(value["sourceProfileSha256"], name="hold mutation source profile SHA-256"),
            _freeze(old),
            _freeze(new),
            canonical_sha256(old),
            canonical_sha256(new),
            canonical_sha256({"planId": _identifier(value["planId"], name="management plan id")}),
        )
        if result.canonical_payload() != value:
            raise BidirectionalGenomeError("hold mutation plan hashes mismatched payload")
        return result

    @property
    def plan_sha256(self) -> str:
        return canonical_sha256(self.canonical_payload())


def apply_hold_mutation(
    module: FrozenModule,
    plan: HoldMutationPlan,
    *,
    native_validator: NativeModuleValidator,
    candidate_id: str,
) -> FrozenModule:
    """Re-admit the changed v2 module; this never modifies a v3 profile."""

    if module.direction != plan.side or module.profile_sha256 != plan.source_profile_sha256:
        raise BidirectionalGenomeError("hold mutation plan is not bound to this exact module")
    profile = _thaw(module.profile)
    plans = (((profile.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
    selected = [item for item in plans if isinstance(item, Mapping) and item.get("id") == plan.plan_id]
    if len(selected) != 1 or canonical_sha256(canonical_hold(selected[0].get("holdPolicy"))) != plan.old_hold_sha256:
        raise BidirectionalGenomeError("hold mutation source plan no longer matches its recorded hold")
    new_hold = _thaw(plan.new_hold)
    if new_hold["kind"] == "none":
        selected[0].pop("holdPolicy", None)
    else:
        selected[0]["holdPolicy"] = new_hold
    return FrozenModule.validate_native(
        program=_thaw(module.program),
        profile=profile,
        grammar_context=module.grammar_context,
        catalog=module.catalog,
        policy=module.policy,
        native_authority_identity=module.native_authority,
        native_validator=native_validator,
        candidate_id=candidate_id,
        lineage=(*[_thaw(item) for item in module.lineage], {"operation": "hold_mutation", "side": plan.side, "holdMutationPlan": plan.canonical_payload(), "holdMutationPlanSha256": plan.plan_sha256}),
    )


@dataclass(frozen=True)
class FrozenPair:
    """Exactly one frozen long and one frozen short module plus canonical v3 output."""

    long: FrozenModule
    short: FrozenModule
    pair_compiler: IdentitySnapshot
    profile: Mapping[str, Any]
    validation: Mapping[str, Any]
    side_targeted_lineage: tuple[Mapping[str, Any], ...]
    raw_pair_sha256: str
    profile_sha256: str
    native_program_sha256: str
    native_validation_report_sha256: str

    @classmethod
    def compile(
        cls,
        *,
        long: FrozenModule,
        short: FrozenModule,
        pair_compiler_identity: IdentitySnapshot,
        pair_compiler: CanonicalPairCompiler,
        candidate_id: str,
        side_targeted_lineage: Sequence[Mapping[str, Any]] = (),
        native_validator: NativeModuleValidator | None = None,
    ) -> "FrozenPair":
        if long.direction != "long" or short.direction != "short":
            raise BidirectionalGenomeError("economic candidates require exactly one long and one short v2 module")
        if pair_compiler is None:
            raise BidirectionalGenomeError("canonical pair compiler authority is mandatory")
        pair_compiler_identity = IdentitySnapshot.from_payload(pair_compiler_identity.canonical_payload(), expected_kind="pairCompiler")
        candidate = _identifier(candidate_id, name="pair candidate id")
        transition_reports: list[dict[str, Any]] = []
        for side, module in (("long", long), ("short", short)):
            _profile, report = normalize_behaviorally_redundant_transitions(
                module.profile
            )
            if not report["removedTransitionCount"]:
                continue
            if native_validator is None:
                raise BidirectionalGenomeError(
                    "pair compilation with redundant transitions requires a native module validator"
                )
            normalized, applied_report = FrozenModule.normalize_transitions(
                module,
                native_validator=native_validator,
                candidate_id=f"{candidate}_{side}_transition_dedupe",
            )
            if applied_report != report:
                raise BidirectionalGenomeError(
                    "transition deduplication report drifted during module re-admission"
                )
            if side == "long":
                long = normalized
            else:
                short = normalized
            transition_reports.append(
                {
                    "side": side,
                    "deduplicationReportSha256": report["reportSha256"],
                    "removedTransitionCount": report["removedTransitionCount"],
                }
            )
        if transition_reports:
            side_targeted_lineage = (
                *side_targeted_lineage,
                *[
                    {
                        "operation": "deduplicate_behaviorally_redundant_transitions",
                        "side": report["side"],
                        "deduplicationReportSha256": report[
                            "deduplicationReportSha256"
                        ],
                        "removedTransitionCount": report[
                            "removedTransitionCount"
                        ],
                    }
                    for report in transition_reports
                ],
            )
        result = _mapping(pair_compiler.compile_pair(long_profile=_thaw(long.profile), short_profile=_thaw(short.profile), candidate_id=candidate), name="canonical pair compiler result")
        if set(result) != {"profile", "validation"}:
            raise BidirectionalGenomeError("canonical pair compiler result fields are not exact")
        profile = _mapping(result["profile"], name="compiled v3 profile")
        _normalized_profile, compiled_transition_report = (
            normalize_behaviorally_redundant_transitions(
                profile,
                preserve_referenced_transition_ids=False,
            )
        )
        if compiled_transition_report["removedTransitionCount"]:
            raise BidirectionalGenomeError(
                "canonical pair compiler emitted behaviorally redundant transitions"
            )
        validation = _mapping(result["validation"], name="compiled v3 validation")
        profile_sha = canonical_sha256(profile)
        if profile.get("version") != "v3" or profile.get("directionMode") != "both" or "hold" in profile:
            raise BidirectionalGenomeError("canonical compiler did not emit a hold-free v3 bidirectional profile")
        if validation.get("schemaVersion") != "temporal_search_candidate_validation_v1" or validation.get("rawSourceProfileSha256") != profile_sha or validation.get("status") != "valid_evaluable" or validation.get("candidateAcceptable") is not True:
            raise BidirectionalGenomeError("canonical pair compiler did not admit the exact v3 profile")
        snapshot_sha = _sha(validation.get("profileSnapshotSha256"), name="compiled v3 profile snapshot SHA-256")
        program_sha = _sha(validation.get("programSha256"), name="compiled v3 program SHA-256")
        validation_sha = _sha(validation.get("validationReportSha256"), name="compiled v3 validation report SHA-256")
        manifests = (((profile.get("graph") or {}).get("entryArbitration") or {}).get("modules") or [])
        native_sources = {item.get("direction"): item.get("sourceProfileSnapshotSha256") for item in manifests if isinstance(item, Mapping)}
        if native_sources != {"long": long.native_snapshot_sha256, "short": short.native_snapshot_sha256}:
            raise BidirectionalGenomeError("canonical compiler did not bind both exact native module snapshots")
        frozen_lineage = []
        for item in side_targeted_lineage:
            event = _mapping(item, name="side-targeted lineage event")
            _side(event.get("side"), name="side-targeted lineage side")
            frozen_lineage.append(_freeze(event))
        return cls(long, short, pair_compiler_identity, _freeze(profile), _freeze(validation), tuple(frozen_lineage), profile_sha, snapshot_sha, program_sha, validation_sha)

    def identity_material(self) -> dict[str, Any]:
        return {
            "schemaVersion": PAIR_SCHEMA,
            "longModule": self.long.identity_material(),
            "shortModule": self.short.identity_material(),
            "pairCompiler": self.pair_compiler.canonical_payload(),
            "compiledV3": {
                "rawPairSha256": self.raw_pair_sha256,
                "profileSha256": self.profile_sha256,
                "programSha256": self.native_program_sha256,
                "validationReportSha256": self.native_validation_report_sha256,
            },
            "sideTargetedLineage": [_thaw(item) for item in self.side_targeted_lineage],
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.identity_material())

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schemaVersion": PAIR_SCHEMA,
            "long": self.long.canonical_payload(),
            "short": self.short.canonical_payload(),
            "pairCompiler": self.pair_compiler.canonical_payload(),
            "profile": _thaw(self.profile),
            "validation": _thaw(self.validation),
            "sideTargetedLineage": [_thaw(item) for item in self.side_targeted_lineage],
            "identities": {**self.identity_material()["compiledV3"], "pairIdentitySha256": self.identity_sha256},
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "FrozenPair":
        value = _mapping(payload, name="frozen pair payload")
        required = {"schemaVersion", "long", "short", "pairCompiler", "profile", "validation", "sideTargetedLineage", "identities"}
        if set(value) != required or value.get("schemaVersion") != PAIR_SCHEMA:
            raise BidirectionalGenomeError("frozen pair payload fields are not exact")
        long, short = FrozenModule.from_payload(_mapping(value["long"], name="long module")), FrozenModule.from_payload(_mapping(value["short"], name="short module"))
        # Reuse compile's structural checks without invoking the external compiler.
        profile, validation = _mapping(value["profile"], name="compiled v3 profile"), _mapping(value["validation"], name="compiled v3 validation")
        profile_sha = canonical_sha256(profile)
        if long.direction != "long" or short.direction != "short" or profile.get("version") != "v3" or profile.get("directionMode") != "both" or "hold" in profile:
            raise BidirectionalGenomeError("persisted pair is not an exact long/short v3 candidate")
        if validation.get("schemaVersion") != "temporal_search_candidate_validation_v1" or validation.get("rawSourceProfileSha256") != profile_sha or validation.get("status") != "valid_evaluable" or validation.get("candidateAcceptable") is not True:
            raise BidirectionalGenomeError("persisted pair validation does not bind its v3 profile")
        manifests = (((profile.get("graph") or {}).get("entryArbitration") or {}).get("modules") or [])
        sources = {item.get("direction"): item.get("sourceProfileSnapshotSha256") for item in manifests if isinstance(item, Mapping)}
        if sources != {"long": long.native_snapshot_sha256, "short": short.native_snapshot_sha256}:
            raise BidirectionalGenomeError("persisted pair native source snapshots mismatched")
        lineage = value["sideTargetedLineage"]
        if not isinstance(lineage, list):
            raise BidirectionalGenomeError("pair side-targeted lineage must be ordered list")
        frozen_lineage = []
        for item in lineage:
            event = _mapping(item, name="side-targeted lineage item")
            _side(event.get("side"), name="side-targeted lineage side")
            frozen_lineage.append(_freeze(event))
        result = cls(long, short, IdentitySnapshot.from_payload(_mapping(value["pairCompiler"], name="pair compiler"), expected_kind="pairCompiler"), _freeze(profile), _freeze(validation), tuple(frozen_lineage), profile_sha, _sha(validation.get("profileSnapshotSha256"), name="compiled v3 profile snapshot SHA-256"), _sha(validation.get("programSha256"), name="compiled v3 program SHA-256"), _sha(validation.get("validationReportSha256"), name="compiled v3 validation report SHA-256"))
        identities = _mapping(value["identities"], name="pair identities")
        expected = {**result.identity_material()["compiledV3"], "pairIdentitySha256": result.identity_sha256}
        if identities != expected:
            raise BidirectionalGenomeError("frozen pair identity material mismatched payload")
        return result


def apply_pair_hold_mutation(
    pair: FrozenPair,
    plan: HoldMutationPlan,
    *,
    native_validator: NativeModuleValidator,
    pair_compiler: CanonicalPairCompiler,
    candidate_id: str,
) -> FrozenPair:
    """Change only the targeted module, then require canonical v3 recompilation."""

    target = pair.long if plan.side == "long" else pair.short
    changed = apply_hold_mutation(target, plan, native_validator=native_validator, candidate_id=f"{candidate_id}_{plan.side}_hold")
    long, short = (changed, pair.short) if plan.side == "long" else (pair.long, changed)
    return FrozenPair.compile(
        long=long,
        short=short,
        pair_compiler_identity=pair.pair_compiler,
        pair_compiler=pair_compiler,
        candidate_id=candidate_id,
        side_targeted_lineage=(*[_thaw(item) for item in pair.side_targeted_lineage], {"operation": "pair_hold_mutation", "side": plan.side, "holdMutationPlanSha256": plan.plan_sha256}),
        native_validator=native_validator,
    )


# Public domain name used by QD policy documents.  Keeping the implementation
# class name preserves the existing persisted payload schema exactly.
BidirectionalGenome = FrozenPair


__all__ = [
    "BidirectionalGenome", "BidirectionalGenomeError", "CanonicalPairCompiler", "FrozenModule", "FrozenPair", "GENOME_SCHEMA",
    "HOLD_MUTATION_SCHEMA", "HoldMutationPlan", "IdentitySnapshot", "MODULE_SCHEMA", "NativeModuleValidator",
    "PAIR_SCHEMA", "SameSideCrossover", "TRANSITION_DEDUPLICATION_SCHEMA", "apply_hold_mutation", "apply_pair_hold_mutation", "canonical_hold",
    "canonical_json", "canonical_sha256", "deterministic_same_side_crossover", "proposal_side",
    "normalize_behaviorally_redundant_transitions",
]
