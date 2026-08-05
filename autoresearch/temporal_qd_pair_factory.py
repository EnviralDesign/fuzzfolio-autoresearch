"""Closed production authority for the opt-in bidirectional QD generator.

The supervisor persists the value returned by :func:`freeze_pair_run_config`.
It contains data only; runtime clients are rebuilt from it for every process
start, so a resumed run cannot inherit a mutable catalog or a Python object.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any

from .temporal_bidirectional_genome import FrozenModule, FrozenPair, IdentitySnapshot, canonical_hold, canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_discovery_validation import DashboardBidirectionalPairCompiler, DashboardV2ModuleValidator, SubprocessCandidateValidator
from .temporal_indicator_learning_v1 import IndicatorLearningRegistry
from .temporal_qd_observability import timed_span, timing_scope
from .temporal_qd_pair_generation import TypedGrammarPairOperator
from .temporal_qd_initial_protection import (
    apply_immigrant_initial_protection,
    apply_initial_protection_plan,
    default_initial_protection_policy,
    enumerate_initial_protection_plans,
    immigrant_initial_protection_selector,
    validate_initial_protection_policy,
)
from .temporal_typed_motif_grammar import (
    ENTRY_ROUTE_DECISION_INDICATOR_CAP,
    ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
    EntryRouteDecisionIndicatorCapError,
    GRAMMAR_SCHEMA,
    GRAMMAR_VERSION,
    REGISTRY,
    GrammarContext,
    GrammarError,
    TypedFragmentGrammar,
    validate_entry_route_decision_indicator_cap,
)

PAIR_RUN_CONFIG_SCHEMA_LEGACY = "temporal_qd_bidirectional_pair_run_config_v1"
PAIR_RUN_CONFIG_SCHEMA = "temporal_qd_bidirectional_pair_run_config_v2"
PAIR_HOLD_POLICY_SCHEMA = "temporal_qd_pair_hold_operator_policy_v2"
PAIR_IMMIGRANT_POLICY_SCHEMA = "temporal_qd_rich_immigrant_construction_policy_v2"
# v2 closes a composition-order hole: an indicator plan is now admitted only
# after it has passed the entry-route decision-indicator cap.  This must bind
# the selector stream and frozen operator identity so a resume never silently
# reinterprets a v1 authority under the stricter construction semantics.
PAIR_IMMIGRANT_BUILDER_VERSION = "temporal_qd_rich_immigrant_builder_v3"

_DEPTH_BUCKETS = (0, 0, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 4)

# The QD operator vocabulary is intentionally finite and M5-specific.  The
# largest market-bar and elapsed-calendar alternatives each cover one week;
# this is a search exposure, not a global validity cap for native seed plans.
_HOLD_OPERATOR_CHOICES = (
    {"kind": "none"},
    {"kind": "market_bars", "bars": 1, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 3, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 6, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 12, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 24, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 48, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 96, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 288, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 576, "timeframe": "M5"},
    {"kind": "market_bars", "bars": 2016, "timeframe": "M5"},
    {"kind": "elapsed_calendar", "hours": 1.0},
    {"kind": "elapsed_calendar", "hours": 4.0},
    {"kind": "elapsed_calendar", "hours": 8.0},
    {"kind": "elapsed_calendar", "hours": 24.0},
    {"kind": "elapsed_calendar", "hours": 48.0},
    {"kind": "elapsed_calendar", "hours": 72.0},
    {"kind": "elapsed_calendar", "hours": 168.0},
)


def default_hold_operator_policy() -> dict[str, Any]:
    """The only hold mutation vocabulary admitted for a frozen pair run."""

    return {
        "schemaVersion": PAIR_HOLD_POLICY_SCHEMA,
        "enabled": True,
        "allowedKinds": ["none", "market_bars", "elapsed_calendar"],
        "choices": [_clone(choice, name="default hold operator choice") for choice in _HOLD_OPERATOR_CHOICES],
    }


def default_immigrant_construction_policy() -> dict[str, Any]:
    """Frozen breadth and fail-fast policy for random pair immigrants.

    The 2/7/6/4/1 depth buckets preserve simple candidates while giving most
    immigrants multiple opportunities to leave the authored seed roots.  An
    operator family is selected before a concrete plan so a large family does
    not crowd smaller but equally meaningful mutation surfaces out of search.
    """

    return {
        "schemaVersion": PAIR_IMMIGRANT_POLICY_SCHEMA,
        "builderVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
        "selector": "sha256_length_prefixed_rejection_uniform_v1",
        "sideSelection": "independent_long_short_v1",
        "seedResources": ["seedName", "evidenceGroup", "eventBinding", "managementPlan", "initialProtection"],
        "grammarMutationDepthBuckets": list(_DEPTH_BUCKETS),
        "grammarSelection": "uniform_available_operation_family_then_plan_v1",
        "indicatorMutationDepthBuckets": list(_DEPTH_BUCKETS),
        "indicatorSelection": "uniform_available_operator_then_plan_v1",
        "holdSelection": "uniform_frozen_hold_choice_v1",
        "initialProtectionSelection": "uniform_mode_then_uniform_coarse_values_v1",
        "nativeAdmission": "compose_then_validate_once_per_side_v2",
        "entryRouteCapPlanAdmission": "reject_invalid_indicator_preview_then_continue_v1",
        "capacityAdmission": {
            "selectorProbeSampleCount": 8192,
            "minimumUniqueSelectorFingerprints": 4096,
            "minimumExpressibleCapacityMultiplier": 4,
        },
        "collisionTripwire": {
            "minimumImmigrantAttempts": 512,
            "minimumAcceptedRatio": 0.25,
        },
    }


def _immigrant_construction_policy(value: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(value, name="pair immigrant construction policy")
    expected = default_immigrant_construction_policy()
    if raw != expected:
        raise TemporalDiscoveryContractError(
            "pair immigrant construction policy is not the closed admitted policy"
        )
    return raw


def _selector_index(seed: str, *, axis: str, size: int) -> int:
    """Deterministically rejection-sample one finite axis without modulo bias."""

    if size < 1:
        raise TemporalDiscoveryContractError(
            f"pair immigrant selector axis has no values: {axis}"
        )
    limit = (1 << 256) - ((1 << 256) % size)
    attempt = 0
    while True:
        seed_token = str(seed).encode("utf-8")
        axis_token = str(axis).encode("utf-8")
        material = (
            len(seed_token).to_bytes(4, "big")
            + seed_token
            + len(axis_token).to_bytes(4, "big")
            + axis_token
            + attempt.to_bytes(8, "big")
        )
        value = int.from_bytes(hashlib.sha256(material).digest(), "big")
        if value < limit:
            return value % size
        attempt += 1


def _selector_value(seed: str, *, axis: str, values: Sequence[Any]) -> Any:
    if not values:
        raise TemporalDiscoveryContractError(
            f"pair immigrant selector axis has no values: {axis}"
        )
    return values[_selector_index(seed, axis=axis, size=len(values))]


def _hold_operator_policy(value: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(value, name="pair hold operator policy")
    expected = default_hold_operator_policy()
    if raw != expected:
        raise TemporalDiscoveryContractError("pair hold operator policy is not the closed admitted policy")
    # Keep the native type seam local and fail closed if a future edit changes
    # a choice without matching the Dashboard hold-policy schema.
    if [canonical_hold(choice) for choice in raw["choices"]] != raw["choices"]:
        raise TemporalDiscoveryContractError("pair hold operator policy choices are not canonical Dashboard hold policies")
    return raw


def _mutable_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _mutable_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable_json(item) for item in value]
    return value


def _clone(value: Any, *, name: str) -> Any:
    try:
        import json
        return json.loads(canonical_json(_mutable_json(value)))
    except Exception as exc:
        raise TemporalDiscoveryContractError(f"{name} must be finite canonical JSON") from exc


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return _clone(dict(value), name=name)


def _transport(value: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(value, name="pair native JSONL authority")
    required = {"command", "timeoutSeconds", "persistentJsonl", "maxLineBytes", "stderrLimitBytes", "interpreterPath", "validatorScriptPath", "dashboardSourceRoot", "environment"}
    if set(raw) != required or raw["persistentJsonl"] is not True:
        raise TemporalDiscoveryContractError("pair native authority must be an exact persistent JSONL contract")
    command = raw["command"]
    if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
        raise TemporalDiscoveryContractError("pair native authority command must be a non-empty string list")
    if any(isinstance(raw[key], bool) or not isinstance(raw[key], (int, float)) for key in ("timeoutSeconds", "maxLineBytes", "stderrLimitBytes")):
        raise TemporalDiscoveryContractError("pair native authority limits must be numeric")
    if not all(isinstance(raw[key], str) and raw[key] for key in ("interpreterPath", "validatorScriptPath", "dashboardSourceRoot")):
        raise TemporalDiscoveryContractError("pair native authority executable/script/source paths are required")
    environment = _mapping(raw["environment"], name="pair native environment")
    if set(environment) != {"PYTHONPATH"} or not isinstance(environment["PYTHONPATH"], list) or not environment["PYTHONPATH"] or not all(isinstance(item, str) and item for item in environment["PYTHONPATH"]):
        raise TemporalDiscoveryContractError("pair native environment must close the required PYTHONPATH contract")
    return raw


def _file_sha(path: Path, *, name: str) -> str:
    if not path.is_file():
        raise TemporalDiscoveryContractError(f"pair native {name} is unavailable")
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _authority_content(transport: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve file/code identities locally; never trust caller-supplied hashes."""
    executable = Path(str(transport["interpreterPath"])).resolve()
    script = Path(str(transport["validatorScriptPath"])).resolve()
    source = Path(str(transport["dashboardSourceRoot"])).resolve()
    command = [str(item) for item in transport["command"]]
    if not source.is_dir() or Path(command[0]).resolve() != executable or Path(command[1]).resolve() != script:
        raise TemporalDiscoveryContractError("pair native command does not bind its interpreter and validator script paths")
    python_path = [str(Path(item).resolve()) for item in transport["environment"]["PYTHONPATH"]]
    if any(not Path(item).is_dir() for item in python_path):
        raise TemporalDiscoveryContractError("pair native PYTHONPATH entry is unavailable")
    try:
        version = subprocess.run([str(executable), "--version"], check=True, capture_output=True, text=True, timeout=10).stdout.strip() or subprocess.run([str(executable), "--version"], check=True, capture_output=True, text=True, timeout=10).stderr.strip()
        commit = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True, timeout=10).stdout.strip().lower()
    except (OSError, subprocess.SubprocessError) as exc:
        raise TemporalDiscoveryContractError("could not resolve local pair native authority identity") from exc
    if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise TemporalDiscoveryContractError("pair Dashboard source root lacks an exact Git commit")
    core = source / "shared" / "python" / "fuzzfolio_core" / "fuzzfolio_core" / "temporal_graph"
    if not core.is_dir():
        raise TemporalDiscoveryContractError("pair Dashboard temporal_graph package is unavailable")
    files = [script, *sorted(core.rglob("*.py"))]
    manifest = [{"path": str(item.resolve().relative_to(source)), "sha256": _file_sha(item, name="authority source")} for item in files]
    content = {"schemaVersion": "temporal_qd_pair_dashboard_source_manifest_v1", "files": manifest}
    try:
        dirty = subprocess.run(["git", "-C", str(source), "status", "--porcelain", "--", str(script.relative_to(source)), str(core.relative_to(source))], check=True, capture_output=True, text=True, timeout=10).stdout.splitlines()
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        raise TemporalDiscoveryContractError("could not inspect pair Dashboard source provenance") from exc
    return {"schemaVersion": "temporal_qd_pair_native_authority_content_v1", "interpreterPath": str(executable), "interpreterSha256": _file_sha(executable, name="interpreter"), "interpreterVersion": version, "validatorScriptPath": str(script), "validatorScriptSha256": _file_sha(script, name="validator script"), "dashboardSourceRoot": str(source), "dashboardSourceGitCommit": commit, "dashboardSourceDirtyProvenance": dirty, "dashboardTemporalGraphContentManifest": content, "dashboardTemporalGraphContentSha256": canonical_sha256(content), "environment": {"PYTHONPATH": python_path}, "jsonlProtocol": "temporal_search_candidate_validation_jsonl_v1", "validateOperation": "validate_candidate", "compileOperation": "compile_bidirectional", "validateRequestSchema": "temporal_search_candidate_validation_jsonl_request_v1", "compileRequestSchema": "temporal_search_bidirectional_compile_jsonl_request_v1", "compileResponseSchema": "temporal_search_bidirectional_compile_jsonl_response_v1"}


def _bound_transport(value: Mapping[str, Any]) -> dict[str, Any]:
    transport = _transport(value)
    return {**transport, "authorityContent": _authority_content(transport)}


def _registry_identity() -> dict[str, Any]:
    rows = []
    for production_id, spec in sorted(REGISTRY.items()):
        rows.append({"productionId": production_id, "family": spec.family, "consumes": spec.consumes.value, "produces": spec.produces.value, "resourceSlots": list(spec.resource_slots), "choiceDomains": _clone(spec.choice_domains, name="grammar choice domains")})
    result = {
        "schemaVersion": "temporal_typed_fragment_registry_identity_v1",
        "grammarSchema": GRAMMAR_SCHEMA,
        "grammarVersion": GRAMMAR_VERSION,
        "entryRouteDecisionIndicatorPolicy": {
            "semanticVersion": ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
            "maxDistinctDecisionIndicatorInstances": ENTRY_ROUTE_DECISION_INDICATOR_CAP,
        },
        "productions": rows,
    }
    result["registrySha256"] = canonical_sha256(result)
    return result


def _side(raw: Mapping[str, Any], direction: str) -> dict[str, Any]:
    value = _mapping(raw, name=f"{direction} pair module seed")
    if set(value) != {"seedNames", "context", "catalog", "policy"}:
        raise TemporalDiscoveryContractError("pair module seed fields are not exact")
    names = value["seedNames"]
    if not isinstance(names, list) or not names or any(item not in {"mean_reversion", "breakout", "trend"} for item in names):
        raise TemporalDiscoveryContractError("pair module seed names are not an admitted grammar vocabulary")
    # Constructing once is a strict shape check, and normalized context is what
    # the grammar itself consumes on every fresh/restart reconstruction.
    context = GrammarContext(
        instrument=str(_mapping(value["context"], name="pair grammar context").get("instrument") or ""),
        indicators=tuple(_mapping(value["context"], name="pair grammar context").get("indicators") or ()),
        evidence_groups=tuple(_mapping(value["context"], name="pair grammar context").get("evidenceGroups") or ()),
        event_bindings=tuple(_mapping(value["context"], name="pair grammar context").get("eventBindings") or ()),
        execution_config=_mapping(value["context"], name="pair grammar context").get("executionConfig") or {},
        budgets=_mapping(value["context"], name="pair grammar context").get("budgets"),
    ).normalized()
    catalog = _mapping(value["catalog"], name="pair indicator catalog")
    registry = IndicatorLearningRegistry(catalog)
    policy = _mapping(value["policy"], name="pair module policy")
    return {"seedNames": sorted(set(names)), "context": context, "catalog": catalog, "catalogSha256": registry.catalog.catalog_sha256, "indicatorPolicy": registry.policy, "policy": policy}


def _immigrant_selector_axes(frozen: Mapping[str, Any]) -> dict[str, Any]:
    hold_choices = tuple(
        _clone(item, name="pair immigrant hold selector choice")
        for item in frozen["holdOperatorPolicy"]["choices"]
    )
    sides: dict[str, Any] = {}
    for direction in ("long", "short"):
        side = frozen[f"{direction}Module"]
        context = side["context"]
        sides[direction] = {
            "seedNames": tuple(str(item) for item in side["seedNames"]),
            "groupIds": tuple(str(item["id"]) for item in context["groups"]),
            "eventIds": tuple(str(item["id"]) for item in context["events"]),
            "planIds": tuple(str(item) for item in context["plans"]),
        }
    result = {
        "sides": sides,
        "holdChoices": hold_choices,
    }
    if "initialProtectionOperatorPolicy" in frozen:
        result["initialProtectionPolicy"] = validate_initial_protection_policy(
            _mapping(
                frozen["initialProtectionOperatorPolicy"],
                name="pair initial protection operator policy",
            )
        )
    return result


def _selector_fingerprint_from_axes(
    axes: Mapping[str, Any], proposal_seed: str
) -> dict[str, Any]:
    """Cheap, no-native fingerprint of independently selectable root axes."""

    result: dict[str, Any] = {}
    hold_choices = axes["holdChoices"]
    protection_policy = axes.get("initialProtectionPolicy")
    for direction in ("long", "short"):
        side = axes["sides"][direction]
        side_seed = canonical_sha256(
            {
                "schemaVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
                "proposalSeed": str(proposal_seed),
                "side": direction,
            }
        )
        result[direction] = {
            "seedName": _selector_value(
                side_seed,
                axis="seed_name",
                values=side["seedNames"],
            ),
            "groupId": _selector_value(
                side_seed,
                axis="evidence_group",
                values=side["groupIds"],
            ),
            "eventId": _selector_value(
                side_seed,
                axis="event_binding",
                values=side["eventIds"],
            ),
            "planId": _selector_value(
                side_seed,
                axis="management_plan",
                values=side["planIds"],
            ),
            "hold": _selector_value(
                side_seed,
                axis="hold_policy",
                values=hold_choices,
            ),
        }
        if isinstance(protection_policy, Mapping):
            result[direction]["initialProtection"] = (
                immigrant_initial_protection_selector(
                    policy=protection_policy,
                    choose=_selector_value,
                    seed=side_seed,
                )
            )
    return result


def _selector_fingerprint(
    frozen: Mapping[str, Any], proposal_seed: str
) -> dict[str, Any]:
    return _selector_fingerprint_from_axes(
        _immigrant_selector_axes(frozen), proposal_seed
    )


def immigrant_capacity_audit(
    frozen: Mapping[str, Any], *, required_unique_candidates: int
) -> dict[str, Any]:
    """Prove the cheap independent axes exceed a requested campaign size.

    Grammar and indicator mutations add further entropy but are deliberately
    excluded from the capacity floor.  This keeps admission conservative and
    makes the prior 2,304-pair seed-only failure impossible to hide behind an
    optimistic estimate of structural mutation breadth.
    """

    required = int(required_unique_candidates)
    if required < 1:
        raise TemporalDiscoveryContractError(
            "pair immigrant capacity requires a positive unique-candidate target"
        )
    policy = _immigrant_construction_policy(
        _mapping(
            frozen.get("immigrantConstructionPolicy"),
            name="pair immigrant construction policy",
        )
    )
    hold_count = len(frozen["holdOperatorPolicy"]["choices"])
    protection_count = 1
    if "initialProtectionOperatorPolicy" in frozen:
        initial_protection = validate_initial_protection_policy(
            _mapping(
                frozen["initialProtectionOperatorPolicy"],
                name="pair initial protection operator policy",
            )
        )
        protection_count = len(initial_protection["stopPercentChoices"]) * (
            len(initial_protection["rewardMultipleChoices"])
            + len(initial_protection["targetPercentChoices"])
        )
    side_capacity: dict[str, int] = {}
    side_axes: dict[str, dict[str, int]] = {}
    for direction in ("long", "short"):
        side = _mapping(frozen[f"{direction}Module"], name=f"{direction} pair module")
        context = _mapping(side["context"], name=f"{direction} grammar context")
        axes = {
            "seedNames": len(side["seedNames"]),
            "evidenceGroups": len(context["groups"]),
            "eventBindings": len(context["events"]),
            "managementPlans": len(context["plans"]),
            "holdPolicies": hold_count,
            **(
                {"initialProtectionPlans": protection_count}
                if protection_count > 1
                else {}
            ),
        }
        if any(value < 1 for value in axes.values()):
            raise TemporalDiscoveryContractError(
                f"pair immigrant {direction} selector axis is empty"
            )
        capacity = 1
        for value in axes.values():
            capacity *= value
        side_axes[direction] = axes
        side_capacity[direction] = capacity
    expressible_capacity = side_capacity["long"] * side_capacity["short"]
    capacity_policy = policy["capacityAdmission"]
    required_capacity = (
        required * int(capacity_policy["minimumExpressibleCapacityMultiplier"])
    )
    if expressible_capacity < required_capacity:
        raise TemporalDiscoveryContractError(
            "pair immigrant independently expressible capacity is below the admitted campaign requirement"
        )

    sample_count = int(capacity_policy["selectorProbeSampleCount"])
    config_sha = str(frozen.get("pairRunConfigSha256") or canonical_sha256(frozen))
    selector_axes = _immigrant_selector_axes(frozen)
    fingerprints = {
        canonical_sha256(
            _selector_fingerprint_from_axes(
                selector_axes,
                canonical_sha256(
                    {
                        "schemaVersion": "temporal_qd_immigrant_selector_probe_seed_v1",
                        "pairRunConfigSha256": config_sha,
                        "ordinal": ordinal,
                    }
                ),
            )
        )
        for ordinal in range(sample_count)
    }
    unique_count = len(fingerprints)
    minimum_unique = max(
        required,
        int(capacity_policy["minimumUniqueSelectorFingerprints"]),
    )
    if unique_count < minimum_unique:
        raise TemporalDiscoveryContractError(
            "pair immigrant selector entropy is below the admitted campaign requirement"
        )
    audit = {
        "schemaVersion": "temporal_qd_pair_immigrant_capacity_audit_v1",
        "requiredUniqueCandidates": required,
        "sideAxes": side_axes,
        "sideExpressibleCapacities": side_capacity,
        "pairExpressibleCapacityFloor": expressible_capacity,
        "requiredCapacityFloor": required_capacity,
        "selectorProbeSampleCount": sample_count,
        "uniqueSelectorFingerprintCount": unique_count,
        "selectorCollisionCount": sample_count - unique_count,
        "minimumUniqueSelectorFingerprints": minimum_unique,
        "grammarAndIndicatorEntropyIncludedInCapacityFloor": False,
        "admitted": True,
    }
    audit["auditSha256"] = canonical_sha256(audit)
    return audit


def freeze_pair_run_config(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate external JSON and return the sole persisted pair authority."""
    value = _mapping(raw, name="bidirectional pair run config")
    required = {"schemaVersion", "longModule", "shortModule", "nativeJsonlAuthority", "holdOperatorPolicy"}
    if set(value) != required or value.get("schemaVersion") not in {
        PAIR_RUN_CONFIG_SCHEMA_LEGACY,
        PAIR_RUN_CONFIG_SCHEMA,
    }:
        raise TemporalDiscoveryContractError("bidirectional pair run config fields/schema are not exact")
    hold = _hold_operator_policy(_mapping(value["holdOperatorPolicy"], name="pair hold operator policy"))
    transport = _bound_transport(_mapping(value["nativeJsonlAuthority"], name="pair native JSONL authority"))
    native_snapshot = IdentitySnapshot.create(kind="nativeAuthority", schema_version="temporal_dashboard_jsonl_native_authority_v1", payload=transport)
    compiler_snapshot = IdentitySnapshot.create(kind="pairCompiler", schema_version="temporal_dashboard_jsonl_pair_compiler_v1", payload=transport)
    result = {
        "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
        "longModule": _side(_mapping(value["longModule"], name="long pair module"), "long"),
        "shortModule": _side(_mapping(value["shortModule"], name="short pair module"), "short"),
        "grammarRegistry": _registry_identity(),
        "holdOperatorPolicy": hold,
        "initialProtectionOperatorPolicy": default_initial_protection_policy(),
        "immigrantConstructionPolicy": default_immigrant_construction_policy(),
        "nativeJsonlAuthority": transport,
        "nativeAuthority": native_snapshot.canonical_payload(),
        "pairCompilerAuthority": compiler_snapshot.canonical_payload(),
    }
    result["operatorImplementation"] = {
        "schemaVersion": "temporal_qd_pair_operator_implementation_v4",
        "typedGrammarRegistrySha256": result["grammarRegistry"]["registrySha256"],
        "longIndicatorPolicySha256": result["longModule"]["indicatorPolicy"]["policySha256"],
        "shortIndicatorPolicySha256": result["shortModule"]["indicatorPolicy"]["policySha256"],
        "holdOperatorPolicySha256": canonical_sha256(hold),
        "initialProtectionOperatorPolicySha256": canonical_sha256(
            result["initialProtectionOperatorPolicy"]
        ),
        "richImmigrantBuilderVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
        "richImmigrantConstructionPolicySha256": canonical_sha256(result["immigrantConstructionPolicy"]),
        "entryRouteDecisionIndicatorPolicy": _clone(
            result["grammarRegistry"]["entryRouteDecisionIndicatorPolicy"],
            name="entry route decision-indicator policy",
        ),
        "nativeAuthoritySha256": native_snapshot.sha256,
        "pairCompilerAuthoritySha256": compiler_snapshot.sha256,
    }
    result["pairRunConfigSha256"] = canonical_sha256(result)
    return result


class _Factory:
    def __init__(
        self,
        bundle: "PairAuthorityBundle",
        *,
        cache_immutable_runtime: bool = False,
    ) -> None:
        self.bundle = bundle
        self._cache_immutable_runtime = cache_immutable_runtime
        self.construction_policy = _immigrant_construction_policy(
            bundle.config["immigrantConstructionPolicy"]
        )
        self._selector_axes = _immigrant_selector_axes(bundle.config)

    def optimized_runtime_view(self) -> "_Factory":
        """Return an equivalent factory that reuses sealed side authority.

        The public bundle factory remains the reference path.  This view is
        selected only by the optimized generator and caches objects keyed by
        the complete frozen pair-run authority; it never caches generated
        profiles, plans, or mutable candidate material.
        """

        if self._cache_immutable_runtime:
            return self
        return _Factory(self.bundle, cache_immutable_runtime=True)

    @staticmethod
    def _seeded_order(
        values: Sequence[Any], *, seed: str, axis: str
    ) -> list[Any]:
        return sorted(
            values,
            key=lambda value: canonical_sha256(
                {
                    "schemaVersion": "temporal_qd_immigrant_seeded_order_v1",
                    "seed": seed,
                    "axis": axis,
                    "value": value,
                }
            ),
        )

    def _apply_grammar_steps(
        self, grammar: TypedFragmentGrammar, program: Any, *, side_seed: str
    ) -> tuple[Any, list[dict[str, Any]], int]:
        with timed_span("immigrant.grammar.select_depth") as span:
            depth = int(
                _selector_value(
                    side_seed,
                    axis="grammar_mutation_depth",
                    values=self.construction_policy["grammarMutationDepthBuckets"],
                )
            )
            span.annotate(plannedDepth=depth)
        with timed_span("immigrant.grammar.initialize_tracking"):
            trace: list[dict[str, Any]] = []
            seen = {canonical_sha256(program.canonical())}
        for step in range(depth):
            with timing_scope(grammarStep=step):
                with timed_span("immigrant.grammar.enumerate_operations") as span:
                    plans = (
                        grammar.enumerate_operations(program)
                        if self._cache_immutable_runtime
                        else [
                            dict(item)
                            for item in grammar.enumerate_operations(program)
                        ]
                    )
                    span.annotate(planCount=len(plans))
                with timed_span("immigrant.grammar.order_operation_families") as span:
                    families = self._seeded_order(
                        sorted({str(item["operation"]) for item in plans}),
                        seed=side_seed,
                        axis=f"grammar_family_{step}",
                    )
                    span.annotate(familyCount=len(families))
            applied = False
            for family in families:
                with timed_span(
                    "immigrant.grammar.order_family_plans",
                    grammarStep=step,
                    operationFamily=family,
                ) as span:
                    family_plans = self._seeded_order(
                        [item for item in plans if item["operation"] == family],
                        seed=side_seed,
                        axis=f"grammar_plan_{step}_{family}",
                    )
                    span.annotate(planCount=len(family_plans))
                for plan in family_plans:
                    try:
                        with timed_span(
                            "immigrant.grammar.apply_plan",
                            grammarStep=step,
                            operationFamily=family,
                            planSha256=canonical_sha256(plan),
                        ):
                            child = grammar.apply(program, plan)
                    except GrammarError:
                        continue
                    with timed_span(
                        "immigrant.grammar.hash_child",
                        grammarStep=step,
                        operationFamily=family,
                    ):
                        child_sha = canonical_sha256(child.canonical())
                    if child_sha in seen:
                        continue
                    program = child
                    seen.add(child_sha)
                    trace.append(
                        {
                            "step": step,
                            "operationFamily": family,
                            "plan": _clone(plan, name="rich immigrant grammar plan"),
                            "planSha256": canonical_sha256(plan),
                            "childProgramSha256": child_sha,
                        }
                    )
                    applied = True
                    break
                if applied:
                    break
            if not applied:
                break
        return program, trace, depth

    def _apply_indicator_steps(
        self,
        registry: IndicatorLearningRegistry,
        profile: Mapping[str, Any],
        *,
        side_seed: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], int, dict[str, Any]]:
        with timed_span("immigrant.indicator.select_depth") as span:
            depth = int(
                _selector_value(
                    side_seed,
                    axis="indicator_mutation_depth",
                    values=self.construction_policy["indicatorMutationDepthBuckets"],
                )
            )
            span.annotate(plannedDepth=depth)
        with timed_span("immigrant.indicator.initialize_tracking"):
            current = (
                profile
                if self._cache_immutable_runtime
                else _clone(profile, name="rich immigrant indicator parent")
            )
            trace: list[dict[str, Any]] = []
            seen = {canonical_sha256(current)}
            cap_rejected_plan_rows: list[dict[str, Any]] = []
        for step in range(depth):
            with timing_scope(indicatorStep=step):
                with timed_span("immigrant.indicator.enumerate_plans") as span:
                    plans = (
                        registry.enumerate_plans(current)
                        if self._cache_immutable_runtime
                        else [
                            dict(item)
                            for item in registry.enumerate_plans(current)
                        ]
                    )
                    span.annotate(planCount=len(plans))
                with timed_span("immigrant.indicator.order_operators") as span:
                    operators = self._seeded_order(
                        sorted({str(item["operatorId"]) for item in plans}),
                        seed=side_seed,
                        axis=f"indicator_operator_{step}",
                    )
                    span.annotate(operatorCount=len(operators))
            applied = False
            for operator_id in operators:
                with timed_span(
                    "immigrant.indicator.resolve_and_order_operator_plans",
                    indicatorStep=step,
                    operatorId=operator_id,
                ) as span:
                    operator = registry.get(operator_id)
                    operator_plans = self._seeded_order(
                        [item for item in plans if item["operatorId"] == operator_id],
                        seed=side_seed,
                        axis=f"indicator_plan_{step}_{operator_id}",
                    )
                    span.annotate(planCount=len(operator_plans))
                for plan in operator_plans:
                    try:
                        with timed_span(
                            "immigrant.indicator.preview_plan",
                            indicatorStep=step,
                            operatorId=operator_id,
                            planSha256=plan["planSha256"],
                        ):
                            child = operator.preview(current, plan)
                    except TemporalDiscoveryContractError:
                        continue
                    # A raw event/trigger can otherwise turn a route with
                    # three fuzzy members into an illegal four-indicator
                    # conjunction.  Test the preview before admitting it as
                    # the next parent so a deterministic later plan remains
                    # eligible.  The final module-level check below remains
                    # the fail-closed backstop for every composition seam.
                    try:
                        with timed_span(
                            "immigrant.indicator.validate_entry_route_cap",
                            indicatorStep=step,
                            operatorId=operator_id,
                            planSha256=plan["planSha256"],
                        ):
                            validate_entry_route_decision_indicator_cap(child)
                    except EntryRouteDecisionIndicatorCapError:
                        cap_rejected_plan_rows.append(
                            {
                                "step": step,
                                "operatorId": operator_id,
                                "planSha256": plan["planSha256"],
                            }
                        )
                        continue
                    with timed_span(
                        "immigrant.indicator.hash_child",
                        indicatorStep=step,
                        operatorId=operator_id,
                    ):
                        child_sha = canonical_sha256(child)
                    if child_sha in seen:
                        continue
                    current = child
                    seen.add(child_sha)
                    construction = plan.get("construction")
                    trace.append(
                        {
                            "step": step,
                            "operatorId": operator_id,
                            "constructionKind": (
                                construction.get("kind")
                                if isinstance(construction, Mapping)
                                else None
                            ),
                            "planSha256": plan["planSha256"],
                            "childProfileSha256": child_sha,
                        }
                    )
                    applied = True
                    break
                if applied:
                    break
            if not applied:
                break
        cap_rejections = {
            "count": len(cap_rejected_plan_rows),
            "rowsSha256": canonical_sha256(cap_rejected_plan_rows),
        }
        return current, trace, depth, cap_rejections

    @staticmethod
    def _apply_hold(
        profile: Mapping[str, Any],
        *,
        plan_id: str,
        hold: Mapping[str, Any],
        copy_profile: bool = True,
    ) -> dict[str, Any]:
        child = (
            _clone(profile, name="rich immigrant hold parent")
            if copy_profile
            else profile
        )
        if not isinstance(child, dict):
            raise TemporalDiscoveryContractError(
                "rich immigrant hold parent must be an owned JSON object"
            )
        plans = (((child.get("executionConfig") or {}).get("managementLibrary") or {}).get("plans") or [])
        selected = [item for item in plans if isinstance(item, Mapping) and item.get("id") == plan_id]
        if len(selected) != 1:
            raise TemporalDiscoveryContractError(
                "rich immigrant hold selector did not resolve one management plan"
            )
        canonical = canonical_hold(hold)
        if canonical["kind"] == "none":
            selected[0].pop("holdPolicy", None)
        else:
            selected[0]["holdPolicy"] = canonical
        return child

    def _apply_dynamic_initial_protection(
        self,
        profile: Mapping[str, Any],
        *,
        side: Mapping[str, Any],
        side_seed: str,
        selector: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], Mapping[str, Any] | None]:
        """Use the canonical scalar-construction transaction for G0 dynamic mode.

        The parent and child validations are deliberately explicit: the
        construction operator binds both program identities into its audit.
        When a seed simply has no authorized completed-bar scalar, retain an
        auditable static fallback rather than pretending dynamic was sampled.
        """

        from .temporal_operator_construction_v3 import (
            ConstructionCatalog,
            ScalarDynamicManagementConstructionOperator,
        )

        desired_site = str(selector.get("dynamicSite") or "")
        if desired_site not in {"initial_stop", "initial_target"}:
            raise TemporalDiscoveryContractError("dynamic initial protection site is invalid")
        operator = ScalarDynamicManagementConstructionOperator(
            ConstructionCatalog(side["catalog"])
        )
        plans = [
            plan
            for plan in operator.enumerate_plans(profile)
            if isinstance(plan.get("construction"), Mapping)
            and plan["construction"].get("site") == desired_site
        ]
        if not plans:
            policy = self.bundle.config["initialProtectionOperatorPolicy"]
            fallback = {
                "mode": "coupled_reward_multiple",
                "stopPercent": _selector_value(
                    side_seed,
                    axis="initial_protection_dynamic_fallback_stop_percent",
                    values=policy["stopPercentChoices"],
                ),
                "rewardMultiple": _selector_value(
                    side_seed,
                    axis="initial_protection_dynamic_fallback_reward_multiple",
                    values=policy["rewardMultipleChoices"],
                ),
            }
            child, audit = apply_immigrant_initial_protection(
                profile,
                plan_id=str(selector["planId"]),
                selector=fallback,
                policy=self.bundle.config["initialProtectionOperatorPolicy"],
            )
            audit["dynamicDisposition"] = "deferred_no_catalog_authorized_completed_bar_scalar"
            audit["requestedDynamicSite"] = desired_site
            audit["applicationSha256"] = canonical_sha256(
                {key: value for key, value in audit.items() if key != "applicationSha256"}
            )
            return child, audit, None
        selected = _selector_value(
            side_seed,
            axis="initial_protection_dynamic_construction_plan",
            values=sorted(plans, key=canonical_sha256),
        )
        parent_report = self.bundle.validator.validate_v2(
            profile=dict(profile),
            candidate_id="qd_rich_dynamic_parent_" + side_seed[7:35],
        )
        preview = operator.preview(profile, selected)
        child_report = self.bundle.validator.validate_v2(
            profile=preview,
            candidate_id="qd_rich_dynamic_child_" + side_seed[7:35],
        )
        child, application = operator.apply(
            profile,
            selected,
            parent_validated_program_sha256=parent_report["programSha256"],
            child_validated_program_sha256=child_report["programSha256"],
        )
        if child != preview:
            raise TemporalDiscoveryContractError(
                "dynamic initial protection construction preview/application diverged"
            )
        construction = selected["construction"]
        plan_id = str(construction["planId"])
        locator_site = "stop" if desired_site == "initial_stop" else "target"
        selected_plan = next(
            item
            for item in child["executionConfig"]["managementLibrary"]["plans"]
            if item.get("id") == plan_id
        )
        locator = selected_plan[
            "initialStop" if locator_site == "stop" else "initialTarget"
        ]
        multiplier_audit: Mapping[str, Any] | None = None
        if locator.get("kind") == "indicator_distance_multiple":
            desired_multiple = _selector_value(
                side_seed,
                axis="initial_protection_dynamic_distance_multiple",
                values=self.bundle.config["initialProtectionOperatorPolicy"][
                    "distanceMultipleChoices"
                ],
            )
            replacement = {
                "kind": "indicator_distance_multiple",
                "bindingId": locator["bindingId"],
                "multiple": desired_multiple,
            }
            if replacement != locator:
                adjustment = next(
                    item
                    for item in enumerate_initial_protection_plans(
                        child, self.bundle.config["initialProtectionOperatorPolicy"]
                    )
                    if item["planId"] == plan_id
                    and item["site"] == locator_site
                    and item["replacement"] == replacement
                )
                child, multiplier_audit = apply_initial_protection_plan(
                    child,
                    adjustment,
                    self.bundle.config["initialProtectionOperatorPolicy"],
                )
                child_report = self.bundle.validator.validate_v2(
                    profile=child,
                    candidate_id="qd_rich_dynamic_adjusted_" + side_seed[7:35],
                )
            else:
                multiplier_audit = {
                    "schemaVersion": "temporal_qd_initial_protection_dynamic_grid_v1",
                    "selectedMultiple": desired_multiple,
                    "disposition": "already_selected_by_construction",
                }
        audit = {
            "schemaVersion": "temporal_qd_initial_protection_immigrant_dynamic_v1",
            "requestedDynamicSite": desired_site,
            "dynamicDisposition": "materialized",
            "constructionPlanSha256": selected["planSha256"],
            "application": application,
            **(
                {"distanceMultipleApplication": multiplier_audit}
                if multiplier_audit is not None
                else {}
            ),
        }
        audit["applicationSha256"] = canonical_sha256(audit)
        return child, audit, child_report

    def _construct_module(
        self,
        direction: str,
        proposal_seed: str,
        *,
        selector: Mapping[str, Any],
    ) -> FrozenModule:
        with timed_span("immigrant.side.resolve_authority"):
            runtime = (
                self.bundle.immigrant_runtime(direction)
                if self._cache_immutable_runtime
                else None
            )
            side = runtime.side if runtime is not None else self.bundle._side(direction)
            side_seed = canonical_sha256(
                {
                    "schemaVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
                    "proposalSeed": str(proposal_seed),
                    "side": direction,
                }
            )
        with timed_span("immigrant.side.identity_snapshots"):
            if runtime is not None:
                context_id = runtime.grammar_context
                catalog_id = runtime.catalog
                policy_id = runtime.policy
            else:
                context_id = IdentitySnapshot.create(
                    kind="grammarContext",
                    schema_version="temporal_typed_grammar_context_v1",
                    payload=side["context"],
                )
                catalog_id = IdentitySnapshot.create(
                    kind="catalog",
                    schema_version="temporal_indicator_learning_catalog_v1",
                    payload={"catalog": side["catalog"], "catalogSha256": side["catalogSha256"]},
                )
                policy_id = IdentitySnapshot.create(
                    kind="policy",
                    schema_version="temporal_qd_pair_module_policy_v2",
                    payload={
                        "modulePolicy": side["policy"],
                        "indicatorPolicy": side["indicatorPolicy"],
                        "holdOperatorPolicy": self.bundle.config["holdOperatorPolicy"],
                        "initialProtectionOperatorPolicy": self.bundle.config[
                            "initialProtectionOperatorPolicy"
                        ],
                        "immigrantConstructionPolicy": self.construction_policy,
                    },
                )
        with timed_span("immigrant.grammar.initialize"):
            grammar = (
                runtime.grammar
                if runtime is not None
                else TypedFragmentGrammar(
                    GrammarContext(
                        instrument=side["context"]["instrument"],
                        indicators=tuple(side["context"]["indicators"]),
                        evidence_groups=tuple(side["context"]["groups"]),
                        event_bindings=tuple(side["context"]["events"]),
                        execution_config=side["context"]["executionConfig"],
                        budgets=side["context"]["budgets"],
                    ),
                    native_authority=self.bundle.validator,
                )
            )
        with timed_span("immigrant.grammar.seed"):
            program = grammar.seed(
                direction=direction,
                name=str(selector["seedName"]),
                group_id=str(selector["groupId"]),
                event_id=str(selector["eventId"]),
                plan_id=str(selector["planId"]),
            )
        program, grammar_trace, planned_grammar_depth = self._apply_grammar_steps(
            grammar, program, side_seed=side_seed
        )
        with timed_span("immigrant.grammar.materialize_profile"):
            profile = grammar.materialize_profile(program)
        with timed_span("immigrant.indicator.initialize_registry"):
            registry = (
                runtime.registry
                if runtime is not None
                else IndicatorLearningRegistry(side["catalog"])
            )
        (
            profile,
            indicator_trace,
            planned_indicator_depth,
            indicator_cap_rejections,
        ) = self._apply_indicator_steps(
            registry, profile, side_seed=side_seed
        )
        with timed_span("immigrant.hold.apply"):
            profile = self._apply_hold(
                profile,
                plan_id=str(selector["planId"]),
                hold=_mapping(selector["hold"], name="rich immigrant selected hold"),
                copy_profile=runtime is None,
            )
        with timed_span("immigrant.initial_protection.apply"):
            protection_selector = _mapping(
                selector["initialProtection"],
                name="rich immigrant initial protection selector",
            )
            protection_selector["planId"] = str(selector["planId"])
            final_native_report: Mapping[str, Any] | None = None
            if protection_selector["mode"] == "dynamic_catalog_authorized":
                profile, initial_protection_audit, final_native_report = (
                    self._apply_dynamic_initial_protection(
                        profile,
                        side=side,
                        side_seed=side_seed,
                        selector=protection_selector,
                    )
                )
            else:
                profile, initial_protection_audit = apply_immigrant_initial_protection(
                    profile,
                    plan_id=str(selector["planId"]),
                    selector=protection_selector,
                    policy=self.bundle.config["initialProtectionOperatorPolicy"],
                )
        # Indicator and hold operations occur after grammar materialization.
        # Recheck the entry-route cap before this path can freeze a module.
        with timed_span("immigrant.entry_route_indicator_cap.validate"):
            entry_route_cap_report = validate_entry_route_decision_indicator_cap(profile)
        with timed_span("immigrant.side.build_construction_audit"):
            graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
            groups = graph.get("evidenceGroups") if isinstance(graph, Mapping) else []
            audit = {
                "schemaVersion": "temporal_qd_rich_immigrant_module_construction_v1",
                "builderVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
                "side": direction,
                "proposalSeed": str(proposal_seed),
                "selector": _clone(selector, name="rich immigrant selector"),
                "grammar": {
                    "plannedDepth": planned_grammar_depth,
                    "appliedDepth": len(grammar_trace),
                    "steps": grammar_trace,
                },
                "indicator": {
                    "plannedDepth": planned_indicator_depth,
                    "appliedDepth": len(indicator_trace),
                    "steps": indicator_trace,
                    "entryRouteCapRejectedPlanCount": indicator_cap_rejections[
                        "count"
                    ],
                    "entryRouteCapRejectedPlanRowsSha256": indicator_cap_rejections[
                        "rowsSha256"
                    ],
                },
                "initialProtection": initial_protection_audit,
                "entryRouteDecisionIndicatorReportSha256": canonical_sha256(
                    entry_route_cap_report
                ),
                "profileShape": {
                    "fragmentCount": len(program.fragments),
                    "indicatorCount": len(profile.get("indicators") or []),
                    "evidenceGroupMemberCounts": sorted(
                        len(item.get("indicatorInstanceIds") or [])
                        for item in (groups or [])
                        if isinstance(item, Mapping)
                    ),
                    "holdKind": canonical_hold(selector["hold"])["kind"],
                    "initialProtectionMode": selector["initialProtection"]["mode"],
                },
            }
            audit["auditSha256"] = canonical_sha256(audit)
        with timed_span("immigrant.side.native_validate_and_freeze"):
            if final_native_report is not None:
                return FrozenModule.freeze(
                    program=program.canonical(),
                    profile=profile,
                    grammar_context=context_id,
                    catalog=catalog_id,
                    policy=policy_id,
                    native_authority=self.bundle.native_identity,
                    native_report=final_native_report,
                    lineage=[
                        {"operation": "typed_seed", "side": direction, "seedName": selector["seedName"], "groupId": selector["groupId"], "eventId": selector["eventId"], "planId": selector["planId"], "proposalSeed": str(proposal_seed)},
                        {"operation": "rich_immigrant_construction", "side": direction, "audit": audit},
                    ],
                )
            return FrozenModule.validate_native(
                program=program.canonical(),
                profile=profile,
                grammar_context=context_id,
                catalog=catalog_id,
                policy=policy_id,
                native_authority_identity=self.bundle.native_identity,
                native_validator=self.bundle.validator,
                candidate_id="qd_rich_module_" + side_seed[7:35],
                lineage=[
                    {
                        "operation": "typed_seed",
                        "side": direction,
                        "seedName": selector["seedName"],
                        "groupId": selector["groupId"],
                        "eventId": selector["eventId"],
                        "planId": selector["planId"],
                        "proposalSeed": str(proposal_seed),
                    },
                    {
                        "operation": "rich_immigrant_construction",
                        "side": direction,
                        "audit": audit,
                    },
                ],
            )

    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        with timed_span("immigrant.pair.select_axes"):
            selectors = _selector_fingerprint_from_axes(
                self._selector_axes, proposal_seed
            )
        with timing_scope(side="long"):
            with timed_span("immigrant.side.total"):
                long = self._construct_module(
                    "long", proposal_seed, selector=selectors["long"]
                )
        with timing_scope(side="short"):
            with timed_span("immigrant.side.total"):
                short = self._construct_module(
                    "short", proposal_seed, selector=selectors["short"]
                )
        with timed_span("immigrant.pair.build_lineage"):
            lineage = []
            for module in (long, short):
                construction = next(
                    item
                    for item in reversed(module.lineage)
                    if item.get("operation") == "rich_immigrant_construction"
                )
                lineage.append(
                    {
                        "operation": "rich_immigrant_construction",
                        "side": module.direction,
                        "proposalSeed": str(proposal_seed),
                        "constructionAuditSha256": construction["audit"]["auditSha256"],
                    }
                )
        with timed_span("immigrant.pair.native_compile_and_freeze"):
            return FrozenPair.compile(
                long=long,
                short=short,
                pair_compiler_identity=self.bundle.compiler_identity,
                pair_compiler=self.bundle.compiler,
                candidate_id="qd_rich_pair_" + canonical_sha256({"seed": proposal_seed})[7:35],
                side_targeted_lineage=lineage,
            )

    def audit_pair(self, pair: FrozenPair) -> dict[str, Any]:
        with timed_span("immigrant.pair.audit_construction"):
            sides: dict[str, Any] = {}
            for module in (pair.long, pair.short):
                construction = next(
                    (
                        item.get("audit")
                        for item in reversed(module.lineage)
                        if item.get("operation") == "rich_immigrant_construction"
                    ),
                    None,
                )
                if not isinstance(construction, Mapping):
                    raise TemporalDiscoveryContractError(
                        "rich immigrant pair lacks a module construction audit"
                    )
                sides[module.direction] = _clone(
                    construction, name="rich immigrant module construction audit"
                )
            audit = {
                "schemaVersion": "temporal_qd_rich_immigrant_pair_construction_v1",
                "pairIdentitySha256": pair.identity_sha256,
                "sides": sides,
            }
            audit["auditSha256"] = canonical_sha256(audit)
            return audit


@dataclass(frozen=True)
class _ImmigrantSideRuntime:
    """Reusable, immutable side authority for optimized immigrants only."""

    authority_key: str
    side: Mapping[str, Any]
    grammar_context: IdentitySnapshot
    catalog: IdentitySnapshot
    policy: IdentitySnapshot
    grammar: TypedFragmentGrammar
    registry: IndicatorLearningRegistry


class PairAuthorityBundle:
    def __init__(self, frozen: Mapping[str, Any]) -> None:
        data = _mapping(frozen, name="frozen pair run config")
        supplied = data.pop("pairRunConfigSha256", None)
        if supplied != canonical_sha256(data) or data.get("schemaVersion") != PAIR_RUN_CONFIG_SCHEMA:
            raise TemporalDiscoveryContractError("pair run config identity/schema mismatch")
        data["holdOperatorPolicy"] = _hold_operator_policy(
            _mapping(data.get("holdOperatorPolicy"), name="frozen pair hold operator policy")
        )
        data["initialProtectionOperatorPolicy"] = validate_initial_protection_policy(
            _mapping(
                data.get("initialProtectionOperatorPolicy"),
                name="frozen pair initial protection operator policy",
            )
        )
        data["immigrantConstructionPolicy"] = _immigrant_construction_policy(
            _mapping(
                data.get("immigrantConstructionPolicy"),
                name="frozen pair immigrant construction policy",
            )
        )
        if data.get("grammarRegistry") != _registry_identity():
            raise TemporalDiscoveryContractError("frozen typed grammar registry implementation drifted")
        for direction in ("long", "short"):
            side = _mapping(data.get(f"{direction}Module"), name=f"frozen {direction} module")
            if IndicatorLearningRegistry(side.get("catalog") or {}).catalog.catalog_sha256 != side.get("catalogSha256"):
                raise TemporalDiscoveryContractError("frozen indicator catalog identity drifted")
            if IndicatorLearningRegistry(side.get("catalog") or {}).policy != side.get("indicatorPolicy"):
                raise TemporalDiscoveryContractError("frozen indicator operator implementation drifted")
        stored_transport = _mapping(data.get("nativeJsonlAuthority"), name="frozen pair native authority")
        raw_transport = {key: value for key, value in stored_transport.items() if key != "authorityContent"}
        if _bound_transport(raw_transport) != stored_transport:
            raise TemporalDiscoveryContractError("frozen pair native authority content drifted")
        expected_operator = {
            "schemaVersion": "temporal_qd_pair_operator_implementation_v4",
            "typedGrammarRegistrySha256": data["grammarRegistry"]["registrySha256"],
            "longIndicatorPolicySha256": data["longModule"]["indicatorPolicy"]["policySha256"],
            "shortIndicatorPolicySha256": data["shortModule"]["indicatorPolicy"]["policySha256"],
            "holdOperatorPolicySha256": canonical_sha256(data["holdOperatorPolicy"]),
            "initialProtectionOperatorPolicySha256": canonical_sha256(
                data["initialProtectionOperatorPolicy"]
            ),
            "richImmigrantBuilderVersion": PAIR_IMMIGRANT_BUILDER_VERSION,
            "richImmigrantConstructionPolicySha256": canonical_sha256(
                data["immigrantConstructionPolicy"]
            ),
            "entryRouteDecisionIndicatorPolicy": _clone(
                data["grammarRegistry"]["entryRouteDecisionIndicatorPolicy"],
                name="frozen entry route decision-indicator policy",
            ),
            "nativeAuthoritySha256": IdentitySnapshot.from_payload(data["nativeAuthority"], expected_kind="nativeAuthority").sha256,
            "pairCompilerAuthoritySha256": IdentitySnapshot.from_payload(data["pairCompilerAuthority"], expected_kind="pairCompiler").sha256,
        }
        if data.get("operatorImplementation") != expected_operator:
            raise TemporalDiscoveryContractError("frozen pair operator implementation identity drifted")
        data["pairRunConfigSha256"] = supplied
        self.config = data
        t = data["nativeJsonlAuthority"]
        self.client = SubprocessCandidateValidator(t["command"], timeout_seconds=float(t["timeoutSeconds"]), persistent_jsonl=True, persistent_max_line_bytes=int(t["maxLineBytes"]), persistent_stderr_limit_bytes=int(t["stderrLimitBytes"]), persistent_environment={"PYTHONPATH": os.pathsep.join(t["authorityContent"]["environment"]["PYTHONPATH"])})
        self.validator = DashboardV2ModuleValidator(self.client)
        self.compiler = DashboardBidirectionalPairCompiler(self.client)
        self.native_identity = IdentitySnapshot.from_payload(data["nativeAuthority"], expected_kind="nativeAuthority")
        self.compiler_identity = IdentitySnapshot.from_payload(data["pairCompilerAuthority"], expected_kind="pairCompiler")
        self._immigrant_runtime_cache: dict[str, _ImmigrantSideRuntime] = {}
        self.factory = _Factory(self)
        self.operator = TypedGrammarPairOperator(
            grammar_factory=self.grammar_for,
            native_validator=self.validator,
            indicator_registry=self.indicator_for,
            hold_operator_policy=data["holdOperatorPolicy"],
            initial_protection_policy=data["initialProtectionOperatorPolicy"],
        )

    def close(self) -> None: self.client.close()
    def __enter__(self) -> "PairAuthorityBundle": return self
    def __exit__(self, *_: object) -> None: self.close()

    def _side(self, direction: str) -> Mapping[str, Any]: return self.config[f"{direction}Module"]

    def immigrant_runtime(self, direction: str) -> _ImmigrantSideRuntime:
        """Return cached immutable construction authority for one frozen side.

        The pair-run config SHA closes every context/catalog/policy/native
        input.  Direction is included to keep long and short authorities
        distinct even when their catalog bytes happen to match.
        """

        side_name = str(direction)
        if side_name not in {"long", "short"}:
            raise TemporalDiscoveryContractError("pair immigrant side is unknown")
        authority_key = canonical_sha256(
            {
                "schemaVersion": "temporal_qd_immigrant_runtime_authority_v1",
                "pairRunConfigSha256": self.config["pairRunConfigSha256"],
                "direction": side_name,
            }
        )
        cached = self._immigrant_runtime_cache.get(authority_key)
        if cached is not None:
            return cached
        side = self._side(side_name)
        context = side["context"]
        context_id = IdentitySnapshot.create(
            kind="grammarContext",
            schema_version="temporal_typed_grammar_context_v1",
            payload=context,
        )
        catalog_id = IdentitySnapshot.create(
            kind="catalog",
            schema_version="temporal_indicator_learning_catalog_v1",
            payload={
                "catalog": side["catalog"],
                "catalogSha256": side["catalogSha256"],
            },
        )
        policy_id = IdentitySnapshot.create(
            kind="policy",
            schema_version="temporal_qd_pair_module_policy_v2",
            payload={
                "modulePolicy": side["policy"],
                "indicatorPolicy": side["indicatorPolicy"],
                "holdOperatorPolicy": self.config["holdOperatorPolicy"],
                "initialProtectionOperatorPolicy": self.config[
                    "initialProtectionOperatorPolicy"
                ],
                "immigrantConstructionPolicy": self.config[
                    "immigrantConstructionPolicy"
                ],
            },
        )
        grammar = TypedFragmentGrammar(
            GrammarContext(
                instrument=context["instrument"],
                indicators=tuple(context["indicators"]),
                evidence_groups=tuple(context["groups"]),
                event_bindings=tuple(context["events"]),
                execution_config=context["executionConfig"],
                budgets=context["budgets"],
            ),
            native_authority=self.validator,
        )
        runtime = _ImmigrantSideRuntime(
            authority_key=authority_key,
            side=side,
            grammar_context=context_id,
            catalog=catalog_id,
            policy=policy_id,
            grammar=grammar,
            registry=IndicatorLearningRegistry(side["catalog"]),
        )
        self._immigrant_runtime_cache[authority_key] = runtime
        return runtime

    def grammar_for(self, module: FrozenModule) -> TypedFragmentGrammar:
        side = self._side(module.direction)
        expected_context = IdentitySnapshot.create(kind="grammarContext", schema_version="temporal_typed_grammar_context_v1", payload=side["context"])
        if module.grammar_context.sha256 != expected_context.sha256 or module.native_authority.sha256 != self.native_identity.sha256:
            raise TemporalDiscoveryContractError("frozen pair module grammar/native authority drifted")
        context = GrammarContext(instrument=side["context"]["instrument"], indicators=tuple(side["context"]["indicators"]), evidence_groups=tuple(side["context"]["groups"]), event_bindings=tuple(side["context"]["events"]), execution_config=side["context"]["executionConfig"], budgets=side["context"]["budgets"])
        return TypedFragmentGrammar(context, native_authority=self.validator)
    def indicator_for(self, module: FrozenModule) -> IndicatorLearningRegistry:
        side = self._side(module.direction)
        expected_catalog = IdentitySnapshot.create(kind="catalog", schema_version="temporal_indicator_learning_catalog_v1", payload={"catalog": side["catalog"], "catalogSha256": side["catalogSha256"]})
        if module.catalog.sha256 != expected_catalog.sha256:
            raise TemporalDiscoveryContractError("frozen pair module indicator catalog drifted")
        return IndicatorLearningRegistry(side["catalog"])
    def seed_module(self, direction: str, proposal_seed: str) -> FrozenModule:
        side = self._side(direction)
        context_id = IdentitySnapshot.create(kind="grammarContext", schema_version="temporal_typed_grammar_context_v1", payload=side["context"])
        catalog_id = IdentitySnapshot.create(kind="catalog", schema_version="temporal_indicator_learning_catalog_v1", payload={"catalog": side["catalog"], "catalogSha256": side["catalogSha256"]})
        policy_id = IdentitySnapshot.create(kind="policy", schema_version="temporal_qd_pair_module_policy_v2", payload={"modulePolicy": side["policy"], "indicatorPolicy": side["indicatorPolicy"], "holdOperatorPolicy": self.config["holdOperatorPolicy"], "initialProtectionOperatorPolicy": self.config["initialProtectionOperatorPolicy"]})
        # The snapshot is checked against the module by every typed operation.
        template = FrozenModule.freeze  # keeps the actual construction below visually local
        grammar = TypedFragmentGrammar(GrammarContext(instrument=side["context"]["instrument"], indicators=tuple(side["context"]["indicators"]), evidence_groups=tuple(side["context"]["groups"]), event_bindings=tuple(side["context"]["events"]), execution_config=side["context"]["executionConfig"], budgets=side["context"]["budgets"]), native_authority=self.validator)
        names = side["seedNames"]
        selector = canonical_sha256({"seed": str(proposal_seed), "side": direction})[7:]
        name = names[int(selector[0:8], 16) % len(names)]
        group_id = side["context"]["groups"][int(selector[8:16], 16) % len(side["context"]["groups"])]["id"]
        event_id = side["context"]["events"][int(selector[16:24], 16) % len(side["context"]["events"])]["id"]
        plan_id = side["context"]["plans"][int(selector[24:32], 16) % len(side["context"]["plans"])]
        compiled = grammar.compile_module(
            grammar.seed(
                direction=direction,
                name=name,
                group_id=group_id,
                event_id=event_id,
                plan_id=plan_id,
            ),
            candidate_id="qd_pair_module_" + selector[:28],
        )
        del template
        return FrozenModule.freeze(program=compiled.program, profile=compiled.profile, grammar_context=context_id, catalog=catalog_id, policy=policy_id, native_authority=self.native_identity, native_report=compiled.native_report, lineage=[{"operation": "typed_seed", "side": direction, "seedName": name, "groupId": group_id, "eventId": event_id, "planId": plan_id, "proposalSeed": str(proposal_seed)}])


def pair_policy_from_config(frozen: Mapping[str, Any]) -> dict[str, Any]:
    data = _mapping(frozen, name="frozen pair run config")
    supplied = data.pop("pairRunConfigSha256", None)
    if supplied != canonical_sha256(data):
        raise TemporalDiscoveryContractError("pair run config identity mismatch")
    return {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": data["pairCompilerAuthority"]}


def refresh_pair_run_config(template: Mapping[str, Any]) -> dict[str, Any]:
    """Re-freeze a self-valid template against the current local authority.

    Only authored inputs are carried forward.  Every derived registry, catalog,
    operator, interpreter, script, environment, and Dashboard source identity is
    recomputed by :func:`freeze_pair_run_config`.
    """

    frozen = _mapping(template, name="pair run config template")
    authored_fields = {"schemaVersion", "longModule", "shortModule", "nativeJsonlAuthority", "holdOperatorPolicy"}
    if set(frozen) == authored_fields:
        return freeze_pair_run_config(frozen)
    supplied = frozen.pop("pairRunConfigSha256", None)
    if supplied != canonical_sha256(frozen) or frozen.get("schemaVersion") not in {
        PAIR_RUN_CONFIG_SCHEMA_LEGACY,
        PAIR_RUN_CONFIG_SCHEMA,
    }:
        raise TemporalDiscoveryContractError("pair run config template identity/schema mismatch")

    def authored_side(direction: str) -> dict[str, Any]:
        side = _mapping(frozen.get(f"{direction}Module"), name=f"frozen {direction} module")
        context = _mapping(side.get("context"), name=f"frozen {direction} grammar context")
        required_context = {"instrument", "indicators", "groups", "events", "executionConfig", "plans", "budgets"}
        if set(context) != required_context:
            raise TemporalDiscoveryContractError("frozen pair grammar context is not the normalized closed schema")
        return {
            "seedNames": side.get("seedNames"),
            "context": {
                "instrument": context["instrument"],
                "indicators": context["indicators"],
                "evidenceGroups": context["groups"],
                "eventBindings": context["events"],
                "executionConfig": context["executionConfig"],
                "budgets": context["budgets"],
            },
            "catalog": side.get("catalog"),
            "policy": side.get("policy"),
        }

    transport = _mapping(frozen.get("nativeJsonlAuthority"), name="frozen pair native authority")
    transport.pop("authorityContent", None)
    raw = {
        "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
        "longModule": authored_side("long"),
        "shortModule": authored_side("short"),
        "nativeJsonlAuthority": transport,
        "holdOperatorPolicy": frozen.get("holdOperatorPolicy"),
    }
    return freeze_pair_run_config(raw)


def load_pair_run_config(value: Mapping[str, Any]) -> dict[str, Any]:
    """Load authored input or verify an already-frozen runtime authority."""

    data = _mapping(value, name="pair run config")
    authored_fields = {"schemaVersion", "longModule", "shortModule", "nativeJsonlAuthority", "holdOperatorPolicy"}
    if set(data) == authored_fields:
        return freeze_pair_run_config(data)
    with PairAuthorityBundle(data):
        pass
    return data


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read pair run config: {path}") from exc
    return _mapping(value, name="pair run config JSON")


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing to overwrite divergent pair run config: {path}")
    path.write_text(encoded, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh or audit the frozen bidirectional QD pair authority.")
    commands = parser.add_subparsers(dest="command", required=True)
    refresh = commands.add_parser("refresh")
    refresh.add_argument("--template", type=Path, required=True)
    refresh.add_argument("--output", type=Path, required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "refresh":
        value = refresh_pair_run_config(_read_json(args.template))
        _write_immutable(args.output, value)
    else:
        value = _read_json(args.config)
        with PairAuthorityBundle(value):
            pass
    print(json.dumps({"ok": True, "pairRunConfigSha256": value["pairRunConfigSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["PAIR_HOLD_POLICY_SCHEMA", "PAIR_IMMIGRANT_POLICY_SCHEMA", "PAIR_RUN_CONFIG_SCHEMA", "PairAuthorityBundle", "default_hold_operator_policy", "default_immigrant_construction_policy", "freeze_pair_run_config", "immigrant_capacity_audit", "load_pair_run_config", "pair_policy_from_config", "refresh_pair_run_config"]
