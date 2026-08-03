"""Closed production authority for the opt-in bidirectional QD generator.

The supervisor persists the value returned by :func:`freeze_pair_run_config`.
It contains data only; runtime clients are rebuilt from it for every process
start, so a resumed run cannot inherit a mutable catalog or a Python object.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import os
from pathlib import Path
import subprocess
from typing import Any

from .temporal_bidirectional_genome import FrozenModule, FrozenPair, IdentitySnapshot, canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_discovery_validation import DashboardBidirectionalPairCompiler, DashboardV2ModuleValidator, SubprocessCandidateValidator
from .temporal_indicator_learning_v1 import IndicatorLearningRegistry
from .temporal_qd_pair_generation import TypedGrammarPairOperator
from .temporal_typed_motif_grammar import GRAMMAR_SCHEMA, GRAMMAR_VERSION, REGISTRY, GrammarContext, TypedFragmentGrammar

PAIR_RUN_CONFIG_SCHEMA = "temporal_qd_bidirectional_pair_run_config_v1"
PAIR_HOLD_POLICY_SCHEMA = "temporal_qd_pair_hold_operator_policy_v1"


def _clone(value: Any, *, name: str) -> Any:
    try:
        import json
        return json.loads(canonical_json(value))
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
    result = {"schemaVersion": "temporal_typed_fragment_registry_identity_v1", "grammarSchema": GRAMMAR_SCHEMA, "grammarVersion": GRAMMAR_VERSION, "productions": rows}
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


def freeze_pair_run_config(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate external JSON and return the sole persisted pair authority."""
    value = _mapping(raw, name="bidirectional pair run config")
    required = {"schemaVersion", "longModule", "shortModule", "nativeJsonlAuthority", "holdOperatorPolicy"}
    if set(value) != required or value.get("schemaVersion") != PAIR_RUN_CONFIG_SCHEMA:
        raise TemporalDiscoveryContractError("bidirectional pair run config fields/schema are not exact")
    hold = _mapping(value["holdOperatorPolicy"], name="pair hold operator policy")
    if hold.get("schemaVersion") != PAIR_HOLD_POLICY_SCHEMA or hold.get("enabled") is not True or hold.get("allowedKinds") != ["none", "market_bars", "elapsed_calendar"]:
        raise TemporalDiscoveryContractError("pair hold operator policy is not the closed admitted policy")
    transport = _bound_transport(_mapping(value["nativeJsonlAuthority"], name="pair native JSONL authority"))
    native_snapshot = IdentitySnapshot.create(kind="nativeAuthority", schema_version="temporal_dashboard_jsonl_native_authority_v1", payload=transport)
    compiler_snapshot = IdentitySnapshot.create(kind="pairCompiler", schema_version="temporal_dashboard_jsonl_pair_compiler_v1", payload=transport)
    result = {
        "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
        "longModule": _side(_mapping(value["longModule"], name="long pair module"), "long"),
        "shortModule": _side(_mapping(value["shortModule"], name="short pair module"), "short"),
        "grammarRegistry": _registry_identity(),
        "holdOperatorPolicy": hold,
        "nativeJsonlAuthority": transport,
        "nativeAuthority": native_snapshot.canonical_payload(),
        "pairCompilerAuthority": compiler_snapshot.canonical_payload(),
    }
    result["operatorImplementation"] = {
        "schemaVersion": "temporal_qd_pair_operator_implementation_v1",
        "typedGrammarRegistrySha256": result["grammarRegistry"]["registrySha256"],
        "longIndicatorPolicySha256": result["longModule"]["indicatorPolicy"]["policySha256"],
        "shortIndicatorPolicySha256": result["shortModule"]["indicatorPolicy"]["policySha256"],
        "holdOperatorPolicySha256": canonical_sha256(hold),
        "nativeAuthoritySha256": native_snapshot.sha256,
        "pairCompilerAuthoritySha256": compiler_snapshot.sha256,
    }
    result["pairRunConfigSha256"] = canonical_sha256(result)
    return result


class _Factory:
    def __init__(self, bundle: "PairAuthorityBundle") -> None: self.bundle = bundle
    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        long = self.bundle.seed_module("long", proposal_seed)
        short = self.bundle.seed_module("short", proposal_seed)
        return FrozenPair.compile(long=long, short=short, pair_compiler_identity=self.bundle.compiler_identity, pair_compiler=self.bundle.compiler, candidate_id="qd_pair_seed_" + canonical_sha256({"seed": proposal_seed})[7:35], side_targeted_lineage=[{"operation": "seed", "side": "long", "proposalSeed": str(proposal_seed)}, {"operation": "seed", "side": "short", "proposalSeed": str(proposal_seed)}])


class PairAuthorityBundle:
    def __init__(self, frozen: Mapping[str, Any]) -> None:
        data = _mapping(frozen, name="frozen pair run config")
        supplied = data.pop("pairRunConfigSha256", None)
        if supplied != canonical_sha256(data) or data.get("schemaVersion") != PAIR_RUN_CONFIG_SCHEMA:
            raise TemporalDiscoveryContractError("pair run config identity/schema mismatch")
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
            "schemaVersion": "temporal_qd_pair_operator_implementation_v1",
            "typedGrammarRegistrySha256": data["grammarRegistry"]["registrySha256"],
            "longIndicatorPolicySha256": data["longModule"]["indicatorPolicy"]["policySha256"],
            "shortIndicatorPolicySha256": data["shortModule"]["indicatorPolicy"]["policySha256"],
            "holdOperatorPolicySha256": canonical_sha256(data["holdOperatorPolicy"]),
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
        self.factory = _Factory(self)
        self.operator = TypedGrammarPairOperator(grammar_factory=self.grammar_for, native_validator=self.validator, indicator_registry=self.indicator_for)

    def close(self) -> None: self.client.close()
    def __enter__(self) -> "PairAuthorityBundle": return self
    def __exit__(self, *_: object) -> None: self.close()

    def _side(self, direction: str) -> Mapping[str, Any]: return self.config[f"{direction}Module"]
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
        policy_id = IdentitySnapshot.create(kind="policy", schema_version="temporal_qd_pair_module_policy_v1", payload={"modulePolicy": side["policy"], "indicatorPolicy": side["indicatorPolicy"], "holdOperatorPolicy": self.config["holdOperatorPolicy"]})
        # The snapshot is checked against the module by every typed operation.
        template = FrozenModule.freeze  # keeps the actual construction below visually local
        grammar = TypedFragmentGrammar(GrammarContext(instrument=side["context"]["instrument"], indicators=tuple(side["context"]["indicators"]), evidence_groups=tuple(side["context"]["groups"]), event_bindings=tuple(side["context"]["events"]), execution_config=side["context"]["executionConfig"], budgets=side["context"]["budgets"]), native_authority=self.validator)
        names = side["seedNames"]
        name = names[int(canonical_sha256({"seed": str(proposal_seed), "side": direction})[-2:], 16) % len(names)]
        compiled = grammar.compile_module(grammar.seed(direction=direction, name=name), candidate_id="qd_pair_module_" + canonical_sha256({"seed": str(proposal_seed), "side": direction})[7:35])
        del template
        return FrozenModule.freeze(program=compiled.program, profile=compiled.profile, grammar_context=context_id, catalog=catalog_id, policy=policy_id, native_authority=self.native_identity, native_report=compiled.native_report, lineage=[{"operation": "typed_seed", "side": direction, "seedName": name, "proposalSeed": str(proposal_seed)}])


def pair_policy_from_config(frozen: Mapping[str, Any]) -> dict[str, Any]:
    data = _mapping(frozen, name="frozen pair run config")
    supplied = data.pop("pairRunConfigSha256", None)
    if supplied != canonical_sha256(data):
        raise TemporalDiscoveryContractError("pair run config identity mismatch")
    return {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": data["pairCompilerAuthority"]}


__all__ = ["PAIR_RUN_CONFIG_SCHEMA", "PairAuthorityBundle", "freeze_pair_run_config", "pair_policy_from_config"]
