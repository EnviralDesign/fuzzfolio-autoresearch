#!/usr/bin/env python3
"""Generate the bounded, source-pinned Evolutionary Substrate Atlas.

This is a static source auditor.  It only reads committed source/configuration
files and Git objects; it never opens a run corpus, market-data file, worker,
gateway, or external service.  The generated ledger intentionally separates
implemented language/runtime capability from historical activation or credit,
which stay ``unavailable`` without an authorized evidence study.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA = "evolutionary_substrate_atlas_v1"
V37_COMMIT = "5fa623b88c641d4d886411bf195ee3ef386d6446"
V38_COMMIT = "51c2f9175f441166e7fc997109e939a9f9103b5d"

# These are deliberate source-count tripwires, not feature limits.  A source
# registry/model change must make this tool and its semantic mapping reviewable.
EXPECTED_SOURCE_COUNTS = {
    "grammarFragments": 23,
    "runtimeGuards": 22,
    "runtimeActions": 8,
    "topologyOperations": 14,
    "operatorFamilies": 6,
    "catalogIndicators": 88,
    "catalogTimeframes": 7,
    "grammarPorts": 7,
}

AR_FILES = {
    "grammar": "rust/temporal-qd/crates/qd-kernel/src/grammar.rs",
    "operators": "rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs",
    "topology": "rust/temporal-qd/crates/qd-kernel/src/v5_topology_operators.rs",
    "nativeBridge": "autoresearch/temporal_qd_v5_native.py",
}
FF_FILES = {
    "guards": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/guards.py",
    "actions": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/action_models.py",
    "kernel": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/kernel.py",
    "compiler": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/bidirectional_compiler.py",
    "catalog": "shared/constants/indicators.json",
}


class AtlasError(RuntimeError):
    """The requested source surface no longer matches this bounded atlas."""


@dataclass(frozen=True)
class SourceFile:
    repo: str
    root: Path
    commit: str
    relative_path: str

    @property
    def path(self) -> Path:
        return self.root / self.relative_path

    @property
    def text(self) -> str:
        return self.path.read_text(encoding="utf-8")

    def binding(self, symbol: str) -> dict[str, str]:
        return {
            "repository": self.repo,
            "commit": self.commit,
            "path": self.relative_path,
            "sourceSha256": file_sha256(self.path),
            "symbol": symbol,
        }


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def git(root: Path, *args: str, text: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=text,
        encoding="utf-8" if text else None,
    )
    return result.stdout.strip() if text else result.stdout


def commit_of(root: Path) -> str:
    return git(root, "rev-parse", "HEAD")


def historical_text(root: Path, commit: str, relative_path: str) -> str:
    return git(root, "show", f"{commit}:{relative_path}")


def snake_case(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def require_count(name: str, values: list[Any]) -> None:
    expected = EXPECTED_SOURCE_COUNTS[name]
    if len(values) != expected:
        raise AtlasError(
            f"{name} changed from {expected} to {len(values)}; update the semantic mapping and tripwire"
        )


def grammar_fragments(source: str) -> list[dict[str, str]]:
    pattern = re.compile(
        r'put\(spec\(\s*"(?P<id>[^"]+)"\s*,\s*"(?P<family>[^"]+)"\s*,'
        r"\s*Port::(?P<consumes>[A-Za-z]+)\s*,\s*Port::(?P<produces>[A-Za-z]+)",
        re.MULTILINE,
    )
    result = [match.groupdict() for match in pattern.finditer(source)]
    require_count("grammarFragments", result)
    return result


def class_models(source: str, suffix: str) -> list[str]:
    result = re.findall(rf"^class ([A-Za-z0-9_]+{suffix})\b", source, flags=re.MULTILINE)
    if len(result) != len(set(result)):
        raise AtlasError(f"duplicate {suffix} models in source")
    return result


def topology_operations(source: str) -> list[str]:
    match = re.search(r"const OPERATIONS:\s*&\[&str\]\s*=\s*&\[(?P<body>.*?)\];", source, re.DOTALL)
    if not match:
        raise AtlasError("could not locate sealed topology operation registry")
    result = re.findall(r'"([a-z_]+)"', match.group("body"))
    require_count("topologyOperations", result)
    return result


def operator_families(source: str) -> list[dict[str, str]]:
    result = [
        {"constant": constant, "operatorId": operator_id}
        for constant, operator_id in re.findall(
            r'pub const (V5_[A-Z_]+_OPERATOR_ID):\s*&str\s*=\s*"([^"]+)";', source
        )
    ]
    require_count("operatorFamilies", result)
    return result


def grammar_ports(source: str) -> list[str]:
    match = re.search(r"pub enum Port\s*\{(?P<body>.*?)\n\}", source, re.DOTALL)
    if not match:
        raise AtlasError("could not locate grammar Port enum")
    result = re.findall(r"^\s*([A-Za-z]+),", match.group("body"), re.MULTILINE)
    require_count("grammarPorts", result)
    return result


def catalog_items(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    indicators = value.get("indicators")
    timeframes = value.get("timeframes")
    if not isinstance(indicators, list) or not isinstance(timeframes, dict):
        raise AtlasError("indicator catalog shape changed")
    normalized: list[dict[str, Any]] = []
    for item in indicators:
        if not isinstance(item, dict) or not isinstance(item.get("meta"), dict):
            raise AtlasError("indicator catalog item lacks metadata")
        meta = item["meta"]
        item_id = meta.get("id")
        if not isinstance(item_id, str) or not item_id:
            raise AtlasError("indicator catalog item lacks id")
        normalized.append(
            {
                "id": item_id,
                "namespace": meta.get("namespace") if isinstance(meta.get("namespace"), str) else None,
                "signalRole": meta.get("signalRole") if isinstance(meta.get("signalRole"), str) else None,
                "preferredTimeframeRole": (
                    meta.get("preferredTimeframeRole")
                    if isinstance(meta.get("preferredTimeframeRole"), str)
                    else None
                ),
            }
        )
    normalized.sort(key=lambda item: item["id"])
    if len({item["id"] for item in normalized}) != len(normalized):
        raise AtlasError("indicator catalog IDs are not unique")
    frame_ids = sorted(timeframes)
    require_count("catalogIndicators", normalized)
    require_count("catalogTimeframes", frame_ids)
    return normalized, frame_ids


def base_reachability(**overrides: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "kernel": "not_applicable",
        "rust": "not_applicable",
        "grammar": "not_applicable",
        "seed": "not_established",
        "mutation": {"add": "not_applicable", "modify": "not_applicable", "remove": "not_applicable"},
        "crossover": "not_applicable",
        "validator": "not_applicable",
        "runtime": "not_applicable",
    }
    for key, value in overrides.items():
        if key.startswith("mutation_"):
            result["mutation"][key.removeprefix("mutation_")] = value
        else:
            result[key] = value
    return result


def historical_status() -> dict[str, str]:
    return {
        "v37": "unavailable_no_run_corpus_read",
        "v38": "unavailable_no_run_corpus_read",
        "reason": "Stage 4.5A permits source inspection and compile-only evidence, not historical market/run data reads.",
    }


def capability(
    capability_id: str,
    kind: str,
    label: str,
    reachability: dict[str, Any],
    binding: dict[str, str],
    *,
    observability: str,
    credit: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "capabilityId": capability_id,
        "kind": kind,
        "label": label,
        "reachability": reachability,
        "observability": observability,
        "credit": credit,
        "historicalEvidence": historical_status(),
        "sourceBindings": [binding],
        "notes": notes,
    }


def grammar_direct_guard_ids(grammar_source: str) -> set[str]:
    # The grammar's only generated runtime guard vocabulary is represented by
    # literal ``kind`` strings.  We do not infer reachability from comments.
    return set(re.findall(r'\("kind",\s*"([a-z_]+)"\.into\(\)\)', grammar_source))


def guard_kind(class_name: str) -> str:
    raw = class_name.removesuffix("Guard")
    aliases = {
        "EvidenceAtLeast": "evidence_at_least",
        "EvidenceBelow": "evidence_below",
        "FreshEvent": "fresh_event",
        "EventAgeAtMost": "event_age_at_most",
        "ConditionStreakAtLeast": "condition_streak_at_least",
        "EventAgeWindow": "event_age_window",
        "ActionCooldownElapsed": "action_cooldown_elapsed",
        "UtcTimeWindow": "utc_time_window",
        "StateAgeAtLeast": "state_age_at_least",
        "StateAgeAtMost": "state_age_at_most",
        "PositionExists": "position_exists",
        "PositionAgeAtLeast": "position_age_at_least",
        "UnrealizedRAtLeast": "unrealized_r_at_least",
        "UnrealizedRAtMost": "unrealized_r_at_most",
        "PredicateEdge": "predicate_edge",
        "ConsecutiveTrue": "consecutive_true",
        "ExecutionStatusIs": "execution_status_is",
        "ExecutionReasonIs": "execution_reason_is",
        "Always": "always",
        "All": "all",
        "Any": "any",
        "Not": "not",
    }
    return aliases.get(raw, snake_case(raw))


def action_kind(class_name: str) -> str:
    return snake_case(class_name.removesuffix("Action")).removesuffix("_next_open") + "_next_open"


def source_authority(
    ar_root: Path, ff_root: Path, ar_commit: str, ff_commit: str, source_files: dict[str, SourceFile]
) -> dict[str, Any]:
    v37_grammar = historical_text(ar_root, V37_COMMIT, AR_FILES["grammar"])
    v38_grammar = historical_text(ar_root, V38_COMMIT, AR_FILES["grammar"])
    return {
        "schemaVersion": SCHEMA,
        "sources": {
            "autoresearch": {"repository": "EnviralDesign/fuzzfolio-autoresearch", "commit": ar_commit},
            "fuzzfolio": {"repository": "EnviralDesign/FuzzFolio", "commit": ff_commit},
        },
        "historicalAutoresearch": {
            "v37": {
                "commit": V37_COMMIT,
                "grammarSourceSha256": sha256_bytes(v37_grammar.encode("utf-8")),
                "grammarFragmentCount": len(grammar_fragments(v37_grammar)),
            },
            "v38": {
                "commit": V38_COMMIT,
                "grammarSourceSha256": sha256_bytes(v38_grammar.encode("utf-8")),
                "grammarFragmentCount": len(grammar_fragments(v38_grammar)),
            },
        },
        "currentSourceFiles": {
            key: {
                "repository": source.repo,
                "commit": source.commit,
                "path": source.relative_path,
                "sourceSha256": file_sha256(source.path),
            }
            for key, source in sorted(source_files.items())
        },
        "ancestry": {
            "branchBase": V38_COMMIT,
            "rule": "The Stage 4.5A branch descends from the verified default V38 commit; no divergent research branch was merged.",
        },
    }


def build_atlas(ar_root: Path, ff_root: Path) -> dict[str, Any]:
    ar_commit = commit_of(ar_root)
    ff_commit = commit_of(ff_root)
    sources = {
        **{
            key: SourceFile("EnviralDesign/fuzzfolio-autoresearch", ar_root, ar_commit, path)
            for key, path in AR_FILES.items()
        },
        **{
            key: SourceFile("EnviralDesign/FuzzFolio", ff_root, ff_commit, path)
            for key, path in FF_FILES.items()
        },
    }
    fragments = grammar_fragments(sources["grammar"].text)
    guards = class_models(sources["guards"].text, "Guard")
    actions = class_models(sources["actions"].text, "Action")
    operations = topology_operations(sources["topology"].text)
    operators = operator_families(sources["operators"].text)
    ports = grammar_ports(sources["grammar"].text)
    indicators, timeframes = catalog_items(sources["catalog"].path)
    require_count("runtimeGuards", guards)
    require_count("runtimeActions", actions)
    direct_guards = grammar_direct_guard_ids(sources["grammar"].text)

    capabilities: list[dict[str, Any]] = []
    for port in ports:
        capabilities.append(
            capability(
                f"grammar.port.{snake_case(port)}",
                "graph_port",
                port,
                base_reachability(
                    kernel="compiled_graph_state",
                    rust="implemented",
                    grammar="direct",
                    seed="direct_v2_grammar_seed",
                    validator="bounded_typed_fragment_validation",
                    runtime="compiled_to_temporal_graph",
                ),
                sources["grammar"].binding("Port"),
                observability="compiled module state/transition topology",
                credit="no component-local credit path is established by source alone",
                notes="A typed grammar port, not an independent market feature.",
            )
        )
    for fragment in fragments:
        capabilities.append(
            capability(
                f"grammar.fragment.{fragment['id']}",
                "grammar_fragment",
                fragment["id"],
                base_reachability(
                    kernel="compiled_graph_state",
                    rust="implemented",
                    grammar="direct",
                    seed="direct_v2_grammar_seed",
                    validator="native_v2_then_bidirectional_compile",
                    runtime="compiled_to_temporal_graph",
                ),
                sources["grammar"].binding(f"registry::{fragment['id']}"),
                observability="activation recipe and compiled transition receipt",
                credit="candidate-level only; no fragment-local selection/credit field established",
                notes=(
                    f"{fragment['family']} fragment: {fragment['consumes']} -> {fragment['produces']}. "
                    "V5 topology mutation is a separate genome surface, so a direct V5 fragment mutation route is not claimed."
                ),
            )
        )
    for class_name in guards:
        kind = guard_kind(class_name)
        grammar_route = "direct" if kind in direct_guards else "kernel_only_no_direct_grammar_production"
        capabilities.append(
            capability(
                f"runtime.guard.{kind}",
                "runtime_guard",
                kind,
                base_reachability(
                    kernel="implemented",
                    rust="compiled_guard_emission" if kind in direct_guards else "not_established",
                    grammar=grammar_route,
                    seed="direct_only_when_grammar_route_exists" if kind in direct_guards else "no_direct_seed_route",
                    mutation_add="not_established" if kind not in direct_guards else "topology_dependent",
                    mutation_modify="not_established" if kind not in direct_guards else "parameter_or_topology_dependent",
                    mutation_remove="not_established" if kind not in direct_guards else "topology_dependent",
                    validator="temporal_graph_validation",
                    runtime="implemented",
                ),
                sources["guards"].binding(class_name),
                observability="runtime guard evaluation and fact-history requirements",
                credit="no guard-local lineage or selection credit field established",
                notes="Runtime language support is not evidence of seed or mutation reachability.",
            )
        )
    for class_name in actions:
        kind = action_kind(class_name)
        capabilities.append(
            capability(
                f"runtime.action.{kind}",
                "runtime_action",
                kind,
                base_reachability(
                    kernel="intent_scheduling",
                    rust="compiled_action_emission",
                    grammar="direct_for_matching_fragment_or_topology_region",
                    seed="grammar_or_compiler_dependent",
                    mutation_add="topology_dependent",
                    mutation_modify="topology_or_scalar_dependent",
                    mutation_remove="topology_dependent",
                    validator="action_contract_and_temporal_validation",
                    runtime="implemented",
                ),
                sources["actions"].binding(class_name),
                observability="execution intent, status/reason transitions, and execution trace",
                credit="candidate-level outcome only; action-local credit is not established",
                notes="Action execution is observable separately from a requested action.",
            )
        )
    for operation in operations:
        verb = operation.split("_", 1)[0]
        capabilities.append(
            capability(
                f"mutation.topology.{operation}",
                "topology_operation",
                operation,
                base_reachability(
                    kernel="sealed_program_transform",
                    rust="implemented",
                    grammar="v5_topology_genome",
                    seed="not_a_seed_constructor",
                    mutation_add="direct" if verb == "insert" else "not_applicable",
                    mutation_modify="direct" if verb == "rewire" else "not_applicable",
                    mutation_remove="direct" if verb == "remove" else "not_applicable",
                    crossover="separate_same_side_operator",
                    validator="post_transform_re_admission",
                    runtime="requires_recompile_before_runtime",
                ),
                sources["topology"].binding(f"OPERATIONS::{operation}"),
                observability="content-bound plan, semantic trace, accepted/no-op/rejected result",
                credit="operator/candidate lineage, not component-local causal credit",
                notes="Only enumerated, sealed one-step transformations are represented.",
            )
        )
    for operator in operators:
        operator_id = operator["operatorId"]
        capabilities.append(
            capability(
                f"mutation.operator.{operator_id}",
                "operator_family",
                operator_id,
                base_reachability(
                    kernel="authority_bound_selection_and_apply",
                    rust="implemented",
                    grammar="family_specific",
                    seed="not_a_seed_constructor",
                    mutation_add="family_specific",
                    mutation_modify="family_specific",
                    mutation_remove="family_specific",
                    crossover="direct" if operator_id.endswith("crossover_v1") else "separate_operator",
                    validator="compiled_child_admission",
                    runtime="requires_fresh_pair_recompile",
                ),
                sources["operators"].binding(operator["constant"]),
                observability="authority-bound plan/receipt, disposition, reason code, semantic trace",
                credit="operator identity is recorded; learned operator-weight update is not established",
                notes="Family existence does not establish sampling frequency or historical activation.",
            )
        )
    for timeframe in timeframes:
        capabilities.append(
            capability(
                f"catalog.timeframe.{timeframe.lower()}",
                "catalog_timeframe",
                timeframe,
                base_reachability(
                    kernel="catalog_configuration",
                    rust="authority_policy_dependent",
                    grammar="not_directly_enumerated",
                    seed="policy_dependent",
                    mutation_add="resource_operator_policy_dependent",
                    mutation_modify="resource_operator_policy_dependent",
                    mutation_remove="resource_operator_policy_dependent",
                    validator="catalog_and_compiler_policy",
                    runtime="indicator_binding_dependent",
                ),
                sources["catalog"].binding(f"timeframes.{timeframe}"),
                observability="catalog selection; active frozen-authority inclusion requires a bound authority fixture",
                credit="no timeframe-local credit is established",
                notes="Catalog availability is distinct from membership in a frozen run authority.",
            )
        )
    for indicator in indicators:
        indicator_id = indicator["id"]
        capabilities.append(
            capability(
                f"catalog.indicator.{indicator_id.lower()}",
                "catalog_indicator",
                indicator_id,
                base_reachability(
                    kernel="resource_binding",
                    rust="resource_operator_policy_dependent",
                    grammar="indirect_via_resource_binding",
                    seed="catalog_and_policy_dependent",
                    mutation_add="resource_operator_policy_dependent",
                    mutation_modify="resource_operator_policy_dependent",
                    mutation_remove="resource_operator_policy_dependent",
                    validator="catalog_then_compiler_admission",
                    runtime="bound_indicator_runtime",
                ),
                sources["catalog"].binding(f"indicators.{indicator_id}"),
                observability="catalog metadata; activation requires compiled candidate/runtime evidence",
                credit="no indicator-local selection credit is established",
                notes=(
                    "Catalog member only. Frozen authority eligibility, actual compilation, activation, "
                    "retention, and selection remain separate states."
                ),
            )
        )
    capabilities.sort(key=lambda item: item["capabilityId"])
    counts = {
        "grammarFragments": len(fragments),
        "runtimeGuards": len(guards),
        "runtimeActions": len(actions),
        "topologyOperations": len(operations),
        "operatorFamilies": len(operators),
        "catalogIndicators": len(indicators),
        "catalogTimeframes": len(timeframes),
        "grammarPorts": len(ports),
        "ledgerCapabilities": len(capabilities),
    }
    return {
        "schemaVersion": SCHEMA,
        "sourceAuthority": source_authority(ar_root, ff_root, ar_commit, ff_commit, sources),
        "sourceCounts": counts,
        "capabilities": capabilities,
    }


def build_gap_matrix(ledger: dict[str, Any]) -> dict[str, Any]:
    direct_guards = {
        item["label"]
        for item in ledger["capabilities"]
        if item["kind"] == "runtime_guard" and item["reachability"]["grammar"] == "direct"
    }
    source_authority = ledger["sourceAuthority"]
    kernel_only = [
        "utc_time_window",
        "any",
        "not",
        "consecutive_true",
        "event_age_window",
        "action_cooldown_elapsed",
        "state_age_at_most",
        "execution_reason_is",
    ]
    gaps = [
        {
            "gapId": f"grammar-route.{name}",
            "kind": "kernel_executable_grammar_no_direct_seed",
            "capability": name,
            "status": "confirmed" if name not in direct_guards else "recheck_required",
            "smallestMissingPrimitive": "sealed grammar production plus seed/mutation route",
            "evidence": "FuzzFolio guard model exists; current Rust typed grammar does not emit the guard kind.",
        }
        for name in kernel_only
    ]
    gaps.extend(
        [
            {
                "gapId": "grammar-route.predicate-edge-falling",
                "kind": "kernel_executable_grammar_no_directional_choice",
                "capability": "predicate_edge.falling",
                "status": "confirmed",
                "smallestMissingPrimitive": "sealed falling-direction grammar choice and source-bound mutation route",
                "evidence": "The grammar emits predicate_edge with direction=rising only.",
            },
            {
                "gapId": "authority-route.h4-d1",
                "kind": "catalog_available_frozen_authority_unknown",
                "capability": "H4/D1",
                "status": "unavailable_without_frozen_authority_fixture",
                "smallestMissingPrimitive": "read-only frozen authority fixture inspection",
                "evidence": "The catalog contains the timeframes; this stage does not open a run authority or infer active policy.",
            },
            {
                "gapId": "grammar-route.regime-session-spread",
                "kind": "no_direct_grammar_route_found",
                "capability": "regime/session/spread",
                "status": "strong_source_hypothesis",
                "smallestMissingPrimitive": "sealed fact source plus grammar production and validator contract",
                "evidence": "No direct production is present in the typed grammar registry.",
            },
            {
                "gapId": "topology-route.capture-latch",
                "kind": "topology_semantics_underidentified",
                "capability": "capture/latch",
                "status": "strong_source_hypothesis",
                "smallestMissingPrimitive": "explicit persistent capture state with deterministic clear/rearm semantics",
                "evidence": "Topology operations add/rewrite regions but do not name a capture/latch semantic primitive.",
            },
            {
                "gapId": "policy-route.abstention-timeout-fallback",
                "kind": "partial_timeout_without_explicit_policy_route",
                "capability": "abstention timeout/fallback/rearm",
                "status": "partially_represented",
                "smallestMissingPrimitive": "explicit abstention/fallback policy production; bounded rearm is separately present",
                "evidence": "Topology has timeout rearm insert/remove; no explicit abstention/fallback family is enumerated.",
            },
            {
                "gapId": "credit-route.side-credit-portfolio-credit",
                "kind": "credit_observability_gap",
                "capability": "side/portfolio/component credit",
                "status": "confirmed",
                "smallestMissingPrimitive": "predeclared lineage-to-outcome attribution fields and an authorized behavioral study",
                "evidence": "Source shows candidate/operator lineage and runtime traces but not component-local or portfolio-local selection credit.",
            },
            {
                "gapId": "archive-route.learned-insertion-prior",
                "kind": "operator_learning_not_established",
                "capability": "archive-informed learned insertion prior",
                "status": "confirmed_absent_from_static_atlas",
                "smallestMissingPrimitive": "authorized archive/selection policy change",
                "evidence": "Stage boundary forbids archive or production-policy mutation; static operator IDs do not establish learned weighting.",
            },
        ]
    )
    return {
        "schemaVersion": "evolutionary_substrate_gap_matrix_v1",
        "sourceAuthority": source_authority,
        "method": "static source comparison only; no market/run corpus or worker evidence read",
        "gaps": gaps,
    }


def build_summary(ledger: dict[str, Any], gaps: dict[str, Any]) -> str:
    counts = ledger["sourceCounts"]
    confirmed = sum(1 for gap in gaps["gaps"] if gap["status"].startswith("confirmed"))
    return "\n".join(
        [
            "# Evolutionary Substrate Atlas — generated summary",
            "",
            "This file is generated from committed source only. It contains no market, P&L, worker, or run-corpus evidence.",
            "",
            f"- Grammar fragments: {counts['grammarFragments']}",
            f"- Runtime guards: {counts['runtimeGuards']}",
            f"- Runtime actions: {counts['runtimeActions']}",
            f"- Topology operations: {counts['topologyOperations']}",
            f"- Operator families: {counts['operatorFamilies']}",
            f"- Catalog indicators: {counts['catalogIndicators']}",
            f"- Confirmed static gaps: {confirmed}",
            "",
            "The ledger records language/runtime support separately from frozen-authority eligibility, compilation, activation, retention, selection, and causal credit.",
            "",
        ]
    )


def main_for_test(ar_root: Path, ff_root: Path, output_dir: Path) -> int:
    """Generate an atlas from already-isolated worktrees for the focused test."""

    ledger = build_atlas(ar_root.resolve(), ff_root.resolve())
    gaps = build_gap_matrix(ledger)
    write_json(output_dir / "source-authority-map.json", ledger["sourceAuthority"])
    write_json(output_dir / "capability-ledger.json", ledger)
    write_json(output_dir / "gap-matrix.json", gaps)
    (output_dir / "generated-summary.md").write_text(
        build_summary(ledger, gaps), encoding="utf-8"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--autoresearch-root", type=Path, required=True)
    parser.add_argument("--fuzzfolio-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    ar_root = args.autoresearch_root.resolve()
    ff_root = args.fuzzfolio_root.resolve()
    if not (ar_root / ".git").exists() or not (ff_root / ".git").exists():
        raise AtlasError("both inputs must be isolated Git worktrees")
    return main_for_test(ar_root, ff_root, args.output_dir)


if __name__ == "__main__":
    raise SystemExit(main())
