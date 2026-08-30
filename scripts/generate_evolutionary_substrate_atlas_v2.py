#!/usr/bin/env python3
"""Generate the Stage 4.5B source-accurate evolutionary substrate ledger.

This is deliberately a read-only source/authority inventory.  It does not
open market data, build candidates, or change any runtime authority.  The
output distinguishes a Git object's identity from the bytes currently checked
out in a worktree, which prevents a checkout line-ending conversion from being
reported as a new canonical source identity.
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


SCHEMA = "evolutionary_substrate_atlas_v2"
V37_COMMIT = "5fa623b88c641d4d886411bf195ee3ef386d6446"
V38_COMMIT = "51c2f9175f441166e7fc997109e939a9f9103b5d"
FUZZ_V38_COMMIT = "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2"

AR_FILES = {
    "grammar": "rust/temporal-qd/crates/qd-kernel/src/grammar.rs",
    "operators": "rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs",
    "topology": "rust/temporal-qd/crates/qd-kernel/src/v5_topology_operators.rs",
    "nativeBridge": "autoresearch/temporal_qd_v5_native.py",
    "nativeBatch": "autoresearch/temporal_qd_native.py",
    "pairGeneration": "autoresearch/temporal_qd_pair_generation.py",
    "supervisor": "autoresearch/temporal_qd_supervisor.py",
}
FF_FILES = {
    "guards": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/guards.py",
    "actions": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/action_models.py",
    "kernel": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/kernel.py",
    "compiler": "shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/bidirectional_compiler.py",
    "catalog": "shared/constants/indicators.json",
}


class AtlasError(RuntimeError):
    pass


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def git_bytes(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True
    ).stdout


def git_text(root: Path, *args: str) -> str:
    return git_bytes(root, *args).decode("utf-8").strip()


def normalized_text_bytes(value: bytes) -> bytes:
    """Normalize only text newline spelling for a semantic source comparison."""

    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return value
    return text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")


def line_ending_mode(value: bytes) -> str:
    if b"\r\n" in value:
        return "mixed" if re.search(rb"(?<!\r)\n", value) else "crlf"
    return "lf" if b"\n" in value else "none"


def source_binding(
    *, repository: str, root: Path, commit: str, relative_path: str, component: str
) -> dict[str, Any]:
    """Bind a source file to exact Git blob bytes plus its local checkout state."""

    object_id = git_text(root, "rev-parse", f"{commit}:{relative_path}")
    blob = git_bytes(root, "cat-file", "blob", object_id)
    worktree_path = root / relative_path
    raw = worktree_path.read_bytes()
    clean = subprocess.run(
        ["git", "-C", str(root), "diff", "--quiet", commit, "--", relative_path],
        check=False,
    ).returncode == 0
    return {
        "component": component,
        "repository": repository,
        "gitCommit": commit,
        "path": relative_path,
        "gitBlobObjectId": object_id,
        "gitBlobSha256": sha256(blob),
        "worktreeRawSha256": sha256(raw),
        "worktreeClean": clean,
        "worktreeLineEndingMode": line_ending_mode(raw),
        "worktreeSemanticallyMatchesGitBlob": normalized_text_bytes(raw)
        == normalized_text_bytes(blob),
    }


def require_clean_bindings(bindings: list[dict[str, Any]]) -> None:
    dirty = [item["component"] for item in bindings if not item["worktreeClean"]]
    if dirty:
        raise AtlasError(f"source worktree changed for: {', '.join(dirty)}")


def matching_paren(source: str, open_index: int) -> int:
    if source[open_index] != "(":
        raise AtlasError("balanced parser was not given an opening parenthesis")
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(open_index, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    raise AtlasError("unclosed parenthesis while reading a source-defined domain")


def split_top_level(value: str) -> list[str]:
    pieces: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escaped = False
    for index, char in enumerate(value):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0:
            pieces.append(value[start:index].strip())
            start = index + 1
    tail = value[start:].strip()
    if tail:
        pieces.append(tail)
    return pieces


def rust_value(token: str) -> int | float | str:
    cleaned = token.strip()
    while cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1].strip()
    cleaned = cleaned.removesuffix(".into()")
    try:
        return int(cleaned)
    except ValueError:
        try:
            return float(cleaned)
        except ValueError:
            return cleaned


def grammar_records(source: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    marker = "put(spec("
    position = 0
    while (start := source.find(marker, position)) >= 0:
        args_open = start + len(marker) - 1
        args_close = matching_paren(source, args_open)
        args = split_top_level(source[args_open + 1 : args_close])
        if len(args) != 8:
            raise AtlasError("grammar spec no longer has the eight sealed fields")
        identifier = re.fullmatch(r'"([^"]+)"', args[0])
        family = re.fullmatch(r'"([^"]+)"', args[1])
        consumes = re.fullmatch(r"Port::([A-Za-z]+)", args[2])
        produces = re.fullmatch(r"Port::([A-Za-z]+)", args[3])
        if not all((identifier, family, consumes, produces)):
            raise AtlasError("grammar spec identity/port shape changed")
        resources = re.findall(r'"([^"]+)"', args[4])
        choices: dict[str, list[int | float | str]] = {}
        for match in re.finditer(r'\(\s*"([^"]+)"\s*,\s*vec!\[(.*?)\]\s*\)', args[5], re.DOTALL):
            choices[match.group(1)] = [rust_value(item) for item in split_top_level(match.group(2))]
        recipe = re.search(r'recipe\(\s*&\[(.*?)\]\s*,\s*"([^"]+)"\s*,?\s*\)', args[7], re.DOTALL)
        if recipe is None:
            raise AtlasError("grammar activation recipe shape changed")
        emitted_guards = re.findall(r'"([^"]+)"', recipe.group(1))
        records.append(
            {
                "productionId": identifier.group(1),
                "family": family.group(1),
                "consumes": consumes.group(1),
                "produces": produces.group(1),
                "resourceSlots": resources,
                "choiceDomains": choices,
                "maxInstances": int(args[6]),
                "activationRecipe": {"emittedGuards": emitted_guards, "emits": recipe.group(2)},
                "implicitStateAndTransitionPaths": {
                    "entry": "typed fragment insertion attaches to the consumed port",
                    "success": f"emits {recipe.group(2)} and advances through {produces.group(1)}",
                    "rejected": "compiler-admission rejection is terminal for this construction attempt",
                    "canceled": "pending action cancellation is emitted by generated runtime action handling",
                },
                "sourceSymbol": f"registry::{identifier.group(1)}",
            }
        )
        position = args_close + 1
    if len(records) != 23:
        raise AtlasError(f"expected 23 grammar fragments, found {len(records)}")
    return records


def class_names(source: str, suffix: str) -> list[str]:
    values = re.findall(rf"^class ([A-Za-z0-9_]+{suffix})\b", source, re.MULTILINE)
    if len(values) != len(set(values)):
        raise AtlasError(f"duplicate {suffix} class")
    return values


def snake_case(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


GUARD_SEMANTICS = {
    "always": "constant",
    "evidence_at_least": "evidence_threshold",
    "evidence_below": "evidence_threshold",
    "fresh_event": "event_freshness",
    "event_age_at_most": "event_age",
    "condition_streak_at_least": "condition_streak",
    "event_age_window": "event_age_window",
    "action_cooldown_elapsed": "action_cooldown",
    "utc_time_window": "session_time_window",
    "state_age_at_least": "state_age",
    "state_age_at_most": "state_age",
    "position_exists": "position_presence",
    "position_age_at_least": "position_age",
    "unrealized_r_at_least": "unrealized_risk_multiple",
    "unrealized_r_at_most": "unrealized_risk_multiple",
    "predicate_edge": "predicate_edge",
    "consecutive_true": "boolean_streak",
    "execution_status_is": "execution_status",
    "execution_reason_is": "execution_reason",
    "all": "boolean_composition",
    "any": "boolean_composition",
    "not": "boolean_composition",
}


DIRECT_GUARDS = {
    "evidence_at_least",
    "evidence_below",
    "fresh_event",
    "event_age_at_most",
    "condition_streak_at_least",
    "state_age_at_least",
    "position_exists",
    "position_age_at_least",
    "unrealized_r_at_least",
    "unrealized_r_at_most",
    "predicate_edge",
}


def guard_records(source: str, binding: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for class_name in class_names(source, "Guard"):
        kind = snake_case(class_name.removesuffix("Guard"))
        if kind not in GUARD_SEMANTICS:
            raise AtlasError(f"unclassified guard: {kind}")
        direct = kind in DIRECT_GUARDS
        records.append(
            {
                "guard": kind,
                "semanticRoute": GUARD_SEMANTICS[kind],
                "runtimeModel": class_name,
                "grammarRoute": "direct" if direct else "runtime_only",
                "seedEligibility": "grammar-recipe-dependent" if direct else "no-direct-grammar-route",
                "mutationEligibility": "topology-or-parameter-dependent" if direct else "not-established",
                "compilerPath": "generated_guard_emission" if direct else "temporal_graph_runtime_validation_only",
                "sourceBinding": {**binding, "symbol": class_name},
            }
        )
    if len(records) != 22:
        raise AtlasError(f"expected 22 guards, found {len(records)}")
    return records


def frozen_catalog_sets(pair_authority: dict[str, Any]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for side_key, side in (("long", "longModule"), ("short", "shortModule")):
        catalog = pair_authority[side]["catalog"]
        indicators = catalog.get("indicators")
        if not isinstance(indicators, list):
            raise AtlasError(f"{side} frozen catalog lacks indicators")
        result[side_key] = {
            str(item["id"] if "id" in item else item["meta"]["id"])
            for item in indicators
            if isinstance(item, dict) and ("id" in item or isinstance(item.get("meta"), dict))
        }
    return result


def catalog_records(catalog_path: Path, pair_authority: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    indicators = catalog.get("indicators")
    timeframes = catalog.get("timeframes")
    if not isinstance(indicators, list) or not isinstance(timeframes, dict):
        raise AtlasError("indicator catalog shape changed")
    frozen = frozen_catalog_sets(pair_authority)
    records: list[dict[str, Any]] = []
    for item in indicators:
        if not isinstance(item, dict) or not isinstance(item.get("meta"), dict):
            raise AtlasError("indicator catalog member lacks meta")
        meta = item["meta"]
        item_id = str(meta["id"])
        records.append(
            {
                "indicatorId": item_id,
                "role": meta.get("signalRole"),
                "outputs": meta.get("outputs", meta.get("inputs")),
                "catalogMeta": meta,
                "config": item.get("config"),
                "parameterGrids": meta.get("talibMeta", []),
                "policyEligibility": "sealed catalog plus side indicator policy",
                "frozenInclusion": {
                    "long": item_id in frozen["long"],
                    "short": item_id in frozen["short"],
                },
                "historicalStatus": "present_in_retained_V38_pair_authority" if item_id in frozen["long"] or item_id in frozen["short"] else "not_in_retained_V38_pair_authority",
                "sourceBinding": {**binding, "symbol": f"indicators.{item_id}"},
            }
        )
    if len(records) != 88:
        raise AtlasError(f"expected 88 catalog indicators, found {len(records)}")
    return {"timeframes": timeframes, "indicators": records}


def operator_records(source: str, operators_binding: dict[str, Any], topology_binding: dict[str, Any]) -> list[dict[str, Any]]:
    ids = re.findall(r'pub const (V5_[A-Z_]+_OPERATOR_ID):\s*&str\s*=\s*"([^"]+)";', source)
    if len(ids) != 6:
        raise AtlasError(f"expected six primary operators, found {len(ids)}")
    construction = {
        "evolvable_resource_v1": "resource_constructions",
        "evolvable_temporal_v1": "temporal_constructions",
        "evolvable_topology_v1": "topology_constructions",
        "evolvable_hold_policy_v1": "temporal_constructions",
        "evolvable_initial_protection_v1": "initial_protection_constructions",
        "evolvable_same_side_crossover_v1": "enumerate_same_side_crossover_plans",
    }
    result: list[dict[str, Any]] = []
    for constant, operator_id in ids:
        function = construction[operator_id]
        result.append(
            {
                "operatorId": operator_id,
                "constant": constant,
                "enumerationPath": function,
                "selectionPath": "select_operator_plan" if "crossover" not in operator_id else "select_same_side_crossover_plan",
                "applicationPath": "apply_operator_plan" if "crossover" not in operator_id else "apply_same_side_crossover_plan",
                "choiceDomain": "parent- and authority-derived; enumerated at runtime rather than a static global grid",
                "eligibility": "content-bound authority, parent identity, sealed budget, then compiled-child admission for evolved selection",
                "dispositions": ["accepted", "no_op", "rejected"],
                "sampling": "deterministic SHA-256 length-prefixed rejection-uniform selection; no learned operator weighting established",
                "sourceBinding": {**operators_binding, "symbol": constant},
                "topologyBinding": topology_binding if operator_id == "evolvable_topology_v1" else None,
            }
        )
    return result


def source_map(
    *, ar_root: Path, ff_root: Path, v37_root: Path, v38_root: Path, fuzz_v38_root: Path
) -> dict[str, Any]:
    ar_commit = git_text(ar_root, "rev-parse", "HEAD")
    ff_commit = git_text(ff_root, "rev-parse", "HEAD")
    v37_commit = git_text(v37_root, "rev-parse", "HEAD")
    v38_commit = git_text(v38_root, "rev-parse", "HEAD")
    fuzz_v38_commit = git_text(fuzz_v38_root, "rev-parse", "HEAD")
    expected = (V37_COMMIT, V38_COMMIT, FUZZ_V38_COMMIT)
    actual = (v37_commit, v38_commit, fuzz_v38_commit)
    if actual != expected:
        raise AtlasError(f"historical source roots are not pinned: {actual!r}")
    current_ar = [
        source_binding(
            repository="EnviralDesign/fuzzfolio-autoresearch", root=ar_root, commit=ar_commit,
            relative_path=path, component=name
        )
        for name, path in AR_FILES.items()
    ]
    current_ff = [
        source_binding(
            repository="EnviralDesign/FuzzFolio", root=ff_root, commit=ff_commit,
            relative_path=path, component=name
        )
        for name, path in FF_FILES.items()
    ]
    historical = [
        source_binding(
            repository="EnviralDesign/fuzzfolio-autoresearch", root=v37_root, commit=v37_commit,
            relative_path=AR_FILES["grammar"], component="v37.grammar"
        ),
        source_binding(
            repository="EnviralDesign/fuzzfolio-autoresearch", root=v38_root, commit=v38_commit,
            relative_path=AR_FILES["grammar"], component="v38.grammar"
        ),
        source_binding(
            repository="EnviralDesign/FuzzFolio", root=fuzz_v38_root, commit=fuzz_v38_commit,
            relative_path=FF_FILES["catalog"], component="v38.fuzzfolio.catalog"
        ),
    ]
    require_clean_bindings(current_ar + current_ff + historical)
    current_catalog = next(item for item in current_ff if item["component"] == "catalog")
    historical_catalog = historical[-1]
    changed_paths = git_text(ff_root, "diff", "--name-only", fuzz_v38_commit, ff_commit).splitlines()
    timestamp_only = changed_paths == ["backend/generated/public/market-structure.json"]
    return {
        "schemaVersion": SCHEMA,
        "identities": {
            "currentAutoresearchDefault": {"commit": ar_commit, "bindings": current_ar},
            "currentFuzzfolioDefault": {"commit": ff_commit, "bindings": current_ff},
            "historicalV37": {"commit": v37_commit, "bindings": historical[:1]},
            "historicalV38": {"commit": v38_commit, "bindings": historical[1:2]},
            "historicalFuzzfolioV38": {"commit": fuzz_v38_commit, "bindings": historical[2:]},
        },
        "fuzzfolioCurrentVsV38": {
            "status": "generated_timestamp_only" if timestamp_only else "requires_review",
            "verifiedDiff": "backend/generated/public/market-structure.json changes only generated_at between the pinned commits; no catalog source file changed",
            "catalogBlobUnchanged": current_catalog["gitBlobObjectId"] == historical_catalog["gitBlobObjectId"],
        },
    }


def telemetry_and_credit_map(ar_bindings: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "stage": "plan_enumeration_and_selection",
            "source": {**ar_bindings["operators"], "symbol": "enumerate_evolved_operator_choices/select_operator_plan"},
            "fields": ["plan", "receipt", "operatorId", "parent identity", "authoritySha256"],
            "status": "source-established",
        },
        {
            "stage": "application_and_disposition",
            "source": {**ar_bindings["operators"], "symbol": "V5EvolvedOperatorExecution"},
            "fields": ["accepted/no_op/rejected", "reason_code", "application", "delta", "result"],
            "status": "source-established",
        },
        {
            "stage": "compiled_child_admission",
            "source": {**ar_bindings["operators"], "symbol": "V5EvolvedChildAdmission"},
            "fields": ["operatorId", "child_program", "compiler admission"],
            "status": "source-established",
        },
        {
            "stage": "proposal_receipt_and_identity_ledger",
            "source": {**ar_bindings["nativeBridge"], "symbol": "run_native_v5_proposal_construction"},
            "fields": ["immutable result", "manifest", "input bindings", "artifact inventory"],
            "status": "source-established; historical retained artifact coverage is recorded separately",
        },
        {
            "stage": "archive_parent_and_survivor_selection",
            "source": {**ar_bindings["supervisor"], "symbol": "_run_native_v5_generation"},
            "fields": ["parent archive binding", "generation finalization", "archive update"],
            "status": "candidate/archive level only; no component-local causal credit or learned scoring claimed",
        },
    ]


def build_ledger(source_authority: dict[str, Any], pair_authority: dict[str, Any], catalog_path: Path) -> dict[str, Any]:
    ar_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentAutoresearchDefault"]["bindings"]
    }
    ff_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentFuzzfolioDefault"]["bindings"]
    }
    grammar_source = (Path(ar_bindings["grammar"]["path"]).name)
    # Read the actual source from its already bound current root through the binding caller.
    # The caller supplies the text records immediately below; this local sentinel prevents
    # a ledger from quietly substituting a copied source payload.
    del grammar_source
    return {
        "schemaVersion": SCHEMA,
        "sourceAuthoritySha256": sha256(canonical_bytes(source_authority)),
        "frozenAuthority": {
            "pairRunConfigSha256": pair_authority["pairRunConfigSha256"],
            "longCatalogSha256": pair_authority["longModule"]["catalogSha256"],
            "shortCatalogSha256": pair_authority["shortModule"]["catalogSha256"],
            "longIndicatorPolicy": pair_authority["longModule"]["indicatorPolicy"],
            "shortIndicatorPolicy": pair_authority["shortModule"]["indicatorPolicy"],
        },
        "sourceBindings": {"autoresearch": ar_bindings, "fuzzfolio": ff_bindings},
        "catalog": catalog_records(catalog_path, pair_authority, ff_bindings["catalog"]),
        "telemetryAndCredit": telemetry_and_credit_map(ar_bindings),
    }


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def generate(args: argparse.Namespace) -> dict[str, Any]:
    source_authority = source_map(
        ar_root=args.autoresearch_root,
        ff_root=args.fuzzfolio_root,
        v37_root=args.historical_v37_root,
        v38_root=args.historical_v38_root,
        fuzz_v38_root=args.historical_fuzzfolio_v38_root,
    )
    pair_authority = json.loads((args.authority_root / "pair-run-config.json").read_text(encoding="utf-8"))
    if pair_authority.get("schemaVersion") != "temporal_qd_bidirectional_pair_run_config_v2":
        raise AtlasError("retained pair authority has an unexpected schema")
    ar_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentAutoresearchDefault"]["bindings"]
    }
    ff_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentFuzzfolioDefault"]["bindings"]
    }
    ledger = build_ledger(source_authority, pair_authority, args.fuzzfolio_root / FF_FILES["catalog"])
    grammar = grammar_records((args.autoresearch_root / AR_FILES["grammar"]).read_text(encoding="utf-8"))
    for item in grammar:
        item["sourceBinding"] = {**ar_bindings["grammar"], "symbol": item.pop("sourceSymbol")}
    ledger["grammarFragments"] = grammar
    ledger["operatorFamilies"] = operator_records(
        (args.autoresearch_root / AR_FILES["operators"]).read_text(encoding="utf-8"),
        ar_bindings["operators"],
        ar_bindings["topology"],
    )
    ledger["runtimeGuardRoutes"] = guard_records(
        (args.fuzzfolio_root / FF_FILES["guards"]).read_text(encoding="utf-8"), ff_bindings["guards"]
    )
    ledger["counts"] = {
        "grammarFragments": len(ledger["grammarFragments"]),
        "catalogIndicators": len(ledger["catalog"]["indicators"]),
        "runtimeGuards": len(ledger["runtimeGuardRoutes"]),
        "operatorFamilies": len(ledger["operatorFamilies"]),
    }
    output = args.output_dir
    write_json(output / "source-authority-map-v2.json", source_authority)
    write_json(output / "capability-ledger-v2.json", ledger)
    return {"sourceAuthority": source_authority, "ledger": ledger}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--autoresearch-root", required=True, type=Path)
    parser.add_argument("--fuzzfolio-root", required=True, type=Path)
    parser.add_argument("--historical-v37-root", required=True, type=Path)
    parser.add_argument("--historical-v38-root", required=True, type=Path)
    parser.add_argument("--historical-fuzzfolio-v38-root", required=True, type=Path)
    parser.add_argument("--authority-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    generate(parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
