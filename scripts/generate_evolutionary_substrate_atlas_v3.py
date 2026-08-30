#!/usr/bin/env python3
"""Generate the Stage 4.5C static, source-bound substrate inventory.

This exporter supersedes the V2 catalogue/guard representation without
altering a production grammar, operator, compiler, or policy.  It preserves
unknowns rather than treating a catalog input as an implementation output or a
runtime guard as directly evolvable merely because it is valid at runtime.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import generate_evolutionary_substrate_atlas_v2 as v2


SCHEMA = "evolutionary_substrate_atlas_v3"


def canonical_bytes(value: object) -> bytes:
    return v2.canonical_bytes(value)


def field(values: list[Any], *, status: str, evidence: str) -> dict[str, Any]:
    return {"values": values, "status": status, "evidence": evidence}


def exact_list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def event_binding(meta: dict[str, Any]) -> dict[str, Any] | None:
    substitution = meta.get("familySubstitution")
    if not isinstance(substitution, dict):
        return None
    schema = substitution.get("eventOutputSchema")
    return schema if isinstance(schema, dict) else None


def indicator_records(catalog_path: Path, pair_authority: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    indicators = catalog.get("indicators")
    timeframes = catalog.get("timeframes")
    if not isinstance(indicators, list) or not isinstance(timeframes, dict):
        raise v2.AtlasError("indicator catalog shape changed")
    frozen = v2.frozen_catalog_sets(pair_authority)
    records: list[dict[str, Any]] = []
    for item in indicators:
        if not isinstance(item, dict) or not isinstance(item.get("meta"), dict):
            raise v2.AtlasError("indicator catalog member lacks meta")
        meta = item["meta"]
        item_id = str(meta["id"])
        binding_outputs = event_binding(meta)
        management_outputs = exact_list(meta.get("managementScalarOutputs"))
        value_range = meta.get("valueRange")
        raw_outputs = exact_list(meta.get("rawImplementationOutputs"))
        processed_outputs = exact_list(meta.get("processedDirectionalOutputs"))
        evidence_outputs = exact_list(meta.get("evidenceValueOutputs"))
        records.append(
            {
                "indicatorId": item_id,
                "catalogMeta": meta,
                "config": item.get("config"),
                "parameterGrids": meta.get("talibMeta", []),
                "requiredInputs": field(
                    exact_list(meta.get("inputs")),
                    status="catalog_declared",
                    evidence="catalog meta.inputs",
                ),
                "rawImplementationOutputs": field(
                    raw_outputs,
                    status="catalog_declared" if "rawImplementationOutputs" in meta else "unavailable_in_catalog",
                    evidence="catalog meta.rawImplementationOutputs when present; inputs are never substituted",
                ),
                "processedDirectionalOutputs": field(
                    processed_outputs,
                    status="catalog_declared" if "processedDirectionalOutputs" in meta else "unavailable_in_catalog",
                    evidence="catalog meta.processedDirectionalOutputs when present",
                ),
                "eventBindingOutputs": field(
                    [binding_outputs] if binding_outputs else [],
                    status="family_substitution_declared" if binding_outputs else "not_an_event_binding_or_unavailable",
                    evidence="catalog meta.familySubstitution.eventOutputSchema",
                ),
                "evidenceValueOutputs": field(
                    evidence_outputs,
                    status="catalog_declared" if "evidenceValueOutputs" in meta else "unavailable_in_catalog",
                    evidence="catalog meta.evidenceValueOutputs when present; valueRange is recorded separately",
                ),
                "managementScalarOutputs": field(
                    management_outputs,
                    status="catalog_declared" if management_outputs else "not_declared",
                    evidence="catalog meta.managementScalarOutputs",
                ),
                "valueRange": value_range,
                "outputKind": (
                    "directional_event_binding"
                    if binding_outputs
                    else "management_scalar"
                    if management_outputs
                    else "configured_value_range"
                    if isinstance(value_range, dict)
                    else "unavailable"
                ),
                "signalRole": meta.get("signalRole"),
                "signalPersistence": meta.get("signalPersistence"),
                "familySubstitutionClass": (
                    meta.get("familySubstitution", {}).get("substitutionClass")
                    if isinstance(meta.get("familySubstitution"), dict)
                    else None
                ),
                "policyEligibility": "sealed catalog plus side indicator policy",
                "frozenInclusion": {"long": item_id in frozen["long"], "short": item_id in frozen["short"]},
                "historicalStatus": (
                    "present_in_retained_V38_pair_authority"
                    if item_id in frozen["long"] or item_id in frozen["short"]
                    else "not_in_retained_V38_pair_authority"
                ),
                "sourceBinding": {**binding, "symbol": f"indicators.{item_id}"},
            }
        )
    if len(records) != 88:
        raise v2.AtlasError(f"expected 88 catalog indicators, found {len(records)}")
    return {"timeframes": timeframes, "indicators": records}


TOPOLOGY_GENERATED = {"always", "all", "any", "not"}
COMPILER_GENERATED = {"always", "all", "any", "not", "execution_status_is"}
DIRECT_MUTATION_GUARDS = set(v2.DIRECT_GUARDS)


def guard_records(
    *, guards_source: str, grammar_source: str, v5_source: str, topology_source: str, binding: dict[str, Any]
) -> list[dict[str, Any]]:
    grammar_records = v2.grammar_records(grammar_source)
    recipe_guards = {
        guard
        for fragment in grammar_records
        for guard in fragment["activationRecipe"]["emittedGuards"]
    }
    records: list[dict[str, Any]] = []
    for class_name in v2.class_names(guards_source, "Guard"):
        guard = v2.snake_case(class_name.removesuffix("Guard"))
        if guard not in v2.GUARD_SEMANTICS:
            raise v2.AtlasError(f"unclassified guard: {guard}")
        direct = guard in DIRECT_MUTATION_GUARDS and guard in recipe_guards
        compiler_generated = guard in COMPILER_GENERATED and f'"{guard}"' in v5_source
        topology_generated = guard in TOPOLOGY_GENERATED and f'"{guard}"' in topology_source
        mutable_support = guard in DIRECT_MUTATION_GUARDS and f'"{guard}"' in v5_source
        records.append(
            {
                "guard": guard,
                "semanticRoute": v2.GUARD_SEMANTICS[guard],
                "runtimeModel": class_name,
                "directGrammarProduction": direct,
                "compilerGenerated": compiler_generated,
                "topologyGenerated": topology_generated,
                "runtimeHandAuthorable": True,
                "seedReachable": direct or compiler_generated,
                "oneStepMutationReachable": mutable_support,
                "parameterMutable": "requires_exact_plan_enumeration" if mutable_support else "not_established",
                "removable": "requires_exact_plan_enumeration",
                "historicallyAuthored": "resolved_by_historical_coverage_join",
                "historicallyActivated": "resolved_only_when_runtime_evidence_exists",
                "sourceTrace": {
                    "grammarRecipe": guard in recipe_guards,
                    "compilerSource": compiler_generated,
                    "topologySource": topology_generated,
                    "runtimeValidation": True,
                },
                "sourceBinding": {**binding, "symbol": class_name},
            }
        )
    if len(records) != 22:
        raise v2.AtlasError(f"expected 22 guards, found {len(records)}")
    return records


def flattened_grammar_choices(grammar: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "recordType": "grammar_choice_domain",
            "productionId": fragment["productionId"],
            "family": fragment["family"],
            "field": field_name,
            "values": values,
            "sourceBinding": fragment["sourceBinding"],
        }
        for fragment in grammar
        for field_name, values in fragment["choiceDomains"].items()
    ]


def static_modulation_inventory(
    grammar: list[dict[str, Any]], operators: list[dict[str, Any]], pair_authority: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schemaVersion": "evolutionary_substrate_modulation_inventory_v3",
        "grammarChoiceDomains": flattened_grammar_choices(grammar),
        "operatorFamilies": operators,
        "frozenIndicatorEligibility": {
            side: pair_authority[f"{side}Module"]["indicatorPolicy"]
            for side in ("long", "short")
        },
        "runtimePlanEnumerationRequirement": (
            "resource, temporal, topology, hold, initial-protection, and crossover sub-operations "
            "remain parent- and authority-derived until the canonical Rust enumeration exporter runs"
        ),
    }


def field_telemetry_map(bindings: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        ("operator plan", "plan_enumeration", "computed", "retained in proposal attempt when executed"),
        ("parent/operator/side route", "operator_selection", "computed", "retained in lineage receipts when executed"),
        ("compiler admission", "evolved_child_admission", "computed", "retained in accepted construction evidence"),
        ("candidate/program/phenotype identity", "identity_ledger", "computed", "retained in compact identity ledger"),
        ("archive descriptor/objective", "native_finalization", "computed", "archive-visible"),
        ("parent selection input", "parent_selection", "computed", "selection-read"),
        ("component/side/lineage credit", "learning", "not_established", "not operator-learning-read"),
        ("state occupancy/transition counts", "runtime_execution", "runtime_computed", "not established in compact construction receipts"),
        ("guard evaluations/action realization", "runtime_execution", "runtime_computed", "not established in compact construction receipts"),
    ]
    return [
        {
            "field": name,
            "stage": stage,
            "availability": availability,
            "retention": retention,
            "sourceBinding": bindings["operators"] if stage.startswith("operator") else bindings["supervisor"],
        }
        for name, stage, availability, retention in fields
    ]


def generate(args: argparse.Namespace) -> dict[str, Any]:
    source_authority = v2.source_map(
        ar_root=args.autoresearch_root,
        ff_root=args.fuzzfolio_root,
        v37_root=args.historical_v37_root,
        v38_root=args.historical_v38_root,
        fuzz_v38_root=args.historical_fuzzfolio_v38_root,
    )
    source_authority["schemaVersion"] = SCHEMA
    pair_authority = json.loads((args.authority_root / "pair-run-config.json").read_text(encoding="utf-8"))
    if pair_authority.get("schemaVersion") != "temporal_qd_bidirectional_pair_run_config_v2":
        raise v2.AtlasError("retained pair authority has an unexpected schema")
    ar_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentAutoresearchDefault"]["bindings"]
    }
    ff_bindings = {
        item["component"]: item
        for item in source_authority["identities"]["currentFuzzfolioDefault"]["bindings"]
    }
    grammar_source = (args.autoresearch_root / v2.AR_FILES["grammar"]).read_text(encoding="utf-8")
    grammar = v2.grammar_records(grammar_source)
    for item in grammar:
        item["sourceBinding"] = {**ar_bindings["grammar"], "symbol": item.pop("sourceSymbol")}
    operators_source = (args.autoresearch_root / v2.AR_FILES["operators"]).read_text(encoding="utf-8")
    operators = v2.operator_records(
        operators_source, ar_bindings["operators"], ar_bindings["topology"]
    )
    ledger = {
        "schemaVersion": SCHEMA,
        "sourceAuthoritySha256": v2.sha256(canonical_bytes(source_authority)),
        "frozenAuthority": {
            "pairRunConfigSha256": pair_authority["pairRunConfigSha256"],
            "longCatalogSha256": pair_authority["longModule"]["catalogSha256"],
            "shortCatalogSha256": pair_authority["shortModule"]["catalogSha256"],
        },
        "sourceBindings": {"autoresearch": ar_bindings, "fuzzfolio": ff_bindings},
        "catalog": indicator_records(
            args.fuzzfolio_root / v2.FF_FILES["catalog"], pair_authority, ff_bindings["catalog"]
        ),
        "grammarFragments": grammar,
        "operatorFamilies": operators,
        "runtimeGuardRoutes": guard_records(
            guards_source=(args.fuzzfolio_root / v2.FF_FILES["guards"]).read_text(encoding="utf-8"),
            grammar_source=grammar_source,
            v5_source=(args.autoresearch_root / "rust/temporal-qd/crates/qd-kernel/src/v5.rs").read_text(encoding="utf-8"),
            topology_source=(args.autoresearch_root / v2.AR_FILES["topology"]).read_text(encoding="utf-8"),
            binding=ff_bindings["guards"],
        ),
        "fieldTelemetryAndLearning": field_telemetry_map(ar_bindings),
    }
    ledger["counts"] = {
        "grammarFragments": len(grammar),
        "grammarChoiceDomains": len(flattened_grammar_choices(grammar)),
        "catalogIndicators": len(ledger["catalog"]["indicators"]),
        "runtimeGuards": len(ledger["runtimeGuardRoutes"]),
        "operatorFamilies": len(operators),
    }
    inventory = static_modulation_inventory(grammar, operators, pair_authority)
    inventory["sourceAuthoritySha256"] = ledger["sourceAuthoritySha256"]
    v2.write_json(args.output_dir / "source-authority-map-v3.json", source_authority)
    v2.write_json(args.output_dir / "capability-ledger-v3.json", ledger)
    v2.write_json(args.output_dir / "modulation-inventory-v3.json", inventory)
    return {"sourceAuthority": source_authority, "ledger": ledger, "inventory": inventory}


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
