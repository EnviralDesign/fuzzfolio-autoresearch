#!/usr/bin/env python3
"""Reduce sealed Stage 4.5D ledgers into the Stage 4.5E static atlas.

This is deliberately a read-only reducer.  It does not invoke the research
engine, inspect market data, or materialize a candidate/archive artifact.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "evolutionary_substrate_stage45e_static_atlas_v1"
FAMILY_BY_KIND = {
    "hold": "hold",
    "initial_protection": "initial_protection",
    "typed_grammar": "typed_grammar",
    "resource": "indicator_learning",
    "temporal": "indicator_learning",
}
PROTECTION_WEIGHTS = {"adjacent": 70, "jump": 25, "kind_switch": 5}
SCALAR_KINDS = {
    "indicator_range_mutate",
    "indicator_period_mutate",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
    "evidence_threshold_mutate",
}


class SummaryError(ValueError):
    pass


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SummaryError(f"expected an object: {path}")
    return value


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            value = json.loads(line)
            if not isinstance(value, dict):
                raise SummaryError(f"JSONL row {number} is not an object: {path}")
            values.append(value)
    return values


def rows(report: dict[str, Any], label: str) -> list[dict[str, Any]]:
    value = report.get("rows")
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise SummaryError(f"{label} has malformed rows")
    return value


def counts(values: Iterable[object]) -> dict[str, int]:
    return dict(sorted(collections.Counter(str(value) for value in values).items()))


def artifact(path: Path) -> dict[str, Any]:
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha256(path)}


def check_report_hash(report: dict[str, Any], label: str) -> str:
    expected = report.get("reportSha256")
    if not isinstance(expected, str):
        raise SummaryError(f"{label} has no reportSha256")
    value = dict(report)
    value.pop("reportSha256", None)
    actual = canonical_sha256(value)
    if actual != expected:
        raise SummaryError(f"{label} report hash drift: {actual} != {expected}")
    return actual


def accepted(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in rows(report, "static report") if row.get("terminalStage") == "accepted_full_pair"]


def construction(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("nativePlanConstruction")
    return value if isinstance(value, dict) else {}


def topology_trace(row: dict[str, Any]) -> dict[str, Any]:
    trace = row.get("deltaGeometry", {}).get("mutationTrace")
    return trace if isinstance(trace, dict) else {}


def group_rows(values: list[dict[str, Any]], fields: tuple[str, ...]) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for value in values:
        key = tuple(value.get(field) for field in fields)
        grouped.setdefault(key, []).append(value)
    return grouped


def static_alias_groups(envelope: list[dict[str, Any]], identity_field: str) -> list[list[dict[str, Any]]]:
    groups = group_rows(envelope, ("parentCandidateId", "side", identity_field))
    return [members for _, members in sorted(groups.items()) if len(members) > 1]


def locality_class(row: dict[str, Any]) -> str:
    """Frozen ordered rules over construction, trace, keyed objects, and topology."""
    kind = row.get("constructionKind")
    choice = row.get("choiceKind")
    geometry = row.get("deltaGeometry", {})
    changed = geometry.get("changedPathCount", 0)
    categories = set((geometry.get("changeCounts") or {}).keys())
    trace = topology_trace(row)
    if row.get("childProgramSha256") == row.get("parentSideProgramSha256"):
        return "semantic_no_op_or_identity_only"
    if choice in {"hold", "initial_protection"}:
        return "hold_or_protection_policy_only"
    if kind in SCALAR_KINDS:
        return "scalar_or_parameter_only"
    if kind == "typed_guard_replace":
        return "one_existing_guard_local_change"
    if kind in {"indicator_substitute", "directional_event_substitute"}:
        return "one_existing_resource_object_change"
    if kind == "indicator_instance_insert":
        return "resource_plus_local_binding_change"
    if kind in {"directional_event_insert", "directional_event_remove"}:
        return "event_route_add_or_remove"
    if kind == "evidence_group_create":
        return "coherent_multi_object_single_motif_change"
    if choice == "typed_grammar":
        operation = trace.get("operation")
        if operation in {"insert_confirmation_rejection", "insert_timeout_rearm"}:
            return "coherent_multi_object_single_motif_change"
        if operation and changed <= 3 and categories <= {"state", "transition", "guard"}:
            return "single_structural_region_change"
        if operation:
            return "multi_region_or_cross_system_blast_radius"
    return "unknown"


def geometry_fingerprint(row: dict[str, Any]) -> str:
    geometry = row.get("deltaGeometry", {})
    return canonical_sha256({"changeCounts": geometry.get("changeCounts"), "changes": geometry.get("changes")})


def quantiles(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"min": None, "median": None, "max": None}
    ordered = sorted(values)
    return {"min": ordered[0], "median": ordered[(len(ordered) - 1) // 2], "max": ordered[-1]}


def exact_probability(row: dict[str, Any], vocabulary: list[dict[str, Any]]) -> dict[str, Any]:
    family = FAMILY_BY_KIND[row["choiceKind"]]
    family_rows = [value for value in vocabulary if FAMILY_BY_KIND[value["choiceKind"]] == family]
    if family == "initial_protection":
        mutation_class = construction(row).get("mutationClass")
        class_rows = [value for value in family_rows if construction(value).get("mutationClass") == mutation_class]
        available = sorted({construction(value).get("mutationClass") for value in family_rows})
        total_weight = sum(PROTECTION_WEIGHTS.get(value, 0) for value in available)
        weight = PROTECTION_WEIGHTS.get(mutation_class, 0)
        if not class_rows or not total_weight or not weight:
            raise SummaryError("protection vocabulary does not bind the sealed class policy")
        return {
            "family": family,
            "mutationClass": mutation_class,
            "formula": f"1/4 * {weight}/{total_weight} * 1/{len(class_rows)}",
            "numerator": weight,
            "denominator": 4 * total_weight * len(class_rows),
            "probability": weight / (4 * total_weight * len(class_rows)),
        }
    if not family_rows:
        raise SummaryError("selected family has no vocabulary")
    return {
        "family": family,
        "mutationClass": None,
        "formula": f"1/4 * 1/{len(family_rows)}",
        "numerator": 1,
        "denominator": 4 * len(family_rows),
        "probability": 1 / (4 * len(family_rows)),
    }


def controls(neighborhood: dict[str, Any], selector: dict[str, Any], crossover: dict[str, Any], grammar: dict[str, Any]) -> dict[str, Any]:
    envelope = accepted(neighborhood)
    aliases = static_alias_groups(envelope, "childProgramSha256")
    registry = grammar.get("executableRegistry", {})
    productions = registry.get("productions") if isinstance(registry, dict) else None
    checks = {
        "rawEqualsAdmittedPlusExcluded": neighborhood.get("rawPlanEnumeratedCount") == 2594
        and neighborhood.get("compiledChildAdmittedPlanCount") == 2381
        and neighborhood.get("excludedByCompiledChildAdmissionCount") == 213,
        "fullPairs": len(envelope) == 2381,
        "childPairIdentities": sum(isinstance(row.get("childPairIdentitySha256"), str) for row in envelope) == 2381,
        "unchangedOpposite": sum(row.get("oppositeSideUnchanged") is True for row in envelope) == 2381,
        "sourceProgramAliases": len(aliases) == 140 and sum(map(len, aliases)) == 280,
        "selectorRows": len(rows(selector, "selector")) == 4000,
        "selectorUniquePlans": len({row.get("planSha256") for row in rows(selector, "selector")}) == 1225,
        "selectorUniqueParentSidePrograms": len({(row.get("parentCandidateId"), row.get("side"), row.get("childProgramSha256")) for row in rows(selector, "selector")}) == 1196,
        "crossoverAccepted": len(accepted(crossover)) == 10,
        "grammarProductionCount": isinstance(productions, list) and len(productions) == 23,
    }
    if not all(checks.values()):
        raise SummaryError(f"V4 control reproduction failed: {checks}")
    return {
        "checks": checks,
        "supersession": [
            "The 242 initial-protection failures were audit-harness false rejections; profile-aware admission accepts all 242.",
            "Regex-derived grammar parameter values are not authority; the 23-production executable registry is authoritative.",
            "Family-name locality labels are superseded by deterministic exact-delta taxonomy rules in this report.",
        ],
    }


def exclusion_ledger(neighborhood: dict[str, Any]) -> dict[str, Any]:
    excluded = [row for row in rows(neighborhood, "neighborhood") if row.get("terminalStage") == "excluded_by_compiled_child_admission"]
    records = []
    for row in excluded:
        plan = construction(row)
        site = {key: plan.get(key) for key in ("site", "nodeId", "route", "planId", "operation") if plan.get(key) is not None}
        records.append({
            "parentCandidateId": row.get("parentCandidateId"), "parentPairIdentitySha256": row.get("parentPairIdentitySha256"),
            "parentRole": row.get("parentRole"), "side": row.get("side"), "operatorId": row.get("operatorId"),
            "choiceKind": row.get("choiceKind"), "constructionKind": row.get("constructionKind"), "siteRouteNode": site,
            "nativePlanSha256": row.get("nativePlanSha256"), "legacyChoiceSha256": row.get("legacyChoiceSha256"),
            "admissionStage": row.get("admissionStage"), "exactReasonCode": row.get("exactReasonCode"),
            "exactReasonDetail": row.get("exactReasonDetail"), "sharedAuthoritySha256": neighborhood.get("sharedAuthoritySha256"),
        })
    records.sort(key=canonical_bytes)
    if len(records) != 213 or any(not row["exactReasonCode"] or not row["exactReasonDetail"] for row in records):
        raise SummaryError("213 exclusions were not fully attributed")
    return {"recordCount": len(records), "byReasonDetail": counts(row["exactReasonDetail"] for row in records),
            "byConstruction": counts(row["constructionKind"] for row in records), "records": records}


def semantic_diversity(envelope: list[dict[str, Any]], selector: dict[str, Any], crossover: list[dict[str, Any]], historical: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    def level_counts(values: list[dict[str, Any]]) -> dict[str, int]:
        return {
            "rows": len(values), "nativePlans": len({row.get("nativePlanSha256", row.get("planSha256", row.get("crossoverPlanSha256"))) for row in values}),
            "legacyChoices": len({row.get("legacyChoiceSha256") for row in values if row.get("legacyChoiceSha256")}),
            "sidePrograms": len({row.get("childProgramSha256") for row in values if row.get("childProgramSha256")}),
            "pairIdentities": len({row.get("childPairIdentitySha256") for row in values if row.get("childPairIdentitySha256")}),
            "executableSemantics": len({row.get("executableSemanticSha256") for row in values if row.get("executableSemanticSha256")}),
            "compactStructuralPhenotypes": len({geometry_fingerprint(row) for row in values}),
        }
    index = {(row.get("parentCandidateId"), row.get("side"), row.get("nativePlanSha256")): row for row in envelope}
    selected: list[dict[str, Any]] = []
    for row in rows(selector, "selector"):
        match = index.get((row.get("parentCandidateId"), row.get("side"), row.get("planSha256")))
        if match is None:
            raise SummaryError("selector row cannot join to sealed semantic envelope")
        selected.append({**row, "executableSemanticSha256": match["executableSemanticSha256"], "normalizedProfileSha256": match["normalizedProfileSha256"], "deltaGeometry": match["deltaGeometry"]})
    historical_accepted = [row for row in historical if row.get("outcome", {}).get("disposition") == "accepted"]
    return ({
        "envelope": level_counts(envelope), "selector": level_counts(selected), "crossover": level_counts(crossover),
        "historicalV38": {"acceptedChildren": len(historical_accepted), "exactExecutableSemanticStatus": "unavailable_in_retained_compact_projection",
                         "reason": "retained V38 projection binds child program/profile hashes and outcomes, but not the pair raw-profile inputs used by the production executable-semantic contract."},
    }, selected)


def taxonomy(envelope: list[dict[str, Any]]) -> dict[str, Any]:
    classified: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in envelope:
        classified[locality_class(row)].append(row)
    output = {}
    for label, members in sorted(classified.items()):
        changed = [row.get("deltaGeometry", {}).get("changedPathCount", 0) for row in members]
        output[label] = {
            "planCount": len(members), "uniqueExecutableSemanticCount": len({row.get("executableSemanticSha256") for row in members}),
            "byConstructionKind": counts(row.get("constructionKind") for row in members),
            "byParentRole": counts(row.get("parentRole") for row in members), "bySide": counts(row.get("side") for row in members),
            "changedKeyedObjects": quantiles(changed),
            "inverseRouteStatus": "requires_bounded_current-vocabulary_probe",
        }
    return {"rulesVersion": "stage45e_locality_ordered_rules_v1", "classes": output}


def aliases_and_probabilities(envelope: list[dict[str, Any]], selected: list[dict[str, Any]]) -> dict[str, Any]:
    source_groups = static_alias_groups(envelope, "childProgramSha256")
    records = []
    for members in source_groups:
        vocabulary = [row for row in envelope if row.get("parentCandidateId") == members[0].get("parentCandidateId") and row.get("side") == members[0].get("side")]
        tickets = [exact_probability(row, vocabulary) for row in members]
        semantics = {row.get("executableSemanticSha256") for row in members}
        records.append({
            "parentCandidateId": members[0].get("parentCandidateId"), "side": members[0].get("side"),
            "childProgramSha256": members[0].get("childProgramSha256"), "memberCount": len(members),
            "planSha256s": sorted(row.get("nativePlanSha256") for row in members),
            "constructionKinds": sorted(row.get("constructionKind") for row in members),
            "creditLabels": sorted(row.get("choiceKind") for row in members),
            "executableSemanticSha256s": sorted(semantics), "sameExecutableSemantic": len(semantics) == 1,
            "separateSelectorTickets": True, "individualTickets": tickets,
            "combinedProbability": sum(ticket["probability"] for ticket in tickets),
            "relativeToComparableSingleton": sum(ticket["probability"] for ticket in tickets) / tickets[0]["probability"],
        })
    semantics = static_alias_groups(envelope, "executableSemanticSha256")
    selected_alias_draws = sum(1 for row in selected if any(row.get("planSha256") in record["planSha256s"] and row.get("parentCandidateId") == record["parentCandidateId"] and row.get("side") == record["side"] for record in records))
    return {"sourceProgramAliasGroupCount": len(records), "records": records,
            "executableSemanticAliasGroupCount": len(semantics), "selectorDrawsUsingKnownSourceAliases": selected_alias_draws,
            "interpretation": "Alias rows are independent production selector tickets. This report diagnoses the resulting probability concentration; it does not deduplicate or change selection."}


def entropy(values: list[str]) -> dict[str, float]:
    counts_ = collections.Counter(values)
    total = len(values)
    h = -sum((count / total) * math.log2(count / total) for count in counts_.values()) if total else 0.0
    return {"bits": h, "effectiveOutcomes": 2**h, "maxShare": max(counts_.values(), default=0) / total if total else 0.0}


def lottery(selected: list[dict[str, Any]], envelope: list[dict[str, Any]]) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    for family in sorted(set(FAMILY_BY_KIND.values())):
        draws = [row for row in selected if FAMILY_BY_KIND[row["choiceKind"]] == family]
        reachable = [row for row in envelope if FAMILY_BY_KIND[row["choiceKind"]] == family]
        unique_plan_order = []
        seen = set()
        for row in draws:
            key = row["planSha256"]
            if key not in seen:
                seen.add(key); unique_plan_order.append(len(seen))
        thresholds = {}
        for fraction in (0.25, 0.5, 0.75, 0.9):
            target = math.ceil(len({row["nativePlanSha256"] for row in reachable}) * fraction)
            thresholds[str(int(fraction * 100))] = next((i + 1 for i, value in enumerate(unique_plan_order) if value >= target), None)
        reports[family] = {
            "drawCount": len(draws), "uniquePlanCount": len({row["planSha256"] for row in draws}),
            "uniqueSideProgramCount": len({row["childProgramSha256"] for row in draws}),
            "uniqueExecutableSemanticCount": len({row["executableSemanticSha256"] for row in draws}),
            "repeatDrawRate": 1 - len({row["planSha256"] for row in draws}) / len(draws),
            "reachableEnvelopePlanCount": len({row["nativePlanSha256"] for row in reachable}),
            "envelopeCoverage": len({row["planSha256"] for row in draws}) / len({row["nativePlanSha256"] for row in reachable}),
            "firstSeenAccumulation": unique_plan_order, "drawsForReachablePlanPercent": thresholds,
            "planEntropy": entropy([row["planSha256"] for row in draws]),
            "byChoiceKind": counts(row["choiceKind"] for row in draws), "byMutationClass": counts(row.get("mutationClass") for row in draws),
        }
    return {"conditionalScope": "fixed parent/side schedule only; excludes parent selection, immigration, crossover, depth, ledger retries, and generation allocation", "families": reports}


def crossover_summary(crossover: list[dict[str, Any]], report: dict[str, Any]) -> dict[str, Any]:
    records = []
    for row in crossover:
        trace = topology_trace(row)
        same_topology = trace.get("beforeTopologySha256") == trace.get("afterTopologySha256")
        classification = "semantically_equivalent_hub_replacement" if same_topology else "genuine_semantic_motif_transfer"
        records.append({"recipientCandidateId": row.get("recipientCandidateId"), "donorCandidateId": row.get("donorCandidateId"), "side": row.get("side"),
                        "childExecutableSemanticSha256": row.get("executableSemanticSha256"), "recipientExecutableSemanticSha256": row.get("recipientExecutableSemanticSha256"),
                        "donorExecutableSemanticSha256": row.get("donorExecutableSemanticSha256"), "childDistinctFromRecipient": row.get("executableSemanticSha256") != row.get("recipientExecutableSemanticSha256"),
                        "childDistinctFromDonor": row.get("executableSemanticSha256") != row.get("donorExecutableSemanticSha256"),
                        "topologySemanticPreserved": same_topology, "replacements": trace.get("replacements"), "classification": classification})
    return {"compatiblePlans": len(records), "orderedRecipientDonorSidePairs": report.get("orderedRecipientDonorSidePairCount"),
            "incompatibleNoPortCount": report.get("orderedRecipientDonorSidePairCount", 0) - len(records), "records": records,
            "interpretation": "Topology identity is invariant to the paired hub identifiers, while the compiler's executable semantic includes the rebuilt module raw profiles. Compilation establishes neither runtime activation nor graft usefulness."}


def historical_join(historical: list[dict[str, Any]]) -> dict[str, Any]:
    matrix: dict[str, dict[str, int]] = {}
    for row in historical:
        family = row.get("matrixSlot", {}).get("operatorFamily")
        outcome = row.get("outcome", {}).get("identityLedgerEffect")
        if isinstance(family, str) and isinstance(outcome, str):
            matrix.setdefault(family, {})[outcome] = matrix.setdefault(family, {}).get(outcome, 0) + 1
    return {"recordCount": len(historical), "familyLedgerEffects": {key: dict(sorted(value.items())) for key, value in sorted(matrix.items())},
            "available": ["parent, role, side, family, terminal hashes, acceptance/duplicate outcome, compact child shape when accepted"],
            "unavailable": ["operator plan body/site, exact Stage45E delta class, executable semantic per historical child, runtime traces, market/economic results"],
            "scope": "observational retained-construction join only; no behavioral or economic causation claim"}


def build(args: argparse.Namespace) -> dict[str, Any]:
    neighborhood, selector, crossover, grammar = (read_object(path) for path in (args.neighborhood, args.selector, args.crossover, args.grammar))
    historical = read_jsonl(args.historical_v38_projection)
    hashes = {"neighborhood": check_report_hash(neighborhood, "neighborhood"), "selector": check_report_hash(selector, "selector"), "crossover": check_report_hash(crossover, "crossover")}
    envelope, cross = accepted(neighborhood), accepted(crossover)
    semantic, selected = semantic_diversity(envelope, selector, cross, historical)
    output = {"schemaVersion": SCHEMA, "scope": "static/compile-only evidence reducer; no market, runtime, generation, archive mutation, worker, gateway, or Vast activity",
              "inputs": {"neighborhood": artifact(args.neighborhood), "selector": artifact(args.selector), "crossover": artifact(args.crossover), "grammar": artifact(args.grammar), "historicalV38Projection": artifact(args.historical_v38_projection)},
              "verifiedInputReportHashes": hashes, "controls": controls(neighborhood, selector, crossover, grammar), "exclusions": exclusion_ledger(neighborhood),
              "semanticDiversity": semantic, "localityTaxonomy": taxonomy(envelope), "aliases": aliases_and_probabilities(envelope, selected),
              "conditionalLottery": lottery(selected, envelope), "crossover": crossover_summary(cross, crossover), "historicalJoin": historical_join(historical),
              "limitations": ["No runtime trace or market evaluation was read or executed.", "Historical V38 executable-semantic identities cannot be reconstructed from its retained compact projection without inventing missing raw-profile inputs."]}
    output["reportSha256"] = canonical_sha256(output)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("neighborhood", "selector", "crossover", "grammar", "historical-v38-projection", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    output = build(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(output) + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
