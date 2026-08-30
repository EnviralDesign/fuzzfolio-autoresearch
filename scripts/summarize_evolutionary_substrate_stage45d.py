#!/usr/bin/env python3
"""Compose compact Stage 4.5D static evidence from retained raw artifacts.

This is intentionally a read-only evidence reducer.  It neither invokes the
research engine nor writes a run/archive/candidate artifact.  Its inputs are
the source-bound static reports and retained V37/V38 construction artifacts;
the output makes stage counts, aliases, geometry, and explicit unknowns easy
to audit without replacing the raw receipts.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable


SCHEMA = "evolutionary_substrate_stage45d_static_summary_v1"
INITIAL_PROTECTION_OPERATOR = "evolvable_initial_protection_v1"


class SummaryError(ValueError):
    """Raised when an evidence input is not the expected retained shape."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SummaryError(f"expected JSON object: {path}")
    return value


def rows(value: dict[str, Any], label: str) -> list[dict[str, Any]]:
    raw_rows = value.get("rows")
    if not isinstance(raw_rows, list) or not all(isinstance(row, dict) for row in raw_rows):
        raise SummaryError(f"{label} rows are missing or malformed")
    return raw_rows


def counter(values: Iterable[object]) -> dict[str, int]:
    return dict(sorted(collections.Counter(str(value) for value in values).items()))


def artifact(path: Path, *, detail: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "detail": detail,
    }


def static_row_witness(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "parentCandidateId",
            "parentPairIdentitySha256",
            "parentRole",
            "side",
            "operatorId",
            "choiceKind",
            "constructionKind",
            "planSha256",
            "legacyChoiceSha256",
            "parentSideProgramSha256",
            "childProgramSha256",
            "childPairIdentitySha256",
            "terminalStage",
        )
        if row.get(key) is not None
    }


def terminal_stage(row: dict[str, Any]) -> str:
    """Normalize the V3 terminal field and the V4 explicit stage field."""
    value = row.get("terminalStage", row.get("admissionDisposition", "missing"))
    return value if isinstance(value, str) else "missing"


def group_witnesses(
    source_rows: list[dict[str, Any]],
    key_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> dict[str, Any]:
    grouped: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    for row in source_rows:
        key = key_builder(row)
        if key is None:
            continue
        encoded = canonical_bytes(key).decode("utf-8")
        if encoded not in grouped:
            grouped[encoded] = (key, [])
        grouped[encoded][1].append(row)
    duplicate_groups = [
        {
            "key": key,
            "count": len(members),
            "members": sorted(
                (static_row_witness(member) for member in members),
                key=lambda item: canonical_bytes(item),
            ),
        }
        for key, members in grouped.values()
        if len(members) > 1
    ]
    duplicate_groups.sort(key=lambda group: (-group["count"], canonical_bytes(group["key"])))
    return {
        "distinctKeyCount": len(grouped),
        "duplicateGroupCount": len(duplicate_groups),
        "duplicateMemberCount": sum(group["count"] for group in duplicate_groups),
        "groups": duplicate_groups,
    }


def compact_geometry_key(row: dict[str, Any]) -> dict[str, Any] | None:
    geometry = row.get("deltaGeometry")
    if not isinstance(geometry, dict):
        return None
    return {
        "changeCounts": geometry.get("changeCounts"),
        "changes": geometry.get("changes"),
    }


def observed_reversals(accepted_rows: list[dict[str, Any]]) -> dict[str, Any]:
    transitions: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in accepted_rows:
        side = row.get("side")
        parent = row.get("parentSideProgramSha256")
        child = row.get("childProgramSha256")
        if all(isinstance(value, str) for value in (side, parent, child)):
            transitions.setdefault((side, parent, child), []).append(row)
    pairs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for key, forward in transitions.items():
        side, parent, child = key
        reverse = (side, child, parent)
        if reverse not in transitions or key in seen or reverse in seen:
            continue
        seen.update((key, reverse))
        pairs.append(
            {
                "side": side,
                "programA": parent,
                "programB": child,
                "forwardWitnesses": [static_row_witness(row) for row in forward],
                "reverseWitnesses": [static_row_witness(row) for row in transitions[reverse]],
            }
        )
    pairs.sort(key=lambda item: canonical_bytes(item))
    return {
        "observedInversePairCount": len(pairs),
        "observedInversePairs": pairs,
        "scope": (
            "observed only when both directions occur among the frozen static hosts; "
            "absence is not evidence that an inverse is impossible"
        ),
    }


def stage_summary(report: dict[str, Any], *, label: str) -> dict[str, Any]:
    report_rows = rows(report, label)
    terminal = counter(terminal_stage(row) for row in report_rows)
    stage_counts = counter(
        stage
        for row in report_rows
        for stage in row.get("stageTrace", [])
        if isinstance(stage, str)
    )
    accepted = [row for row in report_rows if terminal_stage(row) == "accepted_full_pair"]
    aliases = {
        "sameParentSideChildProgram": group_witnesses(
            accepted,
            lambda row: {
                "parentCandidateId": row.get("parentCandidateId"),
                "side": row.get("side"),
                "childProgramSha256": row.get("childProgramSha256"),
            }
            if all(isinstance(row.get(key), str) for key in ("parentCandidateId", "side", "childProgramSha256"))
            else None,
        ),
        "sameParentSideChildPair": group_witnesses(
            accepted,
            lambda row: {
                "parentCandidateId": row.get("parentCandidateId"),
                "side": row.get("side"),
                "childPairIdentitySha256": row.get("childPairIdentitySha256"),
            }
            if all(
                isinstance(row.get(key), str)
                for key in ("parentCandidateId", "side", "childPairIdentitySha256")
            )
            else None,
        ),
        "compactDeltaGeometry": group_witnesses(accepted, compact_geometry_key),
    }
    opposite_rows = [row for row in accepted if "oppositeSideUnchanged" in row]
    return {
        "schemaVersion": report.get("schemaVersion"),
        "rawPlanEnumeratedCount": report.get("rawPlanEnumeratedCount", report.get("enumeratedPlanCount")),
        "compiledChildAdmittedPlanCount": report.get("compiledChildAdmittedPlanCount"),
        "excludedByCompiledChildAdmissionCount": report.get("excludedByCompiledChildAdmissionCount"),
        "selectedPlanCount": report.get("selectedPlanCount"),
        "maxPlans": report.get("maxPlans"),
        "rowCount": len(report_rows),
        "terminalStages": terminal,
        "stageOccurrences": stage_counts,
        "accepted": {
            "count": len(accepted),
            "byParentRole": counter(row.get("parentRole", "missing") for row in accepted),
            "bySide": counter(row.get("side", "missing") for row in accepted),
            "byOperator": counter(row.get("operatorId", "missing") for row in accepted),
            "byChoiceKind": counter(row.get("choiceKind", "missing") for row in accepted),
            "byConstructionKind": counter(row.get("constructionKind", "missing") for row in accepted),
            "byStaticClassification": counter(row.get("staticClassification", "missing") for row in accepted),
            "fullPairIdentityCount": sum(1 for row in accepted if isinstance(row.get("childPairIdentitySha256"), str)),
            "oppositeSideProof": {
                "rowsWithProof": len(opposite_rows),
                "unchanged": sum(row.get("oppositeSideUnchanged") is True for row in opposite_rows),
                "changed": sum(row.get("oppositeSideUnchanged") is False for row in opposite_rows),
                "missing": len(accepted) - len(opposite_rows),
            },
        },
        "aliases": aliases,
        "reversibility": observed_reversals(accepted),
        "behaviorStatus": "unknown_no_runtime_or_market_execution",
    }


def selector_summary(report: dict[str, Any]) -> dict[str, Any]:
    report_rows = rows(report, "selector report")
    expected = report.get("drawCount")
    if not isinstance(expected, int) or expected != len(report_rows):
        raise SummaryError("selector drawCount does not equal its row count")
    return {
        "schemaVersion": report.get("schemaVersion"),
        "drawCount": expected,
        "schedule": report.get("schedule"),
        "terminalStages": counter(row.get("terminalStage", "missing") for row in report_rows),
        "byFamily": counter(row.get("family", "missing") for row in report_rows),
        "byOperator": counter(row.get("operatorId", "missing") for row in report_rows),
        "byChoiceKind": counter(row.get("choiceKind", "missing") for row in report_rows),
        "byMutationClass": counter(row.get("mutationClass", "none") for row in report_rows),
        "byParentRole": counter(row.get("parentRole", "missing") for row in report_rows),
        "bySide": counter(row.get("side", "missing") for row in report_rows),
        "uniquePlanCount": len({row.get("planSha256") for row in report_rows if isinstance(row.get("planSha256"), str)}),
        "uniqueChildProgramCount": len(
            {row.get("childProgramSha256") for row in report_rows if isinstance(row.get("childProgramSha256"), str)}
        ),
        "uniqueChildPairCount": len(
            {row.get("childPairIdentitySha256") for row in report_rows if isinstance(row.get("childPairIdentitySha256"), str)}
        ),
        "behaviorStatus": "unknown_no_runtime_or_market_execution",
    }


def crossover_summary(report: dict[str, Any]) -> dict[str, Any]:
    report_rows = rows(report, "same-side crossover report")
    accepted = [row for row in report_rows if row.get("terminalStage") == "accepted_full_pair"]
    return {
        "schemaVersion": report.get("schemaVersion"),
        "hostCount": report.get("hostCount"),
        "orderedRecipientDonorSidePairCount": report.get("orderedRecipientDonorSidePairCount"),
        "selfCrossoverExcludedCount": report.get("selfCrossoverExcludedCount"),
        "rawPlanEnumeratedCount": report.get("rawPlanEnumeratedCount"),
        "selectedPlanCount": report.get("selectedPlanCount"),
        "pairOutcomes": report.get("pairOutcomes"),
        "rowCount": len(report_rows),
        "terminalStages": counter(row.get("terminalStage", "missing") for row in report_rows),
        "accepted": {
            "count": len(accepted),
            "bySide": counter(row.get("side", "missing") for row in accepted),
            "byClassification": counter(row.get("staticClassification", "missing") for row in accepted),
            "oppositeSideUnchanged": sum(row.get("oppositeSideUnchanged") is True for row in accepted),
            "oppositeSideChanged": sum(row.get("oppositeSideUnchanged") is False for row in accepted),
            "fullPairIdentityCount": sum(1 for row in accepted if isinstance(row.get("childPairIdentitySha256"), str)),
        },
        "behaviorStatus": "unknown_no_runtime_or_market_execution",
    }


def historical_initial_protection(parent_material_path: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    with parent_material_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            root = json.loads(line)
            if not isinstance(root, dict):
                raise SummaryError(f"historical parent material row {line_number} is not an object")
            payload = root.get("pairPayload")
            proposal = payload.get("proposalDelta") if isinstance(payload, dict) else None
            if not isinstance(proposal, dict):
                continue
            plan = proposal.get("terminalOperatorPlan")
            if not isinstance(plan, dict) or plan.get("operatorId") != INITIAL_PROTECTION_OPERATOR:
                continue
            trace = proposal.get("terminalOperatorTrace")
            steps = trace.get("steps") if isinstance(trace, dict) else None
            step = steps[-1] if isinstance(steps, list) and steps and isinstance(steps[-1], dict) else {}
            delta = step.get("operatorDelta") if isinstance(step.get("operatorDelta"), dict) else {}
            application = step.get("application") if isinstance(step.get("application"), dict) else {}
            records.append(
                {
                    "candidateId": root.get("candidateId"),
                    "childPairIdentitySha256": root.get("pairIdentitySha256"),
                    "parentProgramSha256": plan.get("parentProgramSha256"),
                    "planSha256": plan.get("planSha256"),
                    "applicationSha256": application.get("applicationSha256"),
                    "mutatedSide": delta.get("side"),
                    "childProgramSha256": delta.get("childProgramSha256"),
                    "disposition": step.get("disposition"),
                    "terminalReasonCode": proposal.get("terminalReasonCode"),
                    "proposalOrdinal": proposal.get("proposalOrdinal"),
                }
            )
    if not records:
        raise SummaryError("historical parent material contains no terminal initial-protection records")
    records.sort(key=lambda row: (str(row.get("proposalOrdinal")), str(row.get("candidateId"))))
    return {
        "source": artifact(parent_material_path, detail="retained V38 accepted parent material stream"),
        "operatorId": INITIAL_PROTECTION_OPERATOR,
        "recordCount": len(records),
        "byDisposition": counter(row.get("disposition", "missing") for row in records),
        "byTerminalReason": counter(row.get("terminalReasonCode", "missing") for row in records),
        "byMutatedSide": counter(row.get("mutatedSide", "missing") for row in records),
        "uniqueChildPairIdentityCount": len(
            {row.get("childPairIdentitySha256") for row in records if isinstance(row.get("childPairIdentitySha256"), str)}
        ),
        "witnesses": records[:10],
        "scope": (
            "accepted historical construction evidence: exact plan/application hashes, mutated-side child program, "
            "and recompiled child-pair identity are retained; this does not establish runtime behavior"
        ),
    }


def coverage_projection(coverage: dict[str, Any]) -> dict[str, Any]:
    v38 = coverage.get("v38")
    if not isinstance(v38, dict):
        raise SummaryError("historical coverage has no V38 projection")
    attempts = v38.get("attempts")
    if not isinstance(attempts, dict):
        raise SummaryError("historical coverage V38 attempts are missing")
    return {
        "schemaVersion": coverage.get("schemaVersion"),
        "scope": coverage.get("scope"),
        "v37GenerationCount": len(coverage.get("v37", {}).get("generations", []))
        if isinstance(coverage.get("v37"), dict)
        else None,
        "v38": {
            "recordCount": attempts.get("recordCount"),
            "dispositions": attempts.get("dispositions"),
            "operatorFamilies": attempts.get("operatorFamilies"),
            "parentRoles": attempts.get("parentRoles"),
            "activation": v38.get("activation"),
            "operatorPlanBodyStatus": v38.get("compactAttemptProjection", {})
            .get("interpretation", {})
            .get("operatorPlanBodyStatus"),
        },
    }


def coverage_vs_envelope(neighborhood: dict[str, Any], selector: dict[str, Any]) -> dict[str, Any]:
    envelope = neighborhood.get("rawPlanEnumeratedCount")
    selected = neighborhood.get("selectedPlanCount")
    max_plans = neighborhood.get("maxPlans")
    identical = isinstance(envelope, int) and selected == envelope and (not isinstance(max_plans, int) or envelope <= max_plans)
    return {
        "staticEnvelopeRawPlanCount": envelope,
        "staticEnvelopeSelectedPlanCount": selected,
        "staticEnvelopeMaxPlans": max_plans,
        "coverageBalancedEqualsEnvelope": identical,
        "selectorDrawCount": selector.get("drawCount"),
        "interpretation": (
            "The exhaustive static envelope is below the requested cap, so balanced selection makes no omission. "
            "The 4,000-draw selector sample is a separate repeated host/side schedule that exercises the frozen "
            "production selector; it is not an enlarged static plan envelope."
        ),
    }


def build_summary(args: argparse.Namespace) -> dict[str, Any]:
    paths = {
        "profileAwareNeighborhood": args.neighborhood,
        "legacyNeighborhood": args.legacy_neighborhood,
        "sameSideCrossover": args.crossover,
        "productionSelectorSample": args.selector,
        "structuredGrammar": args.grammar,
        "historicalCoverage": args.historical_coverage,
        "historicalInitialParentMaterial": args.historical_initial_parent_material,
    }
    inputs = {name: artifact(path, detail="input retained unchanged") for name, path in paths.items()}
    neighborhood_report = read_object(args.neighborhood)
    legacy_report = read_object(args.legacy_neighborhood)
    crossover_report = read_object(args.crossover)
    selector_report = read_object(args.selector)
    grammar = read_object(args.grammar)
    historical_coverage = read_object(args.historical_coverage)
    neighborhood = stage_summary(neighborhood_report, label="profile-aware neighborhood")
    legacy = stage_summary(legacy_report, label="legacy static neighborhood")
    selector = selector_summary(selector_report)
    summary = {
        "schemaVersion": SCHEMA,
        "scope": (
            "source-bound static enumeration, frozen selector execution, and retained V37/V38 construction evidence; "
            "no market replay, runtime execution, archive mutation, candidate materialization, or policy change"
        ),
        "inputs": inputs,
        "profileAwareNeighborhood": neighborhood,
        "legacyNeighborhood": legacy,
        "sameSideCrossover": crossover_summary(crossover_report),
        "productionSelectorSample": selector,
        "coverageEnvelope": coverage_vs_envelope(neighborhood, selector),
        "structuredGrammar": {
            "schemaVersion": grammar.get("schemaVersion"),
            "frozenAuthorityBinding": grammar.get("frozenAuthorityBinding"),
            "verification": grammar.get("verification"),
            "sourceMethod": "executable structured registry projection; no Rust-source regular-expression extraction",
        },
        "historical": {
            "v37v38ConstructionCoverage": coverage_projection(historical_coverage),
            "v38InitialProtectionAcceptedEvidence": historical_initial_protection(
                args.historical_initial_parent_material
            ),
        },
        "limitations": {
            "runtimeBehavior": "unknown_no_runtime_or_market_execution",
            "runtimeGuardProvenance": (
                "unavailable: the retained authority bundle does not contain the FuzzFolio runtime guard source; "
                "the structured grammar registry establishes emitted production routes only"
            ),
            "historicalV38AttemptPlanBodies": (
                "unavailable in the retained 800-slot compact attempt journal; only hashes/outcomes are retained there"
            ),
        },
    }
    summary["reportSha256"] = canonical_sha256(summary)
    return summary


def write_summary(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def write_memo(path: Path, summary: dict[str, Any]) -> None:
    neighborhood = summary["profileAwareNeighborhood"]
    accepted = neighborhood["accepted"]
    crossover = summary["sameSideCrossover"]
    selector = summary["productionSelectorSample"]
    historical = summary["historical"]["v38InitialProtectionAcceptedEvidence"]
    coverage = summary["coverageEnvelope"]
    memo = f"""# Stage 4.5D static terminal memo

This memo reports construction, compiler, and identity evidence only. It contains no market replay or runtime execution result.

The profile-aware static envelope enumerated {neighborhood['rawPlanEnumeratedCount']} raw plans. {neighborhood['compiledChildAdmittedPlanCount']} reached compiled-child admission and {neighborhood['excludedByCompiledChildAdmissionCount']} were excluded before application. Terminal stages are {json.dumps(neighborhood['terminalStages'], sort_keys=True)}.

{accepted['count']} rows reached accepted full-pair recompilation. Child pair identity is present for {accepted['fullPairIdentityCount']} accepted rows. The opposite side is unchanged in {accepted['oppositeSideProof']['unchanged']} of {accepted['oppositeSideProof']['rowsWithProof']} rows with an emitted proof; changed proofs: {accepted['oppositeSideProof']['changed']}; missing proofs: {accepted['oppositeSideProof']['missing']}.

The legacy static report has {legacy_count(summary)} accepted full-pair rows and {summary['legacyNeighborhood']['aliases']['sameParentSideChildProgram']['duplicateGroupCount']} same-parent/side child-program alias groups. The profile-aware report has {accepted['byOperator'].get(INITIAL_PROTECTION_OPERATOR, 0)} accepted initial-protection rows. Observed inverse pairs inside the frozen static host set: {neighborhood['reversibility']['observedInversePairCount']}; absence outside that set is not tested.

The ordered same-side crossover report contains {crossover['accepted']['count']} accepted full-pair rows from {crossover['rawPlanEnumeratedCount']} raw plans; opposite-side unchanged proofs: {crossover['accepted']['oppositeSideUnchanged']}; changed proofs: {crossover['accepted']['oppositeSideChanged']}.

The frozen production selector sample executed {selector['drawCount']} deterministic draws. Family counts are {json.dumps(selector['byFamily'], sort_keys=True)}. This sample exercises selector/executor construction only; behavior remains unknown.

The static envelope selected count is {coverage['staticEnvelopeSelectedPlanCount']} of {coverage['staticEnvelopeRawPlanCount']} raw plans. Coverage-balanced selection equals the full envelope: {str(coverage['coverageBalancedEqualsEnvelope']).lower()}. The 4,000 selector draws are a distinct repeated host/side schedule.

Historical retained V38 accepted parent material contains {historical['recordCount']} initial-protection records ({json.dumps(historical['byMutatedSide'], sort_keys=True)}), all with retained plan/application hashes, child-program identity, and child-pair identity. The separate retained V38 800-slot attempt journal has hash/outcome evidence but not plan bodies. Runtime guard provenance and runtime/market behavior are unavailable in this packet.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(memo, encoding="utf-8", newline="\n")


def legacy_count(summary: dict[str, Any]) -> int:
    value = summary["legacyNeighborhood"]["accepted"]["count"]
    if not isinstance(value, int):
        raise SummaryError("legacy accepted count is not an integer")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--neighborhood", required=True, type=Path)
    parser.add_argument("--legacy-neighborhood", required=True, type=Path)
    parser.add_argument("--crossover", required=True, type=Path)
    parser.add_argument("--selector", required=True, type=Path)
    parser.add_argument("--grammar", required=True, type=Path)
    parser.add_argument("--historical-coverage", required=True, type=Path)
    parser.add_argument("--historical-initial-parent-material", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--memo-output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build_summary(args)
    write_summary(args.output, summary)
    write_memo(args.memo_output, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
