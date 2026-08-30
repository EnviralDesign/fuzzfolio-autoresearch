#!/usr/bin/env python3
"""Project retained V37/V38 construction evidence without copying result packs.

The V38 proposal journal retains construction outcomes and hashes but not the
operator-plan bodies.  This exporter makes the matrix slot assignment explicit
from the retained contract, verifies it against the journal parent binding, and
keeps plan-specific facts unavailable where the raw records do not contain
them.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "evolutionary_substrate_historical_coverage_v3"
MATRIX_SCHEMA = "temporal_qd_operator_family_matrix_v1"
MATRIX_MODE = "frozen_parent_one_change_v1"


class CoverageError(ValueError):
    """Raised when retained construction artifacts do not match their contract."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CoverageError(f"expected an object in {path}")
    return value


def artifact(root: Path, path: Path, *, status: str, detail: str) -> dict[str, Any]:
    if not path.is_file():
        raise CoverageError(f"missing retained artifact: {path}")
    return {
        "relativePath": path.relative_to(root).as_posix(),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "status": status,
        "detail": detail,
    }


def counter(values: Iterable[str]) -> dict[str, int]:
    return dict(sorted(collections.Counter(values).items()))


def nested_guard_kinds(value: object) -> Iterable[str]:
    if not isinstance(value, dict):
        return []
    kind = value.get("kind")
    nested = value.get("guards")
    values: list[str] = [str(kind)] if isinstance(kind, str) else []
    if isinstance(nested, list):
        for child in nested:
            values.extend(nested_guard_kinds(child))
    return values


def profile_shape(profile: object) -> dict[str, Any] | None:
    if not isinstance(profile, dict):
        return None
    graph = profile.get("graph")
    if not isinstance(graph, dict):
        return None
    transitions = graph.get("transitions")
    transition_rows = transitions if isinstance(transitions, list) else []
    action_count = sum(
        len(row.get("actions", []))
        for row in transition_rows
        if isinstance(row, dict) and isinstance(row.get("actions", []), list)
    )
    return {
        "indicatorCount": len(profile.get("indicators", [])) if isinstance(profile.get("indicators"), list) else 0,
        "stateCount": len(graph.get("states", [])) if isinstance(graph.get("states"), list) else 0,
        "transitionCount": len(transition_rows),
        "eventBindingCount": len(graph.get("eventBindings", [])) if isinstance(graph.get("eventBindings"), list) else 0,
        "evidenceGroupCount": len(graph.get("evidenceGroups", [])) if isinstance(graph.get("evidenceGroups"), list) else 0,
        "actionCount": action_count,
        "guardKinds": counter(
            kind
            for row in transition_rows
            if isinstance(row, dict)
            for kind in nested_guard_kinds(row.get("guard"))
        ),
    }


def changed_shape_fields(parent: dict[str, Any] | None, child: dict[str, Any] | None) -> list[str]:
    if parent is None or child is None:
        return []
    return sorted(key for key in parent if parent.get(key) != child.get(key))


def candidate_summary(population: dict[str, Any]) -> dict[str, Any]:
    candidates = population.get("candidates")
    if not isinstance(candidates, list):
        raise CoverageError("evaluation population candidates are missing")
    operations: list[str] = []
    source_modes: list[str] = []
    seeds: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            raise CoverageError("evaluation population candidate is not an object")
        source_modes.append(str(candidate.get("sourceMode", "unavailable")))
        seeds.append(str(candidate.get("seedId", "unavailable")))
        history = candidate.get("structuralOperatorHistory")
        if isinstance(history, list):
            operations.extend(
                str(item["operation"])
                for item in history
                if isinstance(item, dict) and isinstance(item.get("operation"), str)
            )
    return {
        "candidateCount": len(candidates),
        "sourceModes": counter(source_modes),
        "seedIds": counter(seeds),
        "structuralHistoryOperations": counter(operations),
    }


def archive_summary(root: Path, path: Path) -> dict[str, Any]:
    value = read_json(path)
    return {
        **artifact(root, path, status="observed", detail="retained native-finalization archive"),
        "generationIndex": value.get("generationIndex"),
        "candidateCountSeen": value.get("candidateCountSeen"),
        "candidateCountReducedThisGeneration": value.get("candidateCountReducedThisGeneration"),
        "memberCount": value.get("memberCount"),
        "occupiedCellCount": value.get("occupiedCellCount"),
        "paretoAdmissionCount": value.get("paretoAdmissionCount"),
        "paretoEvictionCount": value.get("paretoEvictionCount"),
        "archiveSha256": value.get("archiveSha256"),
    }


def v37_generation_coverage(root: Path, generation: int) -> dict[str, Any]:
    generation_root = root / "generations" / f"generation-{generation:04d}"
    proposal = generation_root / "proposal"
    evaluation_path = proposal / "evaluation-population.json"
    ledger_path = proposal / "identity-ledger.json"
    parent_material_path = proposal / "parent-material.jsonl"
    archive_path = generation_root / "native-finalization" / "archive.json"
    attempts_path = proposal / "proposal-attempts.jsonl"
    evaluation = read_json(evaluation_path)
    return {
        "generationIndex": generation,
        "population": {
            **artifact(root, evaluation_path, status="observed", detail="retained evaluation population"),
            **candidate_summary(evaluation),
        },
        "identityLedger": artifact(root, ledger_path, status="observed", detail="retained proposal identity ledger"),
        "parentMaterial": artifact(root, parent_material_path, status="observed", detail="retained parent material stream"),
        "operatorAttempt": {
            "status": "observed" if attempts_path.is_file() else "unavailable",
            "reason": None
            if attempts_path.is_file()
            else "V37 retained artifact has no per-attempt proposal journal",
        },
        "activation": {
            "status": "unavailable",
            "reason": "this construction audit does not read market/runtime traces",
        },
        "archive": archive_summary(root, archive_path),
    }


def matrix_slot(matrix: dict[str, Any], ordinal: int) -> dict[str, Any]:
    if matrix.get("schemaVersion") != MATRIX_SCHEMA or matrix.get("mode") != MATRIX_MODE:
        raise CoverageError("V38 operator-family matrix schema/mode is incompatible")
    if matrix.get("includeCrossover") is not False or matrix.get("mutationDepth") != 1:
        raise CoverageError("V38 matrix does not represent frozen one-step non-crossover slots")
    children = matrix.get("childrenPerFamily")
    families = matrix.get("families")
    parents = matrix.get("parents")
    if not isinstance(children, int) or children <= 0 or not isinstance(families, list) or not isinstance(parents, list):
        raise CoverageError("V38 matrix dimensions are invalid")
    total = len(parents) * len(families) * children
    if ordinal < 0 or ordinal >= total:
        raise CoverageError(f"proposal ordinal {ordinal} lies outside the V38 matrix")
    per_parent = len(families) * children
    parent = parents[ordinal // per_parent]
    if not isinstance(parent, dict):
        raise CoverageError("V38 matrix parent is invalid")
    parent_offset = ordinal % per_parent
    family = families[parent_offset // children]
    if not isinstance(family, str) or not isinstance(parent.get("candidateId"), str) or not isinstance(parent.get("role"), str):
        raise CoverageError("V38 matrix slot fields are invalid")
    return {
        "proposalOrdinal": ordinal,
        "parentCandidateId": parent["candidateId"],
        "parentRole": parent["role"],
        "operatorFamily": family,
        "childIndex": parent_offset % children,
    }


def terminal_mutation_history(history: object) -> list[dict[str, Any]]:
    if not isinstance(history, list):
        return []
    fields = (
        "operation",
        "side",
        "proposalSeed",
        "parentCandidateIdentitySha256",
        "semanticTopologySha256",
        "operatorTraceSha256",
        "terminalOperatorApplicationSha256",
        "terminalOperatorPlanSha256",
    )
    return [
        {field: item.get(field) for field in fields if item.get(field) is not None}
        for item in history
        if isinstance(item, dict) and item.get("operation") == "evolvable_module_mutation"
    ]


def accepted_candidate_projection(candidate: dict[str, Any], parent_shape: dict[str, Any] | None) -> dict[str, Any]:
    shape = profile_shape(candidate.get("sourceProfile"))
    changed = changed_shape_fields(parent_shape, shape)
    return {
        "candidateId": candidate.get("candidateId"),
        "candidateIdentitySha256": candidate.get("candidateIdentitySha256"),
        "programSha256": candidate.get("programSha256"),
        "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
        "sourceProfileSha256": candidate.get("sourceProfileSha256"),
        "terminalMutationHistory": terminal_mutation_history(candidate.get("structuralOperatorHistory")),
        "compactStaticShape": shape,
        "changedCompactShapeFieldsFromParent": changed,
        "staticClassification": (
            "shape_preserving_or_parameter_only_change"
            if not changed
            else "small_local_change"
            if len(changed) <= 2
            else "coherent_single_region_change"
            if len(changed) <= 4
            else "large_multi_system_blast_radius"
        ),
        "classificationScope": "compact_profile_projection",
    }


def v38_projection(v37_root: Path, v38_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    generation_root = v38_root / "generations" / "generation-0003"
    proposal = generation_root / "proposal"
    config_path = v38_root / "config.json"
    attempts_path = proposal / "proposal-attempts.jsonl"
    evaluation_path = proposal / "evaluation-population.json"
    ledger_path = proposal / "identity-ledger.json"
    parent_material_path = proposal / "parent-material.jsonl"
    archive_path = generation_root / "native-finalization" / "archive.json"
    config = read_json(config_path)
    matrix = config.get("operatorFamilyMatrix")
    if not isinstance(matrix, dict):
        raise CoverageError("V38 config has no operatorFamilyMatrix")
    slots = len(matrix.get("parents", [])) * len(matrix.get("families", [])) * int(matrix.get("childrenPerFamily", 0))
    if matrix.get("constructionSlotCount") != slots:
        raise CoverageError("V38 constructionSlotCount does not match matrix dimensions")
    v37_population = read_json(v37_root / "generations" / "generation-0002" / "proposal" / "evaluation-population.json")
    parent_shapes = {
        str(candidate.get("candidateId")): profile_shape(candidate.get("sourceProfile"))
        for candidate in v37_population.get("candidates", [])
        if isinstance(candidate, dict)
    }
    evaluation = read_json(evaluation_path)
    accepted_by_ordinal = {
        int(candidate["proposalOrdinal"]): candidate
        for candidate in evaluation.get("candidates", [])
        if isinstance(candidate, dict) and isinstance(candidate.get("proposalOrdinal"), int)
    }
    rows: list[dict[str, Any]] = []
    with attempts_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            attempt = json.loads(line)
            if not isinstance(attempt, dict) or not isinstance(attempt.get("proposalOrdinal"), int):
                raise CoverageError("V38 attempt lacks proposalOrdinal")
            ordinal = attempt["proposalOrdinal"]
            slot = matrix_slot(matrix, ordinal)
            lineage = attempt.get("lineageRefs")
            parent = lineage.get("parent") if isinstance(lineage, dict) else None
            actual_parent = parent.get("candidateId") if isinstance(parent, dict) else None
            if actual_parent != slot["parentCandidateId"]:
                raise CoverageError(f"V38 attempt {ordinal} parent does not match its matrix slot")
            accepted = accepted_by_ordinal.get(ordinal)
            ledger_effect = attempt.get("identityLedgerEffect")
            row: dict[str, Any] = {
                "schemaVersion": "evolutionary_substrate_v38_attempt_projection_v3",
                "proposalOrdinal": ordinal,
                "matrixSlot": slot,
                "attempt": {
                    key: attempt.get(key)
                    for key in (
                        "attemptSha256",
                        "proposalSeed",
                        "operatorApplicationSha256",
                        "operatorPlanSha256",
                        "operatorTraceSha256",
                        "outcomeAuditSha256",
                        "proposalDeltaSha256",
                        "acceptedRecordSha256",
                    )
                },
                "outcome": {
                    "disposition": attempt.get("disposition"),
                    "reasonCode": attempt.get("reasonCode"),
                    "identityLedgerEffect": ledger_effect,
                },
                "operatorPlanBodyStatus": "unavailable",
                "behaviorStatus": "unknown",
            }
            if accepted is not None:
                child_projection = accepted_candidate_projection(
                    accepted, parent_shapes.get(slot["parentCandidateId"])
                )
                row["acceptedChild"] = child_projection
                row["staticClassification"] = child_projection["staticClassification"]
                row["classificationScope"] = child_projection["classificationScope"]
            elif ledger_effect == "duplicate_executable":
                row["staticClassification"] = "duplicate_resolved_program"
            else:
                row["staticClassification"] = "invalid_rejected"
                row["classificationScope"] = "rejected_outcome_with_unavailable_plan_body"
            rows.append(row)
    if len(rows) != slots:
        raise CoverageError(f"V38 attempt journal has {len(rows)} rows, expected {slots}")
    expected_ordinals = list(range(slots))
    if [row["proposalOrdinal"] for row in rows] != expected_ordinals:
        raise CoverageError("V38 attempt journal is not an ordered complete matrix slot grid")
    summary = {
        "matrix": {
            "schemaVersion": matrix.get("schemaVersion"),
            "mode": matrix.get("mode"),
            "childrenPerFamily": matrix.get("childrenPerFamily"),
            "families": matrix.get("families"),
            "parents": matrix.get("parents"),
            "constructionSlotCount": slots,
            "slotAssignment": "ordinal -> parent block -> family block -> child index; verified against every retained lineage parent",
        },
        "attempts": {
            **artifact(v38_root, attempts_path, status="observed", detail="retained V38 proposal attempt journal"),
            "recordCount": len(rows),
            "dispositions": counter(str(row["outcome"]["disposition"]) for row in rows),
            "identityLedgerEffects": counter(str(row["outcome"]["identityLedgerEffect"]) for row in rows),
            "operatorFamilies": counter(str(row["matrixSlot"]["operatorFamily"]) for row in rows),
            "parentRoles": counter(str(row["matrixSlot"]["parentRole"]) for row in rows),
        },
        "population": {
            **artifact(v38_root, evaluation_path, status="observed", detail="retained V38 evaluation population"),
            **candidate_summary(evaluation),
        },
        "identityLedger": artifact(v38_root, ledger_path, status="observed", detail="retained proposal identity ledger"),
        "parentMaterial": artifact(v38_root, parent_material_path, status="observed", detail="retained parent material stream"),
        "archive": archive_summary(v38_root, archive_path),
        "activation": {
            "status": "unavailable",
            "reason": "this construction audit does not read market/runtime traces",
        },
    }
    return summary, rows


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    with path.open("wb") as handle:
        for row in rows:
            encoded = canonical_bytes(row) + b"\n"
            digest.update(encoded)
            handle.write(encoded)
    return "sha256:" + digest.hexdigest()


def generate(args: argparse.Namespace) -> dict[str, Any]:
    v38_summary, rows = v38_projection(args.v37_run_root, args.v38_run_root)
    args.projection_output.parent.mkdir(parents=True, exist_ok=True)
    projection_sha = write_jsonl(args.projection_output, rows)
    value = {
        "schemaVersion": SCHEMA,
        "scope": "retained V37/V38 structural construction artifacts only; no market data or result packs copied",
        "v37": {
            "generations": [v37_generation_coverage(args.v37_run_root, generation) for generation in range(1, 6)],
            "attemptCoverage": "unavailable: no per-attempt proposal journal retained for V37 generations 1-5",
        },
        "v38": {
            **v38_summary,
            "compactAttemptProjection": {
                "path": args.projection_output.name,
                "sha256": projection_sha,
                "recordCount": len(rows),
                "content": "matrix slot, retained hashes/outcome, compact compiled child shape when accepted, and explicit unknowns",
                "interpretation": {
                    "operatorPlanBodyStatus": "retained attempt rows carry plan/application/trace hashes where available, not the plan body",
                    "behaviorStatus": "unknown because this audit does not read result packs or runtime traces",
                    "compact_profile_projection": "shape-only comparison; it cannot establish exact plan site, static liveness, or behavior",
                },
            },
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_bytes(value) + b"\n")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v37-run-root", required=True, type=Path)
    parser.add_argument("--v38-run-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--projection-output", required=True, type=Path)
    generate(parser.parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
