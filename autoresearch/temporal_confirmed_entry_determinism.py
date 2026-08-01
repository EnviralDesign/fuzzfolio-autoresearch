"""Determinism admission for the confirmed-entry structural corpus."""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_confirmed_entry_admission import (
    CONTINUATION_VERSION,
    _base_journal_row,
    _proposal,
    _read,
)
from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_validation import _normalize_preparation
from .temporal_operator_confirmed_entry import (
    ConfirmedEntryStructuralOperator,
    audit_confirmed_entry_application,
    inspect_confirmed_entry_applicability,
    preview_confirmed_entry_plan,
)
from .temporal_structural_operators import build_candidate_lineage

REPORT_SCHEMA = "temporal_confirmed_entry_determinism_admission_v1"
PROBE_SCHEMA = "temporal_confirmed_entry_determinism_probe_v1"


def _project_pairs(pairs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    operator = ConfirmedEntryStructuralOperator()
    rows = []
    for pair in pairs:
        control = pair["control"]
        transformed = pair["transformed"]
        plans = operator.enumerate_plans(control["sourceProfile"])
        plan = pair["operatorPlan"]
        if plan not in plans:
            raise TemporalDiscoveryContractError(
                f"stored plan is no longer applicable for {pair['pairId']}"
            )
        child = preview_confirmed_entry_plan(control["sourceProfile"], plan)
        if child != transformed["sourceProfile"]:
            raise TemporalDiscoveryContractError(
                f"transformed profile replay diverged for {pair['pairId']}"
            )
        child_again, application = operator.apply(
            control["sourceProfile"],
            plan,
            parent_validated_program_sha256=control["programSha256"],
            child_validated_program_sha256=transformed["programSha256"],
        )
        if child_again != child or application != pair["operatorApplication"]:
            raise TemporalDiscoveryContractError(
                f"operator application replay diverged for {pair['pairId']}"
            )
        audit = audit_confirmed_entry_application(
            control["sourceProfile"], child, application
        )
        if audit["allChecksPassed"] is not True:
            raise TemporalDiscoveryContractError(
                f"operator audit failed for {pair['pairId']}"
            )
        stored_lineage = pair["lineage"]
        lineage = build_candidate_lineage(
            candidate_id=transformed["candidateId"],
            candidate_source_profile_sha256=transformed["sourceProfileSha256"],
            candidate_validated_program_sha256=transformed["programSha256"],
            generation_index=stored_lineage["generationIndex"],
            birth_ordinal=stored_lineage["birthOrdinal"],
            parent_candidate_ids=stored_lineage["parentCandidateIds"],
            parent_program_sha256s=stored_lineage["parentValidatedProgramSha256s"],
            operator_id=stored_lineage["operatorId"],
            operator_version=stored_lineage["operatorVersion"],
            plan_sha256=plan["planSha256"],
            application_sha256=application["applicationSha256"],
        )
        if lineage != stored_lineage:
            raise TemporalDiscoveryContractError(
                f"lineage replay diverged for {pair['pairId']}"
            )
        pair_id = (
            "pair_" + application["applicationSha256"].removeprefix("sha256:")[:28]
        )
        if pair_id != pair["pairId"]:
            raise TemporalDiscoveryContractError("pair identity replay diverged")
        rows.append(
            {
                "pairId": pair_id,
                "controlSourceProfileSha256": control["sourceProfileSha256"],
                "controlProgramSha256": control["programSha256"],
                "transformedSourceProfileSha256": transformed["sourceProfileSha256"],
                "transformedProgramSha256": transformed["programSha256"],
                "planSha256": plan["planSha256"],
                "applicationSha256": application["applicationSha256"],
                "lineageSha256": lineage["lineageSha256"],
                "auditSha256": audit["auditSha256"],
            }
        )
    rows.sort(key=lambda item: item["pairId"])
    return {
        "pairCount": len(rows),
        "rowsSha256": canonical_sha256(rows),
        "pairIdsSha256": canonical_sha256([item["pairId"] for item in rows]),
        "planIdsSha256": canonical_sha256([item["planSha256"] for item in rows]),
        "applicationIdsSha256": canonical_sha256(
            [item["applicationSha256"] for item in rows]
        ),
        "lineageIdsSha256": canonical_sha256([item["lineageSha256"] for item in rows]),
    }


def _project_applicability(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for candidate in candidates:
        report = inspect_confirmed_entry_applicability(candidate["sourceProfile"])
        rows.append(
            {
                "candidateId": candidate["candidateId"],
                "programSha256": candidate["programSha256"],
                "applicable": report["applicable"],
                "planSha256s": report["planSha256s"],
                "issueCodes": report["issueCodes"],
                "reportSha256": report["reportSha256"],
            }
        )
    rows.sort(key=lambda item: item["candidateId"])
    return {
        "candidateCount": len(rows),
        "applicableCount": sum(1 for item in rows if item["applicable"]),
        "rowsSha256": canonical_sha256(rows),
    }


def _replay_stream(
    *,
    source_preparation: Mapping[str, Any],
    base_config: Mapping[str, Any],
    base_population: Mapping[str, Any],
    base_journal: Mapping[str, Any],
    admission_journal: Mapping[str, Any],
    pairs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    preparation = _normalize_preparation(source_preparation)
    parameters = _clone(base_config["parameters"], name="generator parameters")
    targets = dict(parameters["sourceModeCounts"])
    seeds = sorted(preparation["seeds"], key=lambda item: str(item["seedId"]))
    rng = random.Random(int(parameters["seed"]))
    mode_counts = {key: 0 for key in targets}
    prefix_rows = []
    for ordinal, expected in enumerate(base_journal["entries"]):
        proposal = _proposal(
            rng=rng,
            ordinal=ordinal,
            mode_counts=mode_counts,
            targets=targets,
            seeds=seeds,
            parameters=parameters,
            continuation=False,
        )
        core = _base_journal_row(proposal)
        expected_core = {key: expected[key] for key in core}
        if core != expected_core:
            raise TemporalDiscoveryContractError(
                f"determinism prefix diverged at proposal {ordinal}"
            )
        prefix_rows.append(core)
        if expected["disposition"] == "accepted":
            mode_counts[str(proposal["sourceMode"])] += 1
    if mode_counts != targets:
        raise TemporalDiscoveryContractError("determinism prefix allocation incomplete")

    continuation_rows = sorted(
        (row for row in admission_journal["entries"] if "continuationOrdinal" in row),
        key=lambda item: int(item["continuationOrdinal"]),
    )
    pair_by_proposal = {
        int(pair["control"]["proposalOrdinal"]): pair
        for pair in pairs
        if pair["cohort"] == "deterministic_continuation"
    }
    programs = {item["programSha256"] for item in base_population["candidates"]}
    child_programs = {
        pair["transformed"]["programSha256"]
        for pair in pairs
        if pair["cohort"] == "admitted_population_anchor"
    }
    accepted_pair_ids = []
    proposal_cores = []
    start = len(base_journal["entries"])
    for offset, expected in enumerate(continuation_rows):
        ordinal = start + offset
        proposal = _proposal(
            rng=rng,
            ordinal=ordinal,
            mode_counts=mode_counts,
            targets=targets,
            seeds=seeds,
            parameters=parameters,
            continuation=True,
        )
        core = {**_base_journal_row(proposal), "continuationOrdinal": offset}
        expected_core = {key: expected[key] for key in core}
        if core != expected_core:
            raise TemporalDiscoveryContractError(
                f"continuation stream diverged at proposal {ordinal}"
            )
        proposal_cores.append(core)
        disposition = expected["disposition"]
        parent_program = expected.get("parentProgramSha256")
        if disposition == "duplicate_parent_program":
            if parent_program not in programs:
                raise TemporalDiscoveryContractError("parent dedup replay diverged")
            continue
        if disposition in {
            "operator_inapplicable",
            "child_validator_rejected",
            "duplicate_child_program",
            "accepted_pair",
        }:
            if parent_program in programs:
                raise TemporalDiscoveryContractError("unique parent replay diverged")
            programs.add(str(parent_program))
            applicability = inspect_confirmed_entry_applicability(proposal["profile"])
            if (
                applicability["planCount"] != expected["planCount"]
                or applicability["issueCodes"] != expected["applicabilityIssueCodes"]
                or applicability["reportSha256"]
                != expected["applicabilityReportSha256"]
            ):
                raise TemporalDiscoveryContractError("applicability replay diverged")
        if disposition == "duplicate_child_program":
            child_program = expected["childProgramSha256"]
            if child_program not in child_programs:
                raise TemporalDiscoveryContractError("child dedup replay diverged")
        elif disposition == "accepted_pair":
            child_program = expected["childProgramSha256"]
            if child_program in child_programs:
                raise TemporalDiscoveryContractError("accepted child replay duplicated")
            child_programs.add(child_program)
            pair = pair_by_proposal.get(ordinal)
            if pair is None or pair["pairId"] != expected["pairId"]:
                raise TemporalDiscoveryContractError("accepted-pair ordering diverged")
            accepted_pair_ids.append(pair["pairId"])
    if len(accepted_pair_ids) != 94:
        raise TemporalDiscoveryContractError(
            "continuation accepted-pair count diverged"
        )
    return {
        "continuationVersion": CONTINUATION_VERSION,
        "basePrefixProposalCount": len(prefix_rows),
        "basePrefixRowsSha256": canonical_sha256(prefix_rows),
        "continuationProposalCount": len(proposal_cores),
        "continuationRowsSha256": canonical_sha256(proposal_cores),
        "acceptedPairIdsSha256": canonical_sha256(accepted_pair_ids),
        "rngStateSha256": canonical_sha256(rng.getstate()),
    }


def _probe(
    *,
    source_preparation_path: Path,
    base_generator_root: Path,
    admission_root: Path,
) -> dict[str, Any]:
    population = _read(
        admission_root / "paired-population.json", name="paired population"
    )
    base_population = _read(
        base_generator_root / "population.json", name="base population"
    )
    base_config = _read(base_generator_root / "config.json", name="base config")
    base_journal = _read(
        base_generator_root / "generation-journal.json", name="base journal"
    )
    admission_journal = _read(
        admission_root / "admission-journal.json", name="admission journal"
    )
    result = {
        "schemaVersion": PROBE_SCHEMA,
        "populationSha256": population["populationSha256"],
        "pairProjection": _project_pairs(population["pairs"]),
        "baseApplicability": _project_applicability(base_population["candidates"]),
        "proposalStream": _replay_stream(
            source_preparation=_read(
                source_preparation_path, name="source preparation"
            ),
            base_config=base_config,
            base_population=base_population,
            base_journal=base_journal,
            admission_journal=admission_journal,
            pairs=population["pairs"],
        ),
    }
    result["probeSha256"] = canonical_sha256(result)
    return result


def _native_witness_identity(root: Path) -> dict[str, str]:
    report = _read(root / "native-witnesses.json", name="native witnesses")
    manifest = _read(root / "manifest.json", name="native witness manifest")
    return {
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
    }


def _run_native_variant(
    *,
    population: Mapping[str, Any],
    pairs: Sequence[Mapping[str, Any]],
    fuzz_core_project: Path,
    witness_script: Path,
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    with tempfile.TemporaryDirectory(
        prefix="confirmed-entry-witness-determinism-"
    ) as raw:
        root = Path(raw)
        input_path = root / "population.json"
        payload = _clone(population, name="witness variant population")
        payload["pairs"] = _clone(list(pairs), name="witness variant pairs")
        input_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        output = root / "output"
        completed = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(fuzz_core_project),
                "python",
                str(witness_script),
                "--population",
                str(input_path),
                "--output-root",
                str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=dict(environment or os.environ),
        )
        if completed.returncode != 0:
            raise TemporalDiscoveryContractError(
                "native witness determinism variant failed: " + completed.stderr.strip()
            )
        return _native_witness_identity(output)


def build_determinism_admission(
    *,
    source_preparation_path: Path,
    base_generator_root: Path,
    admission_root: Path,
    native_witness_root: Path,
    fuzz_core_project: Path,
    witness_script: Path,
    output_path: Path,
) -> dict[str, Any]:
    baseline = _probe(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        admission_root=admission_root,
    )
    repeat = _probe(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        admission_root=admission_root,
    )
    if repeat != baseline:
        raise TemporalDiscoveryContractError("ordinary determinism repeat diverged")
    population = _read(
        admission_root / "paired-population.json", name="paired population"
    )
    base_population = _read(
        base_generator_root / "population.json", name="base population"
    )
    pairs = list(population["pairs"])
    candidates = list(base_population["candidates"])
    order_results = []
    order_variants: list[tuple[str, list[Any], list[Any]]] = [
        ("reversed", list(reversed(pairs)), list(reversed(candidates)))
    ]
    for seed in range(1, 6):
        pair_order = list(pairs)
        candidate_order = list(candidates)
        random.Random(seed).shuffle(pair_order)
        random.Random(seed).shuffle(candidate_order)
        order_variants.append((f"shuffle_{seed}", pair_order, candidate_order))
    for label, pair_order, candidate_order in order_variants:
        pair_projection = _project_pairs(pair_order)
        applicability = _project_applicability(candidate_order)
        exact = (
            pair_projection == baseline["pairProjection"]
            and applicability == baseline["baseApplicability"]
        )
        order_results.append({"variant": label, "exact": exact})
        if not exact:
            raise TemporalDiscoveryContractError(f"order variant diverged: {label}")

    hash_results = []
    for seed in ("1", "2", "3", "4", "5"):
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "autoresearch.temporal_confirmed_entry_determinism",
                "--probe",
                "--source-preparation",
                str(source_preparation_path),
                "--base-generator-root",
                str(base_generator_root),
                "--admission-root",
                str(admission_root),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        observed = json.loads(completed.stdout)
        exact = observed == baseline
        hash_results.append({"pythonHashSeed": int(seed), "exact": exact})
        if not exact:
            raise TemporalDiscoveryContractError(
                f"PYTHONHASHSEED={seed} determinism probe diverged"
            )

    native_baseline = _native_witness_identity(native_witness_root)
    native_order_results = []
    for label, pair_order, _ in order_variants:
        identity = _run_native_variant(
            population=population,
            pairs=pair_order,
            fuzz_core_project=fuzz_core_project,
            witness_script=witness_script,
        )
        exact = identity == native_baseline
        native_order_results.append({"variant": label, "exact": exact})
        if not exact:
            raise TemporalDiscoveryContractError(
                f"native witness order variant diverged: {label}"
            )
    native_hash_results = []
    for seed in ("1", "2", "3", "4", "5"):
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        identity = _run_native_variant(
            population=population,
            pairs=pairs,
            fuzz_core_project=fuzz_core_project,
            witness_script=witness_script,
            environment=environment,
        )
        exact = identity == native_baseline
        native_hash_results.append({"pythonHashSeed": int(seed), "exact": exact})
        if not exact:
            raise TemporalDiscoveryContractError(
                f"native witness PYTHONHASHSEED={seed} diverged"
            )
    report = {
        "schemaVersion": REPORT_SCHEMA,
        "populationSha256": population["populationSha256"],
        "baselineProbeSha256": baseline["probeSha256"],
        "repeatExact": True,
        "orderResults": order_results,
        "hashSeedResults": hash_results,
        "nativeWitnessBaseline": native_baseline,
        "nativeWitnessOrderResults": native_order_results,
        "nativeWitnessHashSeedResults": native_hash_results,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "allChecksPassed": True,
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(output_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--source-preparation", type=Path, required=True)
    parser.add_argument("--base-generator-root", type=Path, required=True)
    parser.add_argument("--admission-root", type=Path, required=True)
    parser.add_argument("--native-witness-root", type=Path)
    parser.add_argument("--fuzz-core-project", type=Path)
    parser.add_argument("--witness-script", type=Path)
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()
    if args.probe:
        print(
            json.dumps(
                _probe(
                    source_preparation_path=args.source_preparation,
                    base_generator_root=args.base_generator_root,
                    admission_root=args.admission_root,
                ),
                sort_keys=True,
            )
        )
        return
    required = {
        "native-witness-root": args.native_witness_root,
        "fuzz-core-project": args.fuzz_core_project,
        "witness-script": args.witness_script,
        "output-path": args.output_path,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error("required without --probe: " + ", ".join(missing))
    report = build_determinism_admission(
        source_preparation_path=args.source_preparation,
        base_generator_root=args.base_generator_root,
        admission_root=args.admission_root,
        native_witness_root=args.native_witness_root,
        fuzz_core_project=args.fuzz_core_project,
        witness_script=args.witness_script,
        output_path=args.output_path,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
