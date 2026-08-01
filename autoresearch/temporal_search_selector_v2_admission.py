"""No-market synthetic admission harness for selector v2."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
from typing import Any, Mapping

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from .temporal_search_selector_v2 import (
    SELECTOR_V2_PARAMETERS,
    freeze_policy_v2_selection,
    select_policy_v2,
)


ADMISSION_SCHEMA = "temporal_discovery_selector_v2_synthetic_admission_v1"
ADMISSION_MANIFEST_SCHEMA = (
    "temporal_discovery_selector_v2_synthetic_admission_manifest_v1"
)


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    ) + "\n"


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encoded(value)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing divergent artifact: {path}")
    path.write_text(encoded, encoding="utf-8")


def _synthetic_aggregate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate["candidateId"])
    value = int(hashlib.sha256(candidate_id.encode("utf-8")).hexdigest(), 16)
    economic = (value % 10_000) / 1_000.0 - 5.0
    trades = [1 + (value % 9), 1 + ((value >> 8) % 9)]
    total_trades = sum(trades)
    return {
        "candidateId": candidate_id,
        "sourceMode": candidate["sourceMode"],
        "seedId": candidate["seedId"],
        "tradeCountsByWindow": trades,
        "totalTrades": total_trades,
        "totalConservativeNetR": economic,
        "worstWindowConservativeNetR": economic - 1.0,
        "maxWindowDrawdownR": 0.5 + ((value >> 16) % 500) / 100.0,
        "costDragR": total_trades * (0.01 + ((value >> 24) % 100) / 1_000.0),
        "managementActivationCount": (value >> 32) % 20,
        "rejectedIntentCount": (value >> 40) % 3,
        "entryFrequencyPerThousand": float(total_trades),
        "averageExposureRatio": ((value >> 48) % 100) / 100.0,
        "averageHoldingBars": 1 + ((value >> 56) % 50),
        "averageWinRate": ((value >> 64) % 100) / 100.0,
        "averageTransitionEntropy": ((value >> 72) % 300) / 100.0,
        "averageMfeR": ((value >> 80) % 300) / 100.0,
        "averageMaeR": ((value >> 88) % 300) / 100.0,
        "equityShape": [
            float((value >> (96 + index * 4)) % 16) / 15.0
            for index in range(12)
        ],
        "entryHourDistribution": {str((value >> 144) % 24): 1.0},
        "actionDistribution": {"synthetic_action": 1.0},
        "closeReasonDistribution": {"synthetic_close": 1.0},
        "stateOccupancyDistribution": {"synthetic_state": 1.0},
        "transitionDistribution": {"synthetic_transition": 1.0},
        "complexity": {
            "stateCount": 4 + (value >> 152) % 5,
            "transitionCount": 5 + (value >> 160) % 8,
            "indicatorCount": 1 + (value >> 168) % 4,
            "managementPlanCount": 1,
        },
    }


def _inputs(population_path: Path):
    population = _read(population_path)
    candidates = list(population.get("candidates") or [])
    if len(candidates) != 256:
        raise TemporalDiscoveryContractError(
            "selector v2 synthetic admission requires 256 candidates"
        )
    aggregates = [_synthetic_aggregate(item) for item in candidates]
    return population, candidates, aggregates


def _hash_probe(population_path: Path) -> str:
    _, candidates, aggregates = _inputs(population_path)
    return select_policy_v2(
        population_candidates=candidates,
        screening_aggregates=aggregates,
    )["selectionSha256"]


def build_synthetic_admission(
    *, population_path: Path | str, output_root: Path | str
) -> dict[str, Any]:
    source_path = Path(population_path)
    population, candidates, aggregates = _inputs(source_path)
    baseline = select_policy_v2(
        population_candidates=candidates,
        screening_aggregates=aggregates,
    )
    expected = baseline["selectionSha256"]
    variants = [
        ("reversed", list(reversed(candidates)), list(reversed(aggregates)))
    ]
    for seed in range(5):
        candidate_variant = list(candidates)
        aggregate_variant = list(aggregates)
        random.Random(seed).shuffle(candidate_variant)
        random.Random(seed + 10_000).shuffle(aggregate_variant)
        variants.append((f"shuffle_{seed}", candidate_variant, aggregate_variant))
    order_results = []
    for name, candidate_variant, aggregate_variant in variants:
        identity = select_policy_v2(
            population_candidates=candidate_variant,
            screening_aggregates=aggregate_variant,
        )["selectionSha256"]
        order_results.append(
            {"variant": name, "selectionSha256": identity, "exact": identity == expected}
        )
    hash_seed_results = []
    for seed in ("1", "2", "3", "4", "5"):
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "autoresearch.temporal_search_selector_v2_admission",
                "--hash-probe",
                str(source_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        identity = completed.stdout.strip()
        hash_seed_results.append(
            {
                "pythonHashSeed": int(seed),
                "selectionSha256": identity,
                "exact": identity == expected,
            }
        )
    if not all(item["exact"] for item in [*order_results, *hash_seed_results]):
        raise TemporalDiscoveryContractError("selector v2 determinism admission failed")

    root = Path(output_root)
    aggregate_artifact = {
        "schemaVersion": "temporal_discovery_selector_v2_synthetic_aggregates_v1",
        "populationSha256": population["populationSha256"],
        "candidateCount": len(candidates),
        "screeningWindowCount": 2,
        "generator": "candidate_id_sha256_only",
        "marketEvidenceRead": False,
        "aggregates": sorted(aggregates, key=lambda item: item["candidateId"]),
    }
    aggregate_artifact["aggregateSetSha256"] = canonical_sha256(aggregate_artifact)
    _write(root / "synthetic-aggregates.json", aggregate_artifact)
    frozen = freeze_policy_v2_selection(
        population_candidates=candidates,
        screening_aggregates=aggregates,
        output_root=root / "selection",
    )
    report = {
        "schemaVersion": ADMISSION_SCHEMA,
        "populationSha256": population["populationSha256"],
        "aggregateSetSha256": aggregate_artifact["aggregateSetSha256"],
        "selectorVersion": baseline["selectorVersion"],
        "selectorParameters": SELECTOR_V2_PARAMETERS,
        "selectionSha256": expected,
        "selectionManifestSha256": frozen["manifestSha256"],
        "activePopulationCount": baseline["activePopulationCount"],
        "eligibleCandidateCount": baseline["eligibleCandidateCount"],
        "economicArchiveCount": len(baseline["economicArchive"]),
        "admissibleNoveltyArchiveCount": len(
            baseline["admissibleNoveltyArchive"]
        ),
        "diagnosticPureNoveltyArchiveCount": len(
            baseline["diagnosticPureNoveltyArchive"]
        ),
        "selectedCandidateCount": baseline["selectedCandidateCount"],
        "stratifiedControlCount": baseline["stratifiedControlCount"],
        "confirmationCandidateCount": baseline["confirmationCandidateCount"],
        "thresholds": baseline["thresholds"],
        "orderDeterminism": order_results,
        "hashSeedDeterminism": hash_seed_results,
        "controlInputs": ["candidateId", "sourceMode", "seedId"],
        "controlProhibitedInputs": ["economics", "sourceProfile"],
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "allChecksPassed": True,
    }
    report["reportSha256"] = canonical_sha256(report)
    _write(root / "admission.json", report)
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json" and path.parent == root:
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
            }
        )
    manifest = {
        "schemaVersion": ADMISSION_MANIFEST_SCHEMA,
        "reportSha256": report["reportSha256"],
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write(root / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_discovery_selector_v2_synthetic_admission_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "selectionSha256": expected,
        "eligibleCandidateCount": report["eligibleCandidateCount"],
        "selectedCandidateCount": report["selectedCandidateCount"],
        "stratifiedControlCount": report["stratifiedControlCount"],
        "confirmationCandidateCount": report["confirmationCandidateCount"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hash-probe", type=Path)
    parser.add_argument("--population", type=Path)
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    if args.hash_probe is not None:
        print(_hash_probe(args.hash_probe))
        return
    if args.population is None or args.output_root is None:
        parser.error("--population and --output-root are required")
    print(
        json.dumps(
            build_synthetic_admission(
                population_path=args.population,
                output_root=args.output_root,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
