"""Deterministic replay admission for the native generator-v2 result ledger."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Mapping

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from .temporal_search_policy_v2 import generate_policy_v2_population


ADMISSION_SCHEMA = "temporal_discovery_generator_v2_determinism_admission_v1"


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


class _LedgerValidator:
    def __init__(self, journal: Mapping[str, Any]) -> None:
        self.rows = {}
        for row in journal.get("entries") or []:
            if row.get("candidateAcceptable") is None:
                continue
            key = str(row["rawSourceProfileSha256"])
            value = {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "candidateAcceptable": bool(row["candidateAcceptable"]),
                "status": row.get("validationStatus"),
                "validationReportSha256": row.get("validationReportSha256"),
                "profileSnapshotSha256": row.get("profileSnapshotSha256"),
                "programSha256": row.get("validatedProgramSha256"),
                "issues": [{"code": code} for code in row.get("issueCodes") or []],
            }
            prior = self.rows.get(key)
            if prior is not None and prior != value:
                raise TemporalDiscoveryContractError(
                    "native validation ledger is inconsistent for one raw profile"
                )
            self.rows[key] = value

    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]:
        if canonical_sha256(source_profile) != expected_raw_source_profile_sha256:
            raise TemporalDiscoveryContractError("replay validation raw identity mismatch")
        try:
            value = dict(self.rows[expected_raw_source_profile_sha256])
        except KeyError as exc:
            raise TemporalDiscoveryContractError(
                "generator proposed a profile absent from the native validation ledger"
            ) from exc
        value["candidateId"] = candidate_id
        return value


def _probe(
    *,
    source_preparation: Path,
    causality_root: Path,
    native_generator_root: Path,
) -> dict[str, Any]:
    journal = _read(native_generator_root / "generation-journal.json")
    parameters = _read(native_generator_root / "config.json")["parameters"]
    with tempfile.TemporaryDirectory(prefix="temporal-generator-v2-replay-") as temporary:
        return generate_policy_v2_population(
            _read(source_preparation),
            validator=_LedgerValidator(journal),
            causality_root=causality_root,
            output_root=temporary,
            parameters=parameters,
        )


def build_determinism_admission(
    *,
    source_preparation: Path | str,
    causality_root: Path | str,
    native_generator_root: Path | str,
    output_path: Path | str,
) -> dict[str, Any]:
    source = Path(source_preparation)
    causality = Path(causality_root)
    native_root = Path(native_generator_root)
    native_population = _read(native_root / "population.json")
    native_journal = _read(native_root / "generation-journal.json")
    baseline = {
        "configSha256": _read(native_root / "config.json")["configSha256"],
        "populationSha256": native_population["populationSha256"],
        "journalSha256": native_journal["journalSha256"],
        "candidateCount": native_population["candidateCount"],
        "proposalCount": native_journal["proposalCount"],
    }
    repeat = _probe(
        source_preparation=source,
        causality_root=causality,
        native_generator_root=native_root,
    )
    repeat_identity = {key: repeat[key] for key in baseline if key in repeat}
    repeat_exact = repeat_identity == baseline
    hash_results = []
    for seed in ("1", "2", "3", "4", "5"):
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = seed
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "autoresearch.temporal_search_generator_v2_admission",
                "--probe",
                "--source-preparation",
                str(source),
                "--causality-root",
                str(causality),
                "--native-generator-root",
                str(native_root),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        observed = json.loads(completed.stdout)
        identity = {key: observed[key] for key in baseline if key in observed}
        hash_results.append(
            {"pythonHashSeed": int(seed), "identity": identity, "exact": identity == baseline}
        )
    if not repeat_exact or not all(item["exact"] for item in hash_results):
        raise TemporalDiscoveryContractError("generator v2 determinism admission failed")
    report = {
        "schemaVersion": ADMISSION_SCHEMA,
        "nativeBaseline": baseline,
        "repeatIdentity": repeat_identity,
        "repeatExact": repeat_exact,
        "hashSeedResults": hash_results,
        "nativeValidatorReplayLedgerSha256": canonical_sha256(native_journal["entries"]),
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "allChecksPassed": True,
    }
    report["reportSha256"] = canonical_sha256(report)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(
            "refusing divergent generator determinism admission"
        )
    path.write_text(encoded, encoding="utf-8")
    return {
        "schemaVersion": "temporal_discovery_generator_v2_determinism_result_v1",
        "reportSha256": report["reportSha256"],
        "populationSha256": baseline["populationSha256"],
        "journalSha256": baseline["journalSha256"],
        "repeatExact": True,
        "hashSeedCount": len(hash_results),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--source-preparation", type=Path, required=True)
    parser.add_argument("--causality-root", type=Path, required=True)
    parser.add_argument("--native-generator-root", type=Path, required=True)
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()
    if args.probe:
        print(
            json.dumps(
                _probe(
                    source_preparation=args.source_preparation,
                    causality_root=args.causality_root,
                    native_generator_root=args.native_generator_root,
                ),
                sort_keys=True,
            )
        )
        return
    if args.output_path is None:
        parser.error("--output-path is required")
    print(
        json.dumps(
            build_determinism_admission(
                source_preparation=args.source_preparation,
                causality_root=args.causality_root,
                native_generator_root=args.native_generator_root,
                output_path=args.output_path,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
