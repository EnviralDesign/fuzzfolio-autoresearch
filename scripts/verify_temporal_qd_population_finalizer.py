"""Prove native finalization against an existing Python-oracle population."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

from autoresearch.temporal_qd_pair_generation import _load_pair_proposal_entry
from autoresearch.temporal_qd_population_finalizer import (
    finalize_population_with_rust,
)


def _population_shell(path: Path) -> bytes:
    marker = b'"candidates":['
    with path.open("rb") as handle:
        first = handle.read(64 * 1024)
        marker_at = first.find(marker)
        if marker_at < 0:
            raise RuntimeError("population candidates marker is absent")
        array_start = marker_at + len(marker)
        prefix = first[:array_start]
        depth = 1
        in_string = False
        escaped = False
        scan = first[array_start:]
        while True:
            for index, byte in enumerate(scan):
                if in_string:
                    if escaped:
                        escaped = False
                    elif byte == 0x5C:
                        escaped = True
                    elif byte == 0x22:
                        in_string = False
                    continue
                if byte == 0x22:
                    in_string = True
                elif byte == 0x5B:
                    depth += 1
                elif byte == 0x5D:
                    depth -= 1
                    if depth == 0:
                        return prefix + b"]" + scan[index + 1 :] + handle.read()
            scan = handle.read(4 * 1024 * 1024)
            if not scan:
                raise RuntimeError("population candidate array is unterminated")


def _files_exact(left: Path, right: Path) -> bool:
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as first, right.open("rb") as second:
        while True:
            left_chunk = first.read(4 * 1024 * 1024)
            right_chunk = second.read(4 * 1024 * 1024)
            if left_chunk != right_chunk:
                return False
            if not left_chunk:
                return True


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    source = args.source_root.resolve()
    output = args.output_root.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("--output-root must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    source_population = source / "population.json"
    generation = json.loads(
        (source / "generation-journal.json").read_text(encoding="utf-8")
    )
    entry_shas = [str(value) for value in generation["entrySha256s"]]

    preparation_started = time.perf_counter()
    output_journal = output / "proposal-journal"
    output_journal.mkdir()
    references: list[dict] = []
    for ordinal, expected_sha in enumerate(entry_shas):
        source_entry = source / "proposal-journal" / f"{ordinal:08d}.json"
        output_entry = output_journal / source_entry.name
        os.link(source_entry, output_entry)
        entry = _load_pair_proposal_entry(source_entry, ordinal=ordinal)
        if entry["entrySha256"] != expected_sha:
            raise RuntimeError(f"generation journal mismatch at ordinal {ordinal}")
        if entry["disposition"] == "accepted":
            candidate = entry["candidate"]
            references.append(
                {
                    "proposalOrdinal": ordinal,
                    "candidateId": candidate["candidateId"],
                    "candidateIdentitySha256": candidate[
                        "candidateIdentitySha256"
                    ],
                }
            )
    shell = json.loads(_population_shell(source_population))
    expected_population_sha256 = shell.pop("populationSha256")
    shell["candidates"] = []
    preparation_seconds = time.perf_counter() - preparation_started

    finalizer_started = time.perf_counter()
    native = finalize_population_with_rust(
        output_root=output,
        population_without_sha=shell,
        expected_entry_sha256s=entry_shas,
        accepted_candidates=references,
    )
    finalizer_seconds = time.perf_counter() - finalizer_started
    output_population = output / "population.json"
    exact = _files_exact(source_population, output_population)
    output_file_sha256 = _sha256_file(output_population)
    report = {
        "schemaVersion": "temporal_qd_population_finalizer_parity_v1",
        "sourceRoot": str(source),
        "outputRoot": str(output),
        "proposalCount": len(entry_shas),
        "candidateCount": len(references),
        "preparationSeconds": preparation_seconds,
        "pythonInvocationWallSeconds": finalizer_seconds,
        "nativeResult": native,
        "expectedPopulationSha256": expected_population_sha256,
        "semanticHashExact": native["populationSha256"]
        == expected_population_sha256,
        "artifactBytesExact": exact,
        "oracleBytes": source_population.stat().st_size,
        "outputBytes": output_population.stat().st_size,
        "outputFileSha256": output_file_sha256,
    }
    if not report["semanticHashExact"] or not report["artifactBytesExact"]:
        raise RuntimeError(json.dumps(report, indent=2, sort_keys=True))
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
