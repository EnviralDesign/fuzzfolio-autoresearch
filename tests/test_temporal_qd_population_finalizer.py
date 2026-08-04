from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from autoresearch.temporal_bidirectional_genome import canonical_json, canonical_sha256
from autoresearch.temporal_qd_population_finalizer import (
    ensure_rust_population_finalizer,
    finalize_population_with_rust,
)


def _candidate(candidate_id: str, marker: str) -> dict:
    identity = canonical_sha256(
        {"candidateId": candidate_id, "marker": marker, "unicode": "λ/雪"}
    )
    return {
        "candidateId": candidate_id,
        "candidateIdentitySha256": identity,
        "escaped": "slash/quote\"/control\n",
        "finiteNumbers": [-0.0, 0, 1, 1.25, 1e-07],
        "marker": marker,
        "nested": {"enabled": True, "nothing": None, "unicode": "λ/雪"},
    }


def _write_entry(
    root: Path,
    *,
    ordinal: int,
    config_sha256: str,
    candidate: dict | None,
) -> str:
    material = {
        **({"candidate": candidate} if candidate is not None else {}),
        "configSha256": config_sha256,
        "disposition": "accepted" if candidate is not None else "rejected",
        "generationIndex": 3,
        "originKind": "random_immigrant",
        "proposalOrdinal": ordinal,
        "schemaVersion": "temporal_qd_proposal_entry_v3",
    }
    entry_sha256 = canonical_sha256(material)
    material["entrySha256"] = entry_sha256
    path = root / "proposal-journal" / f"{ordinal:08d}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((canonical_json(material) + os.linesep).encode("utf-8"))
    return entry_sha256


def _fixture(root: Path, *, duplicate_id: bool = False) -> tuple[dict, list[str], list[dict], bytes]:
    config_sha256 = canonical_sha256({"fixture": "native-finalizer"})
    first = _candidate("qd_z_candidate", "zulu")
    second = _candidate(
        "qd_z_candidate" if duplicate_id else "qd_a_candidate",
        "alpha",
    )
    rejected_sha = _write_entry(
        root,
        ordinal=0,
        config_sha256=config_sha256,
        candidate=None,
    )
    first_sha = _write_entry(
        root,
        ordinal=1,
        config_sha256=config_sha256,
        candidate=first,
    )
    second_sha = _write_entry(
        root,
        ordinal=2,
        config_sha256=config_sha256,
        candidate=second,
    )
    shell = {
        "candidateCount": 2,
        "candidates": [],
        "configSha256": config_sha256,
        "generationIndex": 3,
        "originCounts": {"random_immigrant": 2},
        "policySha256": canonical_sha256({"policy": "fixture"}),
        "proposalAttempts": 3,
        "schemaVersion": "temporal_qd_generation_population_v3",
    }
    references = [
        {
            "proposalOrdinal": 1,
            "candidateId": first["candidateId"],
            "candidateIdentitySha256": first["candidateIdentitySha256"],
        },
        {
            "proposalOrdinal": 2,
            "candidateId": second["candidateId"],
            "candidateIdentitySha256": second["candidateIdentitySha256"],
        },
    ]
    expected_population = dict(shell)
    expected_population["candidates"] = sorted(
        (first, second), key=lambda candidate: candidate["candidateId"]
    )
    expected_population["populationSha256"] = canonical_sha256(expected_population)
    expected_bytes = (
        canonical_json(expected_population) + os.linesep
    ).encode("utf-8")
    return shell, [rejected_sha, first_sha, second_sha], references, expected_bytes


def _invoke_manifest(root: Path) -> subprocess.CompletedProcess[str]:
    binary, _authority = ensure_rust_population_finalizer()
    manifest = root / "performance" / "population-finalizer" / "manifest.json"
    return subprocess.run(
        [str(binary), "--manifest", str(manifest)],
        text=True,
        capture_output=True,
        check=False,
    )


def test_rust_finalizer_matches_python_bytes_and_is_restart_safe(tmp_path: Path) -> None:
    shell, entry_shas, references, expected_bytes = _fixture(tmp_path)
    first = finalize_population_with_rust(
        output_root=tmp_path,
        population_without_sha=shell,
        expected_entry_sha256s=entry_shas,
        accepted_candidates=references,
    )
    assert first["existingArtifactVerified"] is False
    assert first["populationSha256"] == json.loads(expected_bytes)["populationSha256"]
    assert (tmp_path / "population.json").read_bytes() == expected_bytes

    stale = tmp_path / "population.json.temporal-finalizer-interrupted.tmp"
    stale.write_bytes(b"partial population")
    second = finalize_population_with_rust(
        output_root=tmp_path,
        population_without_sha=shell,
        expected_entry_sha256s=entry_shas,
        accepted_candidates=references,
    )
    assert second["existingArtifactVerified"] is True
    assert (tmp_path / "population.json").read_bytes() == expected_bytes
    assert not stale.exists()


@pytest.mark.parametrize("corruption", ["tamper", "truncate", "missing", "extra", "nan"])
def test_rust_finalizer_rejects_journal_corruption(
    tmp_path: Path, corruption: str
) -> None:
    shell, entry_shas, references, _expected_bytes = _fixture(tmp_path)
    finalize_population_with_rust(
        output_root=tmp_path,
        population_without_sha=shell,
        expected_entry_sha256s=entry_shas,
        accepted_candidates=references,
    )
    target = tmp_path / "proposal-journal" / "00000002.json"
    payload = target.read_bytes()
    if corruption == "tamper":
        target.write_bytes(payload.replace(b"alpha", b"omega", 1))
    elif corruption == "truncate":
        target.write_bytes(payload[: len(payload) // 2])
    elif corruption == "missing":
        target.unlink()
    elif corruption == "extra":
        (tmp_path / "proposal-journal" / "unexpected.json").write_bytes(b"{}\n")
    elif corruption == "nan":
        target.write_bytes(payload.replace(b"1.25", b"NaN ", 1))
    completed = _invoke_manifest(tmp_path)
    assert completed.returncode == 2
    assert completed.stderr.startswith("ERROR:")


def test_rust_finalizer_rejects_duplicate_candidate_ids(tmp_path: Path) -> None:
    shell, entry_shas, references, _expected_bytes = _fixture(
        tmp_path, duplicate_id=True
    )
    with pytest.raises(Exception, match="duplicate candidate ID"):
        finalize_population_with_rust(
            output_root=tmp_path,
            population_without_sha=shell,
            expected_entry_sha256s=entry_shas,
            accepted_candidates=references,
        )


def test_rust_finalizer_rejects_divergent_existing_population(tmp_path: Path) -> None:
    shell, entry_shas, references, _expected_bytes = _fixture(tmp_path)
    finalize_population_with_rust(
        output_root=tmp_path,
        population_without_sha=shell,
        expected_entry_sha256s=entry_shas,
        accepted_candidates=references,
    )
    population = tmp_path / "population.json"
    payload = population.read_bytes()
    population.write_bytes(payload.replace(b"alpha", b"omega", 1))
    with pytest.raises(Exception, match="divergent pair-generation artifact"):
        finalize_population_with_rust(
            output_root=tmp_path,
            population_without_sha=shell,
            expected_entry_sha256s=entry_shas,
            accepted_candidates=references,
        )
