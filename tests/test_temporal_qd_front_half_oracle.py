from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import temporal_qd_front_half_oracle as oracle


def _write_json(path: Path, value: object, *, compact: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if compact:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    else:
        payload = json.dumps(value, indent=2, sort_keys=False) + "\n"
    path.write_text(payload, encoding="utf-8", newline="\n")


def _write_run(root: Path, *, pretty_population: bool = False) -> None:
    _write_json(root / "pair-config.json", {"configSha256": "sha256:" + "a" * 64})
    _write_json(
        root / "population.json",
        {
            "schemaVersion": "temporal_qd_generation_population_v3",
            "candidateCount": 1,
            "candidates": [
                {
                    "candidateId": "candidate-000",
                    "candidateIdentitySha256": "sha256:" + "b" * 64,
                }
            ],
        },
        compact=not pretty_population,
    )
    _write_json(
        root / "evaluation-population.json",
        {"candidateCount": 1, "candidateIds": ["candidate-000"]},
    )
    _write_json(
        root / "generation-journal.json",
        {"entrySha256s": ["sha256:" + "c" * 64], "proposalCount": 1},
    )
    _write_json(root / "identity-ledger.json", {"ledgerSha256": "sha256:" + "d" * 64})
    _write_json(
        root / "proposal-journal" / "00000000.json",
        {"proposalOrdinal": 0, "disposition": "materialized"},
    )
    _write_json(root / "g0-bootstrap" / "selection.json", {"selected": ["candidate-000"]})


def test_exact_equal_roots_export_byte_exact_public_tree_and_witness(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_run(left)
    _write_run(right)

    comparison = oracle.compare_roots(left, right, shape=8)

    assert comparison["semanticExact"] is True
    assert comparison["byteExact"] is True
    assert comparison["firstDivergence"] is None

    fixture = oracle.export_fixture(
        source_root=left,
        output_root=tmp_path / "fixture",
        shape=8,
    )
    validated = oracle.validate_fixture(tmp_path / "fixture" / "oracle-fixture.json")
    assert validated == fixture
    assert fixture["semanticWitness"]["shapeMetadata"] == oracle.FIXED_SHAPE_METADATA[8]
    copied = tmp_path / "fixture" / "public-semantic-tree"
    assert (copied / "population.json").read_bytes() == (left / "population.json").read_bytes()
    assert (copied / "proposal-journal" / "00000000.json").read_bytes() == (
        left / "proposal-journal" / "00000000.json"
    ).read_bytes()
    (copied / "population.json").write_text('{"tampered":true}\n', encoding="utf-8")
    with pytest.raises(oracle.OracleFixtureError, match="artifact.*drifted"):
        oracle.validate_fixture(tmp_path / "fixture" / "oracle-fixture.json")


def test_internal_and_performance_files_are_intentionally_excluded(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_run(left)
    _write_run(right)
    _write_json(left / "performance" / "latest-summary.json", {"wallNs": 1})
    _write_json(right / "performance" / "latest-summary.json", {"wallNs": 999})
    _write_json(left / "objects" / "opaque.json", {"layout": "python"})
    _write_json(right / "objects" / "opaque.json", {"layout": "rust"})
    _write_json(left / "internal" / "checkpoint.json", {"offset": 1})
    _write_json(right / "internal" / "checkpoint.json", {"offset": 2})

    comparison = oracle.compare_roots(left, right, shape=64)
    paths = [
        path.relative_to(left).as_posix() for path in oracle.public_semantic_paths(left)
    ]

    assert comparison["semanticExact"] is True
    assert comparison["byteExact"] is True
    assert all(not path.startswith(("performance/", "objects/", "internal/")) for path in paths)


def test_comparator_reports_first_semantic_json_divergence(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_run(left)
    _write_run(right)
    population = json.loads((right / "population.json").read_text(encoding="utf-8"))
    population["candidates"][0]["candidateId"] = "candidate-diverged"
    _write_json(right / "population.json", population)

    comparison = oracle.compare_roots(left, right, shape=1)
    left_artifacts = {
        row["path"]: row for row in oracle.build_semantic_witness(left, shape=1)["artifacts"]
    }
    right_artifacts = {
        row["path"]: row for row in oracle.build_semantic_witness(right, shape=1)["artifacts"]
    }

    assert comparison["semanticExact"] is False
    assert comparison["byteExact"] is False
    assert comparison["firstDivergence"] == {
        "kind": "semantic_json",
        "path": "population.json",
        "leftSemanticSha256": left_artifacts["population.json"]["semanticSha256"],
        "rightSemanticSha256": right_artifacts["population.json"]["semanticSha256"],
        "pointer": "/candidates/0/candidateId",
        "reason": "value",
        "left": '"candidate-000"',
        "right": '"candidate-diverged"',
    }


def test_byte_difference_with_equal_normalized_json_is_reported_separately(
    tmp_path: Path,
) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_run(left)
    _write_run(right, pretty_population=True)

    comparison = oracle.compare_roots(left, right, shape=8)

    assert comparison["semanticExact"] is True
    assert comparison["byteExact"] is False
    assert comparison["firstDivergence"]["kind"] == "public_artifact_bytes"
    assert comparison["firstDivergence"]["path"] == "population.json"


def test_witness_validation_rejects_shape_metadata_and_tree_hash_drift(tmp_path: Path) -> None:
    root = tmp_path / "run"
    _write_run(root)
    witness = oracle.build_semantic_witness(root, shape=128)

    assert oracle.validate_semantic_witness(witness) == witness

    drifted_metadata = json.loads(json.dumps(witness))
    drifted_metadata["shapeMetadata"]["label"] = "drifted"
    with pytest.raises(oracle.OracleFixtureError, match="shape metadata drifted"):
        oracle.validate_semantic_witness(drifted_metadata)

    drifted_tree = json.loads(json.dumps(witness))
    drifted_tree["artifacts"][0]["semanticSha256"] = "sha256:" + "0" * 64
    with pytest.raises(oracle.OracleFixtureError, match="tree identity mismatch"):
        oracle.validate_semantic_witness(drifted_tree)


def test_fixed_shape_metadata_is_closed_to_the_admission_matrix() -> None:
    assert tuple(oracle.FIXED_SHAPE_METADATA) == (1, 8, 64, 128, 1024)
    assert all(
        metadata["pythonOracleReplay"]
        for metadata in oracle.FIXED_SHAPE_METADATA.values()
    )
    with pytest.raises(oracle.OracleFixtureError, match="unsupported fixed oracle shape"):
        oracle.build_semantic_witness(Path.cwd(), shape=2)
