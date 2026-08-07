from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_pair_generation as pair_generation
from scripts.temporal_qd_front_half_python_oracle_corpus import (
    CORPUS_SCHEMA,
    materialize_python_oracle_corpus,
)


def _all_values(value: object) -> list[object]:
    if isinstance(value, dict):
        output: list[object] = []
        for item in value.values():
            output.extend(_all_values(item))
        return output
    if isinstance(value, list):
        output = []
        for item in value:
            output.extend(_all_values(item))
        return output
    return [value]


def test_python_oracle_corpus_materializes_small_complete_admission_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # This is a tiny deterministic contract fixture, not a host-capacity gate.
    # Keep production's 12 GiB headroom policy intact while allowing the corpus
    # to run on bounded CI hosts; observability has dedicated resource-guard tests.
    monkeypatch.setattr(
        pair_generation.PerformanceTrace,
        "assert_resource_guard",
        lambda _self: None,
    )
    manifest = materialize_python_oracle_corpus(tmp_path / "oracle-corpus")

    assert manifest["schemaVersion"] == CORPUS_SCHEMA
    assert manifest["coverage"] == {
        "g0RandomImmigrants": True,
        "parentMutationDepthGreaterThanOne": True,
        "sameSideCrossover": True,
        "crossoverRejection": True,
        "duplicateRejection": True,
        "splitRestart": True,
        "decimalIndicatorAndProtectionValues": [
            20.125,
            39.875,
            1.375,
            1.125,
            2.375,
            1.625,
            0.125,
            36.5,
        ],
    }
    for case in ("shape1G0", "shape8G0", "shape8Offspring"):
        result = manifest["cases"][case]
        assert result["completed"] is True
        assert result["restartSemanticExact"] is True
        assert result["restartByteExact"] is True
        assert str(result["semanticTreeSha256"]).startswith("sha256:")
    assert "operation_rejected" in manifest["cases"]["rejectedCrossover"]["dispositions"]
    assert any(
        disposition.startswith("duplicate_pair_genome")
        for disposition in manifest["cases"]["duplicateRejection"]["dispositions"]
    )

    journal = json.loads(
        (tmp_path / "oracle-corpus" / "shape-8-g0" / "full" / "proposal-journal" / "00000000.json").read_text(
            encoding="utf-8"
        )
    )
    values = _all_values(journal)
    for expected in manifest["coverage"]["decimalIndicatorAndProtectionValues"]:
        assert expected in values

    persisted = json.loads((tmp_path / "oracle-corpus" / "corpus-manifest.json").read_text(encoding="utf-8"))
    assert persisted == manifest
