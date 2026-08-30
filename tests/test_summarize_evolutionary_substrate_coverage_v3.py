from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import summarize_evolutionary_substrate_coverage_v3 as coverage  # noqa: E402


def matrix() -> dict[str, object]:
    return {
        "schemaVersion": coverage.MATRIX_SCHEMA,
        "mode": coverage.MATRIX_MODE,
        "includeCrossover": False,
        "mutationDepth": 1,
        "childrenPerFamily": 2,
        "families": ["hold", "resource"],
        "parents": [
            {"candidateId": "archive", "role": "archive"},
            {"candidateId": "control", "role": "inactive_control"},
        ],
    }


def test_matrix_slot_uses_parent_then_family_then_child_order() -> None:
    assert coverage.matrix_slot(matrix(), 0) == {
        "proposalOrdinal": 0,
        "parentCandidateId": "archive",
        "parentRole": "archive",
        "operatorFamily": "hold",
        "childIndex": 0,
    }
    assert coverage.matrix_slot(matrix(), 3)["operatorFamily"] == "resource"
    assert coverage.matrix_slot(matrix(), 4)["parentCandidateId"] == "control"


def test_matrix_slot_rejects_out_of_range_ordinal() -> None:
    with pytest.raises(coverage.CoverageError, match="outside"):
        coverage.matrix_slot(matrix(), 8)
