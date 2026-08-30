"""Regression checks for the frozen V3 component-surrogate census labels."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


def test_v3_census_keeps_context_coverage_distinct_from_outcome_cohort_counts() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = (
        repo_root
        / "research"
        / "temporal-qd"
        / "component-surrogate-validation-v3"
        / "extraction-census-v3.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    contexts = manifest["contexts"]

    # These counts are derived from the outcome-free accepted contexts.
    side_counts = Counter(context["side"] for context in contexts)
    timeframe_counts = Counter(context["component"]["timeframe"] for context in contexts)
    assert len(contexts) == 41
    assert side_counts == {"long": 20, "short": 21}
    assert timeframe_counts == {"M5": 37, "M15": 4}

    # These are a separate, explicitly named retrospective-reconciliation
    # cohort.  They must never be presented as side or timeframe coverage.
    assert manifest["cohortCounts"] == {
        "acceptedDirectionalEventInsertContexts": 41,
        "uniqueComponentIdentities": 19,
        "p3SamePanelParentComparableContexts": 25,
        "realizedPhenotypesAmongP3Contexts": 17,
        "childrenWithP1P2Backfill": 11,
        "exactParentComparableP1P2Cases": 9,
    }
