"""Regression checks for the frozen V3 component-surrogate census labels."""

from __future__ import annotations

import json
import hashlib
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


def test_v3_projection_runtime_authority_keeps_raw_events_and_fresh_bars_distinct() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    authority_path = (
        repo_root
        / "research"
        / "temporal-qd"
        / "component-surrogate-validation-v3"
        / "feature-projection-runtime-authority-addendum-v1.json"
    )
    authority = json.loads(authority_path.read_text(encoding="utf-8"))
    expected = authority.pop("authorityAddendumCanonicalPayloadSha256")
    actual = "sha256:" + hashlib.sha256(
        json.dumps(
            authority,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert actual == expected

    assert authority["featureProtocolV2"]["gitBlobSha256"] == (
        "sha256:b412209520f0a1b8ea7bacf0d1f0e0bef1eda508f0e8096fdb8e31bc8b8c04cc"
    )
    assert authority["featureProtocolV2"]["worktreeMatchesFrozenGitBlob"] is True
    assert len(authority["componentImplementations"]) == 19
    assert authority["runtime"]["allImportedFuzzfolioCoreModulesUnderPinnedEngine"] is True
    assert authority["projectionAndFreshness"]["freshFeatures"] == [
        "freshEventBarCount",
        "freshEventFraction",
        "freshEventAvailability = freshEventFraction",
    ]
    assert "false-to-true" in authority["projectionAndFreshness"]["eventStarts"]
    assert authority["clockAndFrameContract"]["inputFrame"]["requiredColumns"] == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "pair",
    ]
