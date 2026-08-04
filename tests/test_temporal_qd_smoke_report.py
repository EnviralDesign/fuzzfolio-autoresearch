from __future__ import annotations

import json

from autoresearch.temporal_qd_smoke_report import (
    build_qd_construction_smoke_report,
)


def _write_json(path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_report_combines_interrupted_diversity_cpu_and_ram_evidence(tmp_path) -> None:
    root = tmp_path / "generation"
    _write_json(
        root / "proposal-journal" / "00000000.json",
        {
            "disposition": "accepted",
            "candidate": {
                "candidateIdentitySha256": "sha256:candidate",
                "bidirectionalGenome": {
                    "identities": {"rawPairSha256": "sha256:pair"}
                },
            },
        },
    )
    _write_json(
        root / "proposal-journal" / "00000001.json",
        {"disposition": "duplicate_pair_genome"},
    )
    _write_json(
        root / "performance" / "latest-summary.json",
        {
            "outcome": "error",
            "errorType": "PerformanceResourcePressureError",
            "wallDurationNs": 4_000_000_000,
            "coordinatorCpuNs": 2_000_000_000,
            "result": {},
            "phaseBreakdown": {
                "proposal.construct": {
                    "exclusiveWall": {
                        "count": 2,
                        "totalNs": 1_000_000_000,
                        "meanNs": 500_000_000,
                        "p95Ns": 600_000_000,
                        "maxNs": 700_000_000,
                    },
                    "coordinatorCpuExclusive": {
                        "count": 2,
                        "totalNs": 800_000_000,
                        "meanNs": 400_000_000,
                        "p95Ns": 500_000_000,
                        "maxNs": 600_000_000,
                    },
                },
                "proposal.total": {
                    "aggregationRole": "overlapping_total",
                    "exclusiveWall": {
                        "count": 2,
                        "totalNs": 3_000_000_000,
                        "meanNs": 1_500_000_000,
                        "p95Ns": 1_600_000_000,
                        "maxNs": 1_700_000_000,
                    },
                    "coordinatorCpuExclusive": {
                        "count": 2,
                        "totalNs": 1_500_000_000,
                        "meanNs": 750_000_000,
                        "p95Ns": 800_000_000,
                        "maxNs": 900_000_000,
                    },
                },
            },
            "resources": {
                "peakTreeRssBytes": 9_000_000_000,
                "resourceGuard": {"status": "breached"},
            },
            "instrumentation": {"resourceSampleLineCount": 8},
        },
    )

    report = build_qd_construction_smoke_report(root)

    assert report["outcome"] == "error"
    assert report["errorType"] == "PerformanceResourcePressureError"
    assert report["diversity"]["proposalCount"] == 2
    assert report["diversity"]["acceptedCount"] == 1
    assert report["diversity"]["duplicateCount"] == 1
    assert report["diversity"]["acceptedUniquePairGenomeCount"] == 1
    assert report["timing"]["meanCoordinatorCoreEquivalent"] == 0.5
    assert report["timing"]["topExclusiveWallPhases"][0]["phase"] == (
        "proposal.construct"
    )
    assert report["resources"]["peakTreeRssBytes"] == 9_000_000_000
    assert report["reportSha256"].startswith("sha256:")
