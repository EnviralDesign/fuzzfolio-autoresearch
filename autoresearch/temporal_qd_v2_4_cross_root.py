"""Expanded V2.4 cross-root determinism verifier."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256

SCHEMA = "temporal_qd_topology_v2_4_cross_root_report_v1"


def _sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _pair(left: Path, right: Path, logical_id: str, expected_equal: bool) -> dict[str, Any]:
    left_sha, right_sha = _sha(left), _sha(right)
    equal = left_sha == right_sha
    return {
        "logicalId": logical_id,
        "leftRawSha256": left_sha,
        "rightRawSha256": right_sha,
        "leftSizeBytes": left.stat().st_size,
        "rightSizeBytes": right.stat().st_size,
        "expectedByteIdentical": expected_equal,
        "observedByteIdentical": equal,
        "passed": equal if expected_equal else True,
    }


def build_report(
    *,
    package_root_a: Path,
    package_root_b: Path,
    proof_root_a: Path,
    proof_root_b: Path,
    authority_root: Path,
) -> dict[str, Any]:
    portable: list[dict[str, Any]] = []
    operational: list[dict[str, Any]] = []
    for panel in (1, 2, 3):
        for relative in (
            "campaign-input-checkpoint.json",
            "screening-run/tasks.jsonl",
            "cohort-population.json",
        ):
            portable.append(
                _pair(
                    package_root_a / f"panel-{panel}" / relative,
                    package_root_b / f"panel-{panel}" / relative,
                    f"panel-{panel}/{relative}",
                    True,
                )
            )
        operational.append(
            _pair(
                package_root_a / f"panel-{panel}" / ".native-v5-campaign-freeze-manifest.json",
                package_root_b / f"panel-{panel}" / ".native-v5-campaign-freeze-manifest.json",
                f"panel-{panel}/.native-v5-campaign-freeze-manifest.json",
                False,
            )
        )
        for relative in (
            "campaign-output-local/evaluated-members.jsonl",
            "campaign-output-local/candidate-panel-bundles.jsonl",
            "campaign-output-local/tail-result-index-v4.json",
            "gateway-local-output/.native-gateway-dispatch/execution-receipt.json",
        ):
            portable.append(
                _pair(
                    proof_root_a / f"panel-{panel}" / relative,
                    proof_root_b / f"panel-{panel}" / relative,
                    f"panel-{panel}/{relative}",
                    True,
                )
            )
        for relative in (
            "campaign-output-local/campaign-output-manifest.json",
            "campaign-output-local/campaign-output-checkpoint.json",
        ):
            operational.append(
                _pair(
                    proof_root_a / f"panel-{panel}" / relative,
                    proof_root_b / f"panel-{panel}" / relative,
                    f"panel-{panel}/{relative}",
                    False,
                )
            )
    portable.append(
        _pair(
            proof_root_a / "topology-production-analysis-v2.json",
            proof_root_b / "topology-production-analysis-v2.json",
            "topology-production-analysis-v2.json",
            True,
        )
    )
    # Committed authorities are content-addressed once; record their exact raw
    # identities in the cross-root inventory rather than pretending they have
    # an operational output-root binding.
    authority_files = (
        "topology-production-launch-control-v1.json",
        "topology-production-task-mapping-v1.json",
        "topology-production-output-templates-v1.json",
        "topology-replication-survival-rule-v1.json",
        "topology-post-run-analyzer-contract-v1.json",
    )
    authorities = [
        {
            "logicalId": name,
            "rawSha256": _sha(authority_root / name),
            "sizeBytes": (authority_root / name).stat().st_size,
            "rootIndependentAuthority": True,
        }
        for name in authority_files
    ]
    report: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "portableArtifacts": portable,
        "operationalRootBoundArtifacts": operational,
        "committedAuthorities": authorities,
        "allPortableArtifactsByteIdentical": all(row["passed"] for row in portable),
        "operationalDifferencePermittedOnlyForRootBoundManifestAndCheckpoint": all(
            row["logicalId"].endswith(
                (
                    ".native-v5-campaign-freeze-manifest.json",
                    "campaign-output-manifest.json",
                    "campaign-output-checkpoint.json",
                )
            )
            for row in operational
        ),
    }
    report["crossRootReportSha256"] = canonical_sha256(report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package-root-a", type=Path, required=True)
    parser.add_argument("--package-root-b", type=Path, required=True)
    parser.add_argument("--proof-root-a", type=Path, required=True)
    parser.add_argument("--proof-root-b", type=Path, required=True)
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(
        package_root_a=args.package_root_a,
        package_root_b=args.package_root_b,
        proof_root_a=args.proof_root_a,
        proof_root_b=args.proof_root_b,
        authority_root=args.authority_root,
    )
    if not report["allPortableArtifactsByteIdentical"]:
        raise SystemExit("portable cross-root artifact drifted")
    args.output.write_text(canonical_json(report) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
